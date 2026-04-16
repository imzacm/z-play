mod file_cache;
mod queue;
mod serve_dir;

use std::borrow::Cow;
use std::cell::LazyCell;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{LazyLock, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::Request;
use axum::http::header::{CACHE_CONTROL, EXPIRES, PRAGMA};
use axum::http::{HeaderName, StatusCode};
use axum::middleware::Next;
use axum::response::sse::Event;
use axum::response::{Html, IntoResponse, Json, Response, Sse};
use axum::routing::{get, patch};
use axum::{Router, middleware};
use axum_extra::extract::Query;
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::Stream;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use triomphe::Arc;
use z_play::inotify::{self, INotify};
use z_play::random_files;
#[cfg(feature = "immich")]
use z_play::random_files_immich::{self, ImmichClient};
use z_play::walkdir::walk_roots_filter;

use self::queue::Queue;
use crate::http::queue::QueueStats;

const QUEUE_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-count");
const QUEUE_SIZE_HEADER: HeaderName = HeaderName::from_static("x-queue-size");
const QUEUE_VIDEO_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-video-count");
const QUEUE_IMAGE_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-image-count");
const QUEUE_AUDIO_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-audio-count");

static QUEUE: OnceLock<Queue> = OnceLock::new();

// 5 GiB
const FILE_CACHE_LIMIT: usize = 5 * 1024 * 1024 * 1024;
#[thread_local]
static FILE_CACHE: LazyCell<file_cache::FileCache> =
    LazyCell::new(|| file_cache::FileCache::new(FILE_CACHE_LIMIT));

pub fn start_server(port: u16, roots: Vec<PathBuf>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        start_server_inner(port, roots).await;
    });
}

async fn start_server_inner(port: u16, mut roots: Vec<PathBuf>) {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/roots", get(get_roots))
        .route("/roots", patch(patch_roots))
        .route("/random", get(random_path_handler))
        .route("/queue", get(queue_info_handler))
        .route("/reset", get(reset_queue_handler))
        .route("/shuffle", get(shuffle_queue_handler))
        .route("/sse", get(sse_handler))
        .nest(
            "/files",
            Router::new()
                .route("/{*path}", get(serve_dir::serve_dir))
                .route_layer(middleware::from_fn(validate_path_middleware)),
        );

    roots.retain_mut(|root| match root.canonicalize() {
        Ok(path) => {
            println!("Adding root: {}", path.display());
            *root = path;
            true
        }
        Err(error) => {
            println!("Removing invalid root \"{}\": {error}", root.display());
            false
        }
    });
    roots.shrink_to_fit();

    let queue = QUEUE.get_or_init(move || Queue::new(roots));

    #[cfg(feature = "immich")]
    compio::runtime::spawn(immich_queue_feeder(queue)).detach();

    compio::runtime::spawn(queue_feeder(queue, None)).detach();

    std::thread::spawn(|| directory_counts());

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = compio::net::TcpListener::bind(address).await.unwrap();
    cyper_axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum FileKind {
    Video,
    Audio,
    Image,
}

impl FileKind {
    pub const ALL: [Self; 3] = [Self::Video, Self::Audio, Self::Image];
    pub const NUM_VARIANTS: usize = Self::ALL.len();

    fn is_valid_char(c: char) -> bool {
        c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-' || c == '_'
    }

    fn from_path<P>(path: P) -> Option<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let extension = path.extension()?.to_str()?;

        let extension = if extension.chars().all(Self::is_valid_char) {
            Cow::Borrowed(extension)
        } else {
            Cow::Owned(extension.to_ascii_lowercase())
        };

        Self::from_extension(&extension)
    }

    fn from_extension(extension: &str) -> Option<Self> {
        assert!(extension.chars().all(Self::is_valid_char));
        match extension {
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "avif" | "ico" | "apng" => {
                Some(Self::Image)
            }
            "mp4" | "mkv" | "webm" | "avi" | "mov" | "wmv" | "flv" | "mpeg" | "ogv" => {
                Some(Self::Video)
            }
            "mp3" | "wav" | "ogg" | "m4a" | "flac" | "aac" | "mpga" | "opus" | "weba" | "oga" => {
                Some(Self::Audio)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RandomQuery {
    #[serde(default, rename = "kind")]
    kinds: FxHashSet<FileKind>,
    #[serde(default, rename = "root")]
    roots: FxHashSet<String>,
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootJson {
    path: String,
    enabled: bool,
}

async fn get_roots() -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    let (enabled_roots, disabled_roots) = futures_util::join!(
        queue.enabled_roots().read_async(),
        queue.disabled_roots().read_async()
    );

    let roots = enabled_roots
        .iter()
        .map(|path| (path, true))
        .chain(disabled_roots.iter().map(|path| (path, false)))
        .map(|(path, enabled)| RootJson { path: path.to_string_lossy().into_owned(), enabled })
        .collect::<Vec<_>>();

    (queue_info(), Json(roots))
}

async fn patch_roots(body: Json<Vec<RootJson>>) -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    let (mut enabled_roots, mut disabled_roots) = futures_util::join!(
        queue.enabled_roots().write_async(),
        queue.disabled_roots().write_async()
    );

    for RootJson { path, enabled } in body.0 {
        let path = Path::new(&path);
        let (from, to) = if enabled {
            (&mut disabled_roots, &mut enabled_roots)
        } else {
            (&mut enabled_roots, &mut disabled_roots)
        };

        let index = from.iter().position(|p| p == path);
        let Some(index) = index else { continue };
        let path = from.remove(index);
        to.push(path);
    }

    drop(enabled_roots);
    drop(disabled_roots);

    queue.refresh_roots().await;

    (queue_info(), StatusCode::NO_CONTENT)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathResponse {
    path: String,
    kind: FileKind,
}

async fn random_path_handler(query: Query<RandomQuery>) -> impl IntoResponse {
    let RandomQuery { kinds, roots } = query.0;

    let queue = QUEUE.get().unwrap();

    let (path, file_kind) = loop {
        let filter_kinds = if kinds.is_empty() { None } else { Some(&kinds) };
        let filter_roots = if roots.is_empty() { None } else { Some(&roots) };
        let (path, file_kind) = queue.find_pop_async(filter_kinds, filter_roots).await;

        let path = Utf8PathBuf::from_path_buf(path).expect("Only UTF-8 paths are supported");

        let path_clone = path.clone();
        compio::runtime::spawn(precache_file(path_clone)).detach();
        break (path, file_kind);
    };

    (
        [
            (CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (PRAGMA, "no-cache"),
            (EXPIRES, "0"),
        ],
        queue_info(),
        Json(PathResponse { path: path.into_string(), kind: file_kind }),
    )
}

async fn queue_info_handler() -> [(HeaderName, String); 5] {
    queue_info()
}

fn queue_info() -> [(HeaderName, String); 5] {
    let queue = QUEUE.get().unwrap();
    let stats = queue.stats();
    [
        (QUEUE_COUNT_HEADER, queue.len().to_string()),
        (QUEUE_SIZE_HEADER, Queue::QUEUE_SIZE.to_string()),
        (QUEUE_VIDEO_COUNT_HEADER, stats.video_count.to_string()),
        (QUEUE_IMAGE_COUNT_HEADER, stats.image_count.to_string()),
        (QUEUE_AUDIO_COUNT_HEADER, stats.audio_count.to_string()),
    ]
}

async fn reset_queue_handler() -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    queue.reset().await;
    compio::runtime::spawn(queue_feeder(queue, Some(1))).detach();
    StatusCode::NO_CONTENT
}

async fn shuffle_queue_handler() -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    queue.shuffle();
    StatusCode::NO_CONTENT
}

async fn validate_path_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let path_query = request.uri().path();
    let decoded_path = urlencoding::decode(path_query).map_err(|_| StatusCode::BAD_REQUEST)?;
    let requested_path = Path::new(decoded_path.as_ref());

    let is_valid = {
        let queue = QUEUE.get().unwrap();
        let roots = queue.enabled_roots().read_async().await;
        roots.iter().any(|root| requested_path.starts_with(root))
    };
    if is_valid { Ok(next.run(request).await) } else { Err(StatusCode::NOT_FOUND) }
}

#[derive(Debug, Clone, Serialize)]
struct QueueStatsJson {
    queue_count: usize,
    queue_size: usize,
    video_count: usize,
    image_count: usize,
    audio_count: usize,
}

async fn sse_handler() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);

    let tx_clone = tx.clone();
    compio::runtime::spawn(async move {
        let tx = tx_clone;
        let queue = QUEUE.get().unwrap();

        let mut prev_len = 0;
        let mut prev_stats = queue::QueueStats::default();

        loop {
            let stats = queue.stats();
            let len = queue.len();
            if stats == prev_stats && len == prev_len {
                compio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let json = QueueStatsJson {
                queue_count: len,
                queue_size: Queue::QUEUE_SIZE,
                video_count: stats.video_count,
                image_count: stats.image_count,
                audio_count: stats.audio_count,
            };

            prev_stats = stats;
            prev_len = len;

            let json = serde_json::to_string(&json).unwrap();
            let event = Event::default().event("queue_info").data(json);
            if tx.send_async(Ok(event)).await.is_err() {
                break;
            }

            compio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .detach();

    compio::runtime::spawn(async move {
        let mut interval = compio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            // The browser ignores this, but it keeps the TCP socket warm.
            let event = Event::default().comment("keep-alive");
            if tx.send_async(Ok(event)).await.is_err() {
                break;
            }
        }
    })
    .detach();

    let stream = rx.into_stream();
    Sse::new(stream)
}

fn filter_path<P>(path: P) -> bool
where
    P: AsRef<Path>,
{
    let queue = QUEUE.get().unwrap();
    if queue.contains_path(path.as_ref()) {
        return false;
    }

    let stats = queue.stats();
    match FileKind::from_path(path) {
        Some(FileKind::Video) if stats.video_count < Queue::QUEUE_SIZE => true,
        Some(FileKind::Image) if stats.image_count < Queue::QUEUE_SIZE => true,
        Some(FileKind::Audio) if stats.audio_count < Queue::QUEUE_SIZE => true,
        _ => false,
    }
}

#[cfg(feature = "immich")]
async fn immich_queue_feeder(queue: &Queue) {
    let immich = {
        let immich_url =
            std::env::var("IMMICH_URL").expect("IMMICH_URL environment variable must be set");
        let immich_api_key = std::env::var("IMMICH_API_KEY")
            .expect("IMMICH_API_KEY environment variable must be set");

        ImmichClient::new(immich_url, &immich_api_key)
    };

    println!("Starting queue feeder");
    'main: loop {
        let queue_len = queue.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let mut timeout_ms = 100 + (9900 * queue_len / Queue::MAX_QUEUE_SIZE);

        let path = loop {
            if queue.len() == Queue::MAX_QUEUE_SIZE {
                let listener = queue.observe_pop();
                if queue.len() == Queue::MAX_QUEUE_SIZE {
                    println!("Queue is full, waiting for pop");
                    listener.await;
                    continue 'main;
                }
            }

            let mut roots = queue.enabled_roots().read_async().await.clone();
            if roots.is_empty() {
                println!("No enabled roots, sleeping for 1s");
                compio::time::sleep(Duration::from_secs(1)).await;
                continue 'main;
            }

            {
                use rand::seq::SliceRandom;

                let mut rng = rand::rng();
                roots.shuffle(&mut rng);
            }

            let timeout = Duration::from_millis(timeout_ms as u64) * 2;

            let path = random_files_immich::random_file_with_timeout_filter(
                &immich,
                &roots,
                timeout,
                |path| filter_path(path),
            )
            .await;
            match path {
                Some(path) => break path,
                None => {
                    timeout_ms += 1000;
                    println!("[Immich] No files found, increasing timeout to {timeout_ms}ms");

                    if queue.len() > 1 {
                        compio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        };

        queue.push_async(path).await;
    }
}

async fn queue_feeder(queue: &Queue, max_count: Option<usize>) {
    fn filter(path: &Path, is_dir: bool) -> bool {
        if is_dir {
            return true;
        }
        filter_path(path)
    }

    println!("Starting queue feeder");
    let mut counter = 0;
    'main: loop {
        let queue_len = queue.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let mut timeout_ms = 100 + (9900 * queue_len / Queue::MAX_QUEUE_SIZE);

        let path = loop {
            let pop_listener = queue.observe_pop();
            let dir_count_listener = DIR_COUNTS.notify.listener();
            {
                let total_counts = DIR_COUNTS.total_counts.read();
                let queue_counts = queue.stats();
                if (queue_counts.video_count == Queue::QUEUE_SIZE
                    || queue_counts.video_count == total_counts.video_count)
                    && (queue_counts.image_count == Queue::QUEUE_SIZE
                        || queue_counts.image_count == total_counts.image_count)
                    && (queue_counts.audio_count == Queue::QUEUE_SIZE
                        || queue_counts.audio_count == total_counts.audio_count)
                {
                    println!("Queue contains all available files, waiting for pop or new files");
                    tokio::select! {
                        _ = pop_listener => (),
                        _ = dir_count_listener => (),
                    }
                }
            }

            if queue.len() == Queue::MAX_QUEUE_SIZE {
                let listener = queue.observe_pop();
                if queue.len() == Queue::MAX_QUEUE_SIZE {
                    println!("Queue is full, waiting for pop");
                    listener.await;
                    continue 'main;
                }
            }

            let mut roots = queue.enabled_roots().read_async().await.clone();
            if roots.is_empty() {
                println!("No enabled roots, sleeping for 1s");
                compio::time::sleep(Duration::from_secs(1)).await;
                continue 'main;
            }

            {
                use rand::seq::SliceRandom;

                let mut rng = rand::rng();
                roots.shuffle(&mut rng);
            }

            let timeout = Duration::from_millis(timeout_ms as u64);
            let path = random_files::random_file_with_timeout_filter(&roots, timeout, filter).await;

            match path {
                Some(path) => break path,
                None => {
                    timeout_ms += 1000;
                    println!("No files found, increasing timeout to {timeout_ms}ms");
                    if queue.len() > 1 {
                        compio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        };

        queue.push_async(path).await;

        if max_count.is_some_and(|v| counter >= v) {
            break;
        }
        counter += 1;
    }
}

#[derive(Debug)]
struct DirectoryCounts {
    total_counts: Arc<z_sync::Lock16<QueueStats>>,
    dir_counts: Arc<z_sync::Lock16<FxHashMap<PathBuf, QueueStats>>>,
    notify: z_sync::Notify16,
}

static DIR_COUNTS: LazyLock<DirectoryCounts> = LazyLock::new(|| {
    let mut total_counts = QueueStats::default();

    // Stop the queue from pausing before initial counts are loaded.
    total_counts.video_count = usize::MAX;
    total_counts.image_count = usize::MAX;
    total_counts.audio_count = usize::MAX;

    DirectoryCounts {
        total_counts: Arc::new(z_sync::Lock16::new(total_counts)),
        dir_counts: Arc::new(z_sync::Lock16::new(FxHashMap::default())),
        notify: z_sync::Notify16::new(),
    }
});

fn directory_counts() {
    let inotify = match INotify::new() {
        Ok(inotify) => Arc::new(inotify),
        Err(error) => {
            eprintln!("Failed to initialize inotify: {error}");
            return;
        }
    };

    let roots = {
        let mut roots = Vec::new();
        let queue = QUEUE.get().unwrap();
        roots.extend(queue.enabled_roots().read().clone());
        roots.extend(queue.disabled_roots().read().clone());
        roots
    };

    let total_counts = Arc::new(z_sync::Lock16::new(QueueStats::default()));

    let inotify_clone = inotify.clone();
    let total_counts_clone = total_counts.clone();
    let filter = move |path: &Path, is_dir: bool| {
        if is_dir {
            let result = inotify_clone.add_watch(
                path.to_path_buf(),
                rustix::fs::inotify::WatchFlags::DELETE_SELF
                    | rustix::fs::inotify::WatchFlags::DELETE
                    | rustix::fs::inotify::WatchFlags::CREATE
                    | rustix::fs::inotify::WatchFlags::MOVE
                    | rustix::fs::inotify::WatchFlags::MOVE_SELF,
            );
            if let Err(error) = result {
                eprintln!("Failed to add watch for {}: {error}", path.display());
            }
            DIR_COUNTS.dir_counts.write().insert(path.to_path_buf(), QueueStats::default());
            return true;
        }

        let Some(kind) = FileKind::from_path(path) else { return false };

        total_counts_clone.write().add(kind);
        let parent = path.parent().unwrap();
        let mut dir_counts = DIR_COUNTS.dir_counts.write();
        if !dir_counts.contains_key(parent) {
            dir_counts.insert(parent.to_path_buf(), QueueStats::default());
        }
        dir_counts.get_mut(parent).unwrap().add(kind);

        true
    };

    let compio_rt = compio::runtime::Runtime::new().unwrap();

    compio_rt.block_on(async move {
        let rx = match walk_roots_filter(&roots, None, filter).await {
            Ok(rx) => rx,
            Err(error) => {
                eprintln!("Failed to walk roots: {error}");
                return;
            }
        };

        while let Ok(_) = rx.recv_async().await {}
    });

    {
        let mut total_counts = total_counts.write();
        let mut static_total_counts = DIR_COUNTS.total_counts.write();
        std::mem::swap(&mut *total_counts, &mut *static_total_counts);
    }

    loop {
        let mut delete_dirs = FxHashSet::default();
        let mut add_dirs = Vec::new();

        let visit = |event: inotify::Event<'_>| {
            let is_delete_self = event.mask.contains(rustix::fs::inotify::ReadFlags::DELETE_SELF);
            let is_move_self = event.mask.contains(rustix::fs::inotify::ReadFlags::MOVE_SELF);
            let path = if is_delete_self || is_move_self {
                event.dir.clone()
            } else {
                let name = event.name.unwrap();
                event.dir.join(name)
            };

            let is_dir = event.mask.contains(rustix::fs::inotify::ReadFlags::ISDIR);
            let is_delete = event.mask.contains(rustix::fs::inotify::ReadFlags::DELETE);
            let is_create = event.mask.contains(rustix::fs::inotify::ReadFlags::CREATE);
            let is_move_from = event.mask.contains(rustix::fs::inotify::ReadFlags::MOVED_FROM);
            let is_move_to = event.mask.contains(rustix::fs::inotify::ReadFlags::MOVED_TO);

            if is_dir {
                if is_delete || is_move_from || is_delete_self || is_move_self {
                    delete_dirs.insert(path);
                } else if is_create || is_move_to {
                    let result = inotify.add_watch(
                        path.clone(),
                        rustix::fs::inotify::WatchFlags::DELETE_SELF
                            | rustix::fs::inotify::WatchFlags::DELETE
                            | rustix::fs::inotify::WatchFlags::CREATE
                            | rustix::fs::inotify::WatchFlags::MOVE
                            | rustix::fs::inotify::WatchFlags::MOVE_SELF,
                    );
                    if let Err(error) = result {
                        eprintln!("Failed to add watch for {}: {error}", path.display());
                    }
                    DIR_COUNTS.dir_counts.write().insert(path.clone(), QueueStats::default());
                    add_dirs.push(path);
                }
                return;
            }

            let Some(kind) = FileKind::from_path(&path) else { return };
            let parent = path.parent().unwrap();
            if is_delete || is_move_from {
                if let Some(counts) = DIR_COUNTS.dir_counts.write().get_mut(parent) {
                    counts.remove(kind);
                }
                DIR_COUNTS.total_counts.write().remove(kind);

                QUEUE.get().unwrap().remove(&path);
            } else if is_create || is_move_to {
                {
                    let mut dir_counts = DIR_COUNTS.dir_counts.write();
                    if !dir_counts.contains_key(parent) {
                        dir_counts.insert(parent.to_path_buf(), QueueStats::default());
                    }
                    DIR_COUNTS.dir_counts.write().get_mut(parent).unwrap().add(kind);
                }

                DIR_COUNTS.total_counts.write().add(kind);

                DIR_COUNTS.notify.notify(1);
            }
        };

        if let Err(error) = inotify.wait(visit) {
            eprintln!("Failed to wait for inotify events: {error}");
        }

        let queue = QUEUE.get().unwrap();

        {
            {
                let mut total_counts = DIR_COUNTS.total_counts.write();
                let mut dir_counts = DIR_COUNTS.dir_counts.write();

                {
                    let mut enabled_roots = queue.enabled_roots().write();
                    let mut disabled_roots = queue.disabled_roots().write();
                    for path in delete_dirs {
                        dir_counts.retain(|p, counts| {
                            if !p.starts_with(&path) {
                                return true;
                            }

                            total_counts.video_count -= counts.video_count;
                            total_counts.image_count -= counts.image_count;
                            total_counts.audio_count -= counts.audio_count;
                            false
                        });

                        enabled_roots.retain(|p| !p.starts_with(&path));
                        disabled_roots.retain(|p| !p.starts_with(&path));
                    }
                }
            }

            compio_rt.block_on(queue.refresh_roots());
        }

        let inotify_clone = inotify.clone();
        let filter = move |path: &Path, is_dir: bool| {
            if is_dir {
                let result = inotify_clone.add_watch(
                    path.to_path_buf(),
                    rustix::fs::inotify::WatchFlags::DELETE_SELF
                        | rustix::fs::inotify::WatchFlags::DELETE
                        | rustix::fs::inotify::WatchFlags::CREATE
                        | rustix::fs::inotify::WatchFlags::MOVE
                        | rustix::fs::inotify::WatchFlags::MOVE_SELF,
                );
                if let Err(error) = result {
                    eprintln!("Failed to add watch for {}: {error}", path.display());
                }
                DIR_COUNTS.dir_counts.write().insert(path.to_path_buf(), QueueStats::default());
                return true;
            }

            let Some(kind) = FileKind::from_path(path) else { return false };

            DIR_COUNTS.total_counts.write().add(kind);
            let parent = path.parent().unwrap();
            let mut dir_counts = DIR_COUNTS.dir_counts.write();
            if !dir_counts.contains_key(parent) {
                dir_counts.insert(parent.to_path_buf(), QueueStats::default());
            }
            dir_counts.get_mut(parent).unwrap().add(kind);

            // This is a new dir, so presumably the file is also new.
            DIR_COUNTS.notify.notify(1);

            true
        };

        compio_rt.block_on(async move {
            let rx = match walk_roots_filter(&add_dirs, None, filter).await {
                Ok(rx) => rx,
                Err(error) => {
                    eprintln!("Failed to walk roots: {error}");
                    return;
                }
            };

            while let Ok(_) = rx.recv_async().await {}
        });
    }
}

async fn precache_file<'p, P>(path: P) -> Result<(), std::io::Error>
where
    P: Into<Cow<'p, Utf8Path>>,
{
    let path = path.into();

    println!("Pre-caching file: {path}");

    let file = FILE_CACHE.open(path.as_ref()).await?;
    let mut size = file.size();
    let mut offset = 0;
    while size > 0 {
        let bytes = file.read_at(offset, file_cache::CachedFile::CHUNK_SIZE as usize).await?;

        if bytes.is_empty() {
            break;
        }

        size -= bytes.len() as u64;
        offset += bytes.len() as u64;
    }
    Ok(())
}

pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                // Wake the task immediately so the executor re-schedules it
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    YieldNow { yielded: false }.await;
}
