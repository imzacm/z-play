mod queue;
mod serve_dir;

use std::borrow::Cow;
use std::cell::{LazyCell, RefCell};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::OnceLock;
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
use compio::BufResult;
use compio::buf::IoBuf;
use futures_util::Stream;
use rustc_hash::{FxBuildHasher, FxHashSet};
use serde::{Deserialize, Serialize};
use z_play::random_files;
#[cfg(feature = "immich")]
use z_play::random_files_immich::{self, ImmichClient};

use self::queue::Queue;

const QUEUE_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-count");
const QUEUE_SIZE_HEADER: HeaderName = HeaderName::from_static("x-queue-size");
const QUEUE_VIDEO_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-video-count");
const QUEUE_IMAGE_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-image-count");
const QUEUE_AUDIO_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-audio-count");

// Pre-cache tuning:
// - Read up to this many bytes per queued file. Good default for mixed media: 4–16 MiB.
//
// 8 MiB
const PRECACHE_READ_BYTES: u64 = 8 * 1024 * 1024;

static QUEUE: OnceLock<Queue> = OnceLock::new();

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

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = compio::net::TcpListener::bind(address).await.unwrap();
    cyper_axum::serve(listener, app).await.unwrap();
}

async fn open_file<'p, P>(path: P) -> Result<Rc<compio::fs::File>, std::io::Error>
where
    P: Into<Cow<'p, Utf8Path>>,
{
    struct Entry {
        file: Rc<compio::fs::File>,
        expires_at: std::time::Instant,
    }

    type FileCache = lru::LruCache<Utf8PathBuf, Entry, FxBuildHasher>;

    const CAP: NonZeroUsize = NonZeroUsize::new(200).unwrap();
    const EXPIRY: Duration = Duration::from_secs(60);

    #[thread_local]
    static CACHE: LazyCell<RefCell<FileCache>> = LazyCell::new(|| {
        compio::runtime::spawn(async {
            loop {
                compio::time::sleep(Duration::from_secs(10)).await;

                let mut cache = CACHE.borrow_mut();
                let now = std::time::Instant::now();
                while let Some((_, entry)) = cache.peek_lru()
                    && entry.expires_at < now
                {
                    cache.pop_lru();
                }
            }
        })
        .detach();

        RefCell::new(lru::LruCache::with_hasher(CAP, FxBuildHasher))
    });

    let path = path.into();
    let now = std::time::Instant::now();

    {
        let mut cache = CACHE.borrow_mut();
        if let Some(entry) = cache.get_mut(path.as_ref()) {
            entry.expires_at = now + EXPIRY;
            return Ok(entry.file.clone());
        }
    }

    let file = compio::fs::File::open(path.as_ref()).await?;

    let mut cache = CACHE.borrow_mut();
    if let Some(entry) = cache.get_mut(path.as_ref()) {
        entry.expires_at = now + EXPIRY;
        return Ok(entry.file.clone());
    }

    let entry = Entry { file: Rc::new(file), expires_at: std::time::Instant::now() + EXPIRY };
    let file = entry.file.clone();
    cache.put(path.into_owned(), entry);

    Ok(file)
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

async fn precache_file<'p, P>(path: P) -> Result<(), std::io::Error>
where
    P: Into<Cow<'p, Utf8Path>>,
{
    use compio::io::AsyncReadExt;

    let path = path.into();

    println!("Pre-caching file: {path}");

    let file = open_file(path.as_ref()).await?;
    let file = std::io::Cursor::new(file);

    let mut take = file.take(PRECACHE_READ_BYTES);
    match compio::io::copy(&mut take, &mut WriteSink).await {
        Ok(n) => println!("Pre-cached {n} bytes from file {path}"),
        Err(error) => {
            println!("Failed to read file {path}: {error}");
            return Err(error);
        }
    }
    Ok(())
}

struct WriteSink;

impl compio::io::AsyncWrite for WriteSink {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        BufResult(Ok(buf.buf_len()), buf)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
