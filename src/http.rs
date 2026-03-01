use std::borrow::Cow;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::Request;
use axum::http::header::{CACHE_CONTROL, EXPIRES, PRAGMA};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, patch};
use axum::{Router, middleware};
use axum_extra::extract::Query;
use parking_lot::RwLock;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;
use z_play::path_cache::PathCache;
use z_play::random_files::random_file_with_timeout;

const QUEUE_SIZE: usize = 1000;
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
// 1 MiB
const PRECACHE_CHUNK_BYTES: usize = 1024 * 1024;

static QUEUE: OnceLock<Queue> = OnceLock::new();

struct Queue {
    enabled_roots: RwLock<Vec<PathBuf>>,
    disabled_roots: RwLock<Vec<PathBuf>>,
    tx: flume::Sender<PathBuf>,
    rx: flume::Receiver<PathBuf>,
    stats: QueueStats,
}

impl Queue {
    fn new(roots: Vec<PathBuf>) -> Self {
        let (tx, rx) = flume::bounded(QUEUE_SIZE);
        let len = roots.len();
        Self {
            enabled_roots: RwLock::new(roots),
            disabled_roots: RwLock::new(Vec::with_capacity(len)),
            tx,
            rx,
            stats: QueueStats::new(),
        }
    }
}

#[derive(Debug)]
struct QueueStats {
    video_count: AtomicUsize,
    image_count: AtomicUsize,
    audio_count: AtomicUsize,
}

impl QueueStats {
    const fn new() -> Self {
        Self {
            video_count: AtomicUsize::new(0),
            image_count: AtomicUsize::new(0),
            audio_count: AtomicUsize::new(0),
        }
    }

    fn push_path(&self, path: &Path) {
        match FileKind::from_path(path) {
            Some(FileKind::Video) => self.video_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Image) => self.image_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Audio) => self.audio_count.fetch_add(1, Ordering::Relaxed),
            None => 0,
        };
    }

    fn remove_path(&self, path: &Path) {
        match FileKind::from_path(path) {
            Some(FileKind::Video) => self.video_count.fetch_sub(1, Ordering::Relaxed),
            Some(FileKind::Image) => self.image_count.fetch_sub(1, Ordering::Relaxed),
            Some(FileKind::Audio) => self.audio_count.fetch_sub(1, Ordering::Relaxed),
            None => 0,
        };
    }

    fn reset(&self) {
        self.video_count.store(0, Ordering::Relaxed);
        self.image_count.store(0, Ordering::Relaxed);
        self.audio_count.store(0, Ordering::Relaxed);
    }
}

pub fn start_server(port: u16, roots: Vec<PathBuf>) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
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
        .nest_service(
            "/files",
            ServiceBuilder::new()
                // .layer(CompressionLayer::new())
                .layer(SetResponseHeaderLayer::if_not_present(
                    CACHE_CONTROL,
                    HeaderValue::from_static("public, max-age=31536000"),
                ))
                .layer(middleware::from_fn(validate_path_middleware))
                .service(ServeDir::new("/")),
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

    let queue_tx = QUEUE.get_or_init(move || Queue::new(roots)).tx.clone();
    std::thread::spawn(move || queue_feeder(queue_tx));

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
enum FileKind {
    Video,
    Audio,
    Image,
}

impl FileKind {
    fn from_path<P>(path: P) -> Option<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let extension = path.extension()?.to_str()?;

        let extension = if extension.chars().all(|c| c.is_ascii_lowercase()) {
            Cow::Borrowed(extension)
        } else {
            Cow::Owned(extension.to_ascii_lowercase())
        };

        Self::from_extension(&extension)
    }

    fn from_extension(extension: &str) -> Option<Self> {
        match extension.to_lowercase().as_str() {
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "svg" | "tiff" => Some(Self::Image),
            "mp4" | "mkv" | "webm" | "avi" | "mov" | "wmv" | "flv" | "mpeg" => Some(Self::Video),
            "mp3" | "wav" | "ogg" | "m4a" | "flac" | "aac" => Some(Self::Audio),
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
    let enabled: Vec<_> = {
        let roots = queue.enabled_roots.read();
        roots.iter().map(|root| root.to_string_lossy().into_owned()).collect()
    };
    let disabled: Vec<_> = {
        let roots = queue.disabled_roots.read();
        roots.iter().map(|root| root.to_string_lossy().into_owned()).collect()
    };
    let roots: Vec<_> = enabled
        .into_iter()
        .map(|path| RootJson { path, enabled: true })
        .chain(disabled.into_iter().map(|path| RootJson { path, enabled: false }))
        .collect();
    (queue_info(), Json(roots))
}

async fn patch_roots(body: Json<Vec<RootJson>>) -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    let mut enabled_roots = queue.enabled_roots.write();
    let mut disabled_roots = queue.disabled_roots.write();

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

    for path in queue.rx.try_iter() {
        if !enabled_roots.iter().any(|root| path.starts_with(root)) {
            queue.stats.remove_path(&path);
            continue;
        }
        tokio::spawn(async move {
            queue.tx.send_async(path).await.expect("Queue channel disconnected");
        });
    }

    (queue_info(), StatusCode::NO_CONTENT)
}

async fn random_path_handler(query: Query<RandomQuery>) -> impl IntoResponse {
    let RandomQuery { kinds, roots } = query.0;

    let queue = QUEUE.get().unwrap();

    let path = loop {
        let path = queue.rx.recv_async().await.expect("Queue channel disconnected");

        if !roots.is_empty() && !roots.iter().any(|root| path.starts_with(root)) {
            queue.stats.remove_path(&path);
            continue;
        }

        if !kinds.is_empty()
            && let Some(file_kind) = FileKind::from_path(&path)
            && !kinds.contains(&file_kind)
        {
            tokio::spawn(async move {
                let queue = QUEUE.get().unwrap();
                queue.tx.send_async(path).await.expect("Queue channel disconnected");
            });
            continue;
        }

        let future = precache_file(&path);
        match tokio::time::timeout(Duration::from_millis(100), future).await {
            Ok(Ok(())) => break path,
            Ok(Err(_)) => (),
            Err(_) => {
                println!("Pre-caching file {} timed out", path.display());
                tokio::spawn(async move {
                    let queue = QUEUE.get().unwrap();
                    queue.tx.send_async(path).await.expect("Queue channel disconnected");
                });
            }
        }
    };

    queue.stats.remove_path(&path);
    let path_str = path.to_str().expect("Only UTF-8 paths are supported");

    (
        [
            (CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (PRAGMA, "no-cache"),
            (EXPIRES, "0"),
        ],
        queue_info(),
        path_str.to_string(),
    )
}

async fn queue_info_handler() -> [(HeaderName, String); 5] {
    queue_info()
}

fn queue_info() -> [(HeaderName, String); 5] {
    let queue = QUEUE.get().unwrap();

    let video_count = queue.stats.video_count.load(Ordering::Relaxed);
    let image_count = queue.stats.image_count.load(Ordering::Relaxed);
    let audio_count = queue.stats.audio_count.load(Ordering::Relaxed);
    [
        (QUEUE_COUNT_HEADER, queue.rx.len().to_string()),
        (QUEUE_SIZE_HEADER, QUEUE_SIZE.to_string()),
        (QUEUE_VIDEO_COUNT_HEADER, video_count.to_string()),
        (QUEUE_IMAGE_COUNT_HEADER, image_count.to_string()),
        (QUEUE_AUDIO_COUNT_HEADER, audio_count.to_string()),
    ]
}

async fn reset_queue_handler() -> impl IntoResponse {
    let queue = QUEUE.get().unwrap();

    for _ in queue.rx.try_iter() {}
    queue.stats.reset();
    StatusCode::NO_CONTENT
}

async fn validate_path_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let path_query = request.uri().path();
    let decoded_path = urlencoding::decode(path_query).map_err(|_| StatusCode::BAD_REQUEST)?;
    let requested_path = Path::new(decoded_path.as_ref());

    let is_valid = {
        let queue = QUEUE.get().unwrap();
        let roots = queue.enabled_roots.read();
        roots.iter().any(|root| requested_path.starts_with(root))
    };
    if is_valid { Ok(next.run(request).await) } else { Err(StatusCode::NOT_FOUND) }
}

fn queue_feeder(queue_tx: flume::Sender<PathBuf>) {
    println!("Starting queue feeder");
    let queue = QUEUE.get().unwrap();
    let mut cache = PathCache::default();
    'main: loop {
        let queue_len = queue_tx.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let mut timeout_ms = 100 + (9900 * queue_len / QUEUE_SIZE);

        let path = loop {
            let roots = queue.enabled_roots.read();
            while roots.is_empty() {
                drop(roots);
                println!("No enabled roots, sleeping for 1s");
                std::thread::sleep(Duration::from_secs(1));
                continue 'main;
            }

            let busy_timeout = Duration::from_millis(timeout_ms as u64);
            let scan_timeout = busy_timeout * 2;

            let path = if scan_timeout.as_secs() > 1 {
                let roots_clone = roots.clone();
                drop(roots);
                random_file_with_timeout(&roots_clone, scan_timeout, busy_timeout)
            } else {
                random_file_with_timeout(&roots, scan_timeout, busy_timeout)
            };

            match path {
                Some(path) => break path,
                None => {
                    println!("No files found, increasing timeout");
                    timeout_ms += 1000;
                }
            }
        };

        if cache.insert_or_remove(path.clone()) {
            queue_tx.send(path.clone()).expect("Queue channel disconnected");
            queue.stats.push_path(&path);
        }
    }
}

async fn precache_file(path: &Path) -> Result<(), std::io::Error> {
    use tokio::io::AsyncReadExt;

    println!("Pre-caching file: {}", path.display());

    let mut file = tokio::fs::File::open(path).await?;

    let mut remaining = PRECACHE_READ_BYTES;
    let mut buffer = vec![0u8; PRECACHE_CHUNK_BYTES];

    while remaining != 0 {
        match file.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => remaining = remaining.saturating_sub(n as u64),
            Err(error) => {
                println!("Failed to read file {}: {error}", path.display());
                return Err(error);
            }
        }
    }
    println!("Pre-cached file: {}", path.display());
    Ok(())
}
