mod queue;

use std::borrow::Cow;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::Request;
use axum::http::header::{CACHE_CONTROL, EXPIRES, PRAGMA};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, patch};
use axum::{Router, middleware};
use axum_extra::extract::Query;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;
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
        .route("/shuffle", get(shuffle_queue_handler))
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

    let queue = QUEUE.get_or_init(move || Queue::new(roots));

    #[cfg(feature = "immich")]
    tokio::spawn(immich_queue_feeder(queue));

    std::thread::spawn(move || queue_feeder(queue, None));

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
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

    fn from_path<P>(path: P) -> Option<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let extension = path.extension()?.to_str()?;

        let extension = if extension.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
            Cow::Borrowed(extension)
        } else {
            Cow::Owned(extension.to_ascii_lowercase())
        };

        Self::from_extension(&extension)
    }

    fn from_extension(extension: &str) -> Option<Self> {
        assert!(extension.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
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

    let (enabled_roots, disabled_roots) =
        tokio::join!(queue.enabled_roots().read_async(), queue.disabled_roots().read_async());

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

    let (mut enabled_roots, mut disabled_roots) =
        tokio::join!(queue.enabled_roots().write_async(), queue.disabled_roots().write_async());

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

    let mut counter = 0;
    let (path, file_kind) = loop {
        if queue.len() <= 5 {
            std::thread::spawn(|| queue_feeder(queue, Some(5)));
        }

        if counter > (Queue::QUEUE_SIZE * 3) {
            panic!("Queue does not contain files matching filter");
        }
        counter += 1;

        let filter_kinds = if kinds.is_empty() { None } else { Some(&kinds) };
        let filter_roots = if roots.is_empty() { None } else { Some(&roots) };
        let (path, file_kind) = queue.find_pop_async(filter_kinds, filter_roots).await;

        let future = precache_file(&path);
        match tokio::time::timeout(Duration::from_millis(100), future).await {
            Ok(Ok(())) => break (path, file_kind),
            Ok(Err(_)) => (),
            Err(_) => {
                println!("Pre-caching file {} timed out", path.display());
                tokio::spawn(queue.push_async(path));
            }
        }
    };

    let path_str = path.to_str().expect("Only UTF-8 paths are supported");

    (
        [
            (CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (PRAGMA, "no-cache"),
            (EXPIRES, "0"),
        ],
        queue_info(),
        Json(PathResponse { path: path_str.into(), kind: file_kind }),
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
    std::thread::spawn(|| queue_feeder(queue, Some(5)));
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
        let mut timeout_ms = 100 + (9900 * queue_len / Queue::QUEUE_SIZE);

        let path = loop {
            let mut roots = queue.enabled_roots().read_async().await.clone();
            if roots.is_empty() {
                println!("No enabled roots, sleeping for 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue 'main;
            }

            {
                use rand::seq::SliceRandom;

                let mut rng = rand::rng();
                roots.shuffle(&mut rng);
            }

            let timeout = Duration::from_millis(timeout_ms as u64) * 2;

            let path =
                random_files_immich::random_file_with_timeout(&immich, &roots, timeout).await;
            match path {
                Some(path) => break path,
                None => {
                    timeout_ms += 1000;
                    println!("No files found, increasing timeout to {timeout_ms}ms");
                }
            }
        };

        queue.push_async(path).await;
    }
}

fn queue_feeder(queue: &Queue, max_count: Option<usize>) {
    println!("Starting queue feeder");
    let mut counter = 0;
    'main: loop {
        let queue_len = queue.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let mut timeout_ms = 100 + (9900 * queue_len / Queue::QUEUE_SIZE);

        let path = loop {
            let mut roots = queue.enabled_roots().read().clone();
            if roots.is_empty() {
                println!("No enabled roots, sleeping for 1s");
                std::thread::sleep(Duration::from_secs(1));
                continue 'main;
            }

            {
                use rand::seq::SliceRandom;

                let mut rng = rand::rng();
                roots.shuffle(&mut rng);
            }

            let busy_timeout = Duration::from_millis(timeout_ms as u64);
            let scan_timeout = busy_timeout * 2;

            let path = random_files::random_file_with_timeout(&roots, scan_timeout, busy_timeout);

            match path {
                Some(path) => break path,
                None => {
                    timeout_ms += 1000;
                    println!("No files found, increasing timeout to {timeout_ms}ms");
                }
            }
        };

        queue.push(path);

        if max_count.is_some_and(|v| counter >= v) {
            break;
        }
        counter += 1;
    }
}

async fn precache_file(path: &Path) -> Result<(), std::io::Error> {
    use tokio::io::AsyncReadExt;

    println!("Pre-caching file: {}", path.display());

    let file = tokio::fs::File::open(path).await?;

    let mut take = file.take(PRECACHE_READ_BYTES);
    match tokio::io::copy(&mut take, &mut tokio::io::sink()).await {
        Ok(n) => println!("Pre-cached {n} bytes from file {}", path.display()),
        Err(error) => {
            println!("Failed to read file {}: {error}", path.display());
            return Err(error);
        }
    }
    Ok(())
}
