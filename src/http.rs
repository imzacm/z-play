use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::Request;
use axum::http::header::{CACHE_CONTROL, EXPIRES, PRAGMA};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Router, middleware};
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;
use z_play::path_cache::PathCache;
use z_play::random_files::random_file_with_timeout;

const QUEUE_SIZE: usize = 1000;
const QUEUE_COUNT_HEADER: HeaderName = HeaderName::from_static("x-queue-count");
const QUEUE_SIZE_HEADER: HeaderName = HeaderName::from_static("x-queue-size");

static ROOTS: OnceLock<Vec<PathBuf>> = OnceLock::new();
static QUEUE_RX: OnceLock<flume::Receiver<PathBuf>> = OnceLock::new();

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
        .route("/random", get(random_path_handler))
        .route("/queue", get(queue_info))
        .route("/reset", get(reset_queue_handler))
        .nest_service(
            "/files",
            ServiceBuilder::new()
                .layer(CompressionLayer::new())
                .layer(SetResponseHeaderLayer::if_not_present(
                    CACHE_CONTROL,
                    HeaderValue::from_static("public, max-age=31536000"),
                ))
                .layer(middleware::from_fn(validate_path_middleware))
                .service(ServeDir::new("/")),
        );

    roots.retain_mut(|root| match root.canonicalize() {
        Ok(path) => {
            *root = path;
            true
        }
        Err(error) => {
            eprintln!("Removing invalid root \"{}\": {error}", root.display());
            false
        }
    });

    ROOTS.get_or_init(move || roots);

    let (queue_tx, queue_rx) = flume::bounded(QUEUE_SIZE);
    QUEUE_RX.get_or_init(move || queue_rx);
    std::thread::spawn(move || queue_feeder(queue_tx));

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn random_path_handler() -> impl IntoResponse {
    let queue_rx = QUEUE_RX.get().unwrap();
    let path = queue_rx.recv_async().await.expect("Queue channel disconnected");
    let path_str = path.to_str().expect("Only UTF-8 paths are supported");

    (
        [
            (CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (PRAGMA, "no-cache"),
            (EXPIRES, "0"),
        ],
        [
            (QUEUE_COUNT_HEADER, queue_rx.len().to_string()),
            (QUEUE_SIZE_HEADER, QUEUE_SIZE.to_string()),
        ],
        path_str.to_string(),
    )
}

async fn queue_info() -> impl IntoResponse {
    let queue_rx = QUEUE_RX.get().unwrap();
    [(QUEUE_COUNT_HEADER, queue_rx.len().to_string()), (QUEUE_SIZE_HEADER, QUEUE_SIZE.to_string())]
}

async fn reset_queue_handler() -> impl IntoResponse {
    for _ in QUEUE_RX.get().unwrap().try_iter() {}
    StatusCode::NO_CONTENT
}

async fn validate_path_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let path_query = request.uri().path();
    let decoded_path = urlencoding::decode(path_query).map_err(|_| StatusCode::BAD_REQUEST)?;
    let requested_path = Path::new(decoded_path.as_ref());

    let roots = ROOTS.get().unwrap();

    let is_valid = roots.iter().any(|root| requested_path.starts_with(root));
    if is_valid { Ok(next.run(request).await) } else { Err(StatusCode::NOT_FOUND) }
}

fn queue_feeder(queue_tx: flume::Sender<PathBuf>) {
    let roots = ROOTS.get().unwrap();
    let mut cache = PathCache::default();
    loop {
        let queue_len = queue_tx.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let mut timeout_ms = 100 + (9900 * queue_len / QUEUE_SIZE);
        let path = loop {
            let busy_timeout = Duration::from_millis(timeout_ms as u64);
            let scan_timeout = busy_timeout * 2;
            match random_file_with_timeout(roots, scan_timeout, busy_timeout) {
                Some(path) => break path,
                None => {
                    eprintln!("No files found, increasing timeout");
                    timeout_ms += 1000;
                }
            }
        };

        if cache.insert_or_remove(path.clone()) {
            queue_tx.send(path).expect("Queue channel disconnected");
        }
    }
}
