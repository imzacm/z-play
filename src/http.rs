use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Router, middleware};
use tower::ServiceBuilder;
use tower_http::services::ServeDir;

const QUEUE_SIZE: usize = 1000;

static ROOTS: OnceLock<Vec<PathBuf>> = OnceLock::new();
static QUEUE_RX: OnceLock<flume::Receiver<PathBuf>> = OnceLock::new();

pub fn start_server(port: u16, roots: Vec<PathBuf>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            start_server_inner(port, roots).await;
        });
}

async fn start_server_inner(port: u16, roots: Vec<PathBuf>) {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/random", get(random_path_handler))
        .nest_service(
            "/files",
            ServiceBuilder::new()
                .layer(middleware::from_fn(validate_path_middleware))
                .service(ServeDir::new("/")),
        );

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
            (axum::http::header::CACHE_CONTROL, "no-cache, no-store, must-revalidate"),
            (axum::http::header::PRAGMA, "no-cache"),
            (axum::http::header::EXPIRES, "0"),
        ],
        path_str.to_string(),
    )
}

async fn validate_path_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let path_query = request.uri().path();
    let decoded_path = urlencoding::decode(path_query).map_err(|_| StatusCode::BAD_REQUEST)?;
    let requested_path = PathBuf::from(decoded_path.into_owned());

    let canonical_requested = requested_path.canonicalize().map_err(|_| StatusCode::NOT_FOUND)?;

    let roots = ROOTS.get().unwrap();

    let is_valid = roots.iter().any(|root| {
        if let Ok(canonical_root) = root.canonicalize() {
            canonical_requested.starts_with(canonical_root)
        } else {
            false
        }
    });

    if is_valid { Ok(next.run(request).await) } else { Err(StatusCode::NOT_FOUND) }
}

fn queue_feeder(queue_tx: flume::Sender<PathBuf>) {
    let roots = ROOTS.get().unwrap();
    loop {
        let queue_len = queue_tx.len();
        // Start at 100ms and scale up to 10s based on queue length.
        let timeout_ms = 100 + (9900 * queue_len / QUEUE_SIZE);
        let timeout = Duration::from_millis(timeout_ms as u64);
        let path =
            z_play::random_files::random_file_with_timeout(roots, timeout).expect("No files found");
        queue_tx.send(path).expect("Queue channel disconnected");
    }
}
