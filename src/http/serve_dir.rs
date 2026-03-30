use std::num::NonZeroUsize;

use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use compio::BufResult;
use compio::fs::File;
use compio::io::AsyncReadAt;
use http::{StatusCode, header};

#[axum::debug_handler]
pub async fn serve_dir(
    Path(path): Path<String>,
    request: axum::extract::Request,
) -> impl IntoResponse {
    let (result_tx, result_rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);
    let path = std::path::Path::new("/").join(path);

    let path_clone = path.clone();
    compio::runtime::spawn(async move {
        let result = compio::fs::metadata(&path_clone).await;
        _ = result_tx.send_async(result).await;
    })
    .detach();

    let file_size = match result_rx.recv_async().await.unwrap() {
        Ok(metadata) => metadata.len(),
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("IO Error: {error:?}"))
                .into_response();
        }
    };

    // Parse the HTTP Range header
    let mut start = 0;
    let mut end = file_size - 1;
    let mut is_range = false;

    let headers = request.headers();
    if let Some(range_hdr) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_hdr.to_str() {
            if range_str.starts_with("bytes=") {
                let parts: Vec<&str> = range_str["bytes=".len()..].split('-').collect();
                if let Some(start_str) = parts.get(0) {
                    if let Ok(s) = start_str.parse::<u64>() {
                        start = s;
                        is_range = true;
                    }
                }
                if let Some(end_str) = parts.get(1) {
                    if let Ok(e) = end_str.parse::<u64>() {
                        end = e.min(file_size - 1);
                    }
                }
            }
        }
    }

    let chunk_size = end - start + 1;
    let mime = mime_guess::from_path(&path).first_or_octet_stream();

    let (tx, rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);

    compio::runtime::spawn(async move {
        let file = File::open(&path).await.expect("Failed to open file");

        let mut current_offset = start;
        let buffer_size = 64 * 1024; // Stream in 64KB chunks

        while current_offset <= end {
            // Calculate how much we have left to read
            let read_size = (end - current_offset + 1).min(buffer_size) as usize;

            // Allocate a buffer for Compio to write into
            let buf = vec![0u8; read_size];

            // Perform the async read exactly at our current offset
            let BufResult(result, mut data) = file.read_at(buf, current_offset).await;
            match result {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        break;
                    } // EOF reached

                    current_offset += bytes_read as u64;

                    // Truncate the buffer just in case we read less than expected
                    data.truncate(bytes_read);

                    let item = Ok::<_, std::io::Error>(Bytes::from(data));
                    if tx.send_async(item).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    eprintln!("Error reading file at {current_offset}: {error:?}");
                    _ = tx.send_async(Err(error)).await;
                    break;
                }
            }
        }
    })
    .detach();

    // Build the correct HTTP Response
    let mut response = Response::builder()
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::ACCEPT_RANGES, "bytes");

    // If it's a range request, we MUST return a 206 Partial Content status
    if is_range {
        response = response
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{file_size}"))
            .header(header::CONTENT_LENGTH, chunk_size);
    } else {
        response = response.status(StatusCode::OK).header(header::CONTENT_LENGTH, file_size);
    }

    response = response.header(header::CACHE_CONTROL, "public, max-age=31536000");

    let stream = rx.into_stream();
    response.body(Body::from_stream(stream)).unwrap().into_response()
}
