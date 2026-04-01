use std::num::NonZeroUsize;

use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use camino::Utf8Path;
use compio::BufResult;
use compio::io::AsyncReadAt;
use http::{StatusCode, header};

use super::open_file;

#[axum::debug_handler]
pub async fn serve_dir(
    Path(path): Path<String>,
    request: axum::extract::Request,
) -> impl IntoResponse {
    let (result_tx, result_rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);
    let path = Utf8Path::new("/").join(path);

    let path_clone = path.clone();
    compio::runtime::spawn(async move {
        let file = match open_file(path_clone).await {
            Ok(file) => file,
            Err(error) => {
                _ = result_tx.send_async(Err(error)).await;
                return;
            }
        };
        let result = file.metadata().await;
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
    let mut end = file_size.saturating_sub(1);
    let mut is_range = false;

    if let Some(range_hdr) = request.headers().get(header::RANGE) {
        if let Ok(range_str) = range_hdr.to_str() {
            if range_str.starts_with("bytes=") {
                let range = &range_str["bytes=".len()..];
                if let Some((start_str, end_str)) = range.split_once('-') {
                    if start_str.is_empty() {
                        // Suffix Range: `bytes=-500` (last 500 bytes)
                        if let Ok(suffix) = end_str.parse::<u64>() {
                            start = file_size.saturating_sub(suffix);
                            end = file_size.saturating_sub(1);
                            is_range = true;
                        }
                    } else {
                        // Standard / Prefix Range: `bytes=500-1000` or `bytes=500-`
                        if let Ok(s) = start_str.parse::<u64>() {
                            start = s;
                            is_range = true;
                            if let Ok(e) = end_str.parse::<u64>() {
                                end = e.min(file_size.saturating_sub(1));
                            }
                        }
                    }
                }
            }
        }
    }

    // Validate Range Bounds
    if start >= file_size || start > end {
        return (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{file_size}"))],
        )
            .into_response();
    }

    let chunk_size = end - start + 1;
    let mime = mime_guess::from_path(&path).first_or_octet_stream();

    let (tx, rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);

    compio::runtime::spawn(async move {
        let file = open_file(path).await.expect("Failed to open file");

        let mut current_offset = start;
        let buffer_size: usize = 1024 * 1024; // Stream in 1MB chunks

        while current_offset <= end {
            // Calculate how much we have left to read
            let read_size = (end - current_offset + 1).min(buffer_size as u64) as usize;

            let buffer = vec![0u8; read_size];

            // Perform the async read exactly at our current offset
            let BufResult(result, mut data) = file.read_at(buffer, current_offset).await;
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
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONNECTION, "keep-alive")
        .header(header::CACHE_CONTROL, "public, max-age=31536000");

    // If it's a range request, we MUST return a 206 Partial Content status
    if is_range {
        response = response
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{file_size}"))
            .header(header::CONTENT_LENGTH, chunk_size);
    } else {
        response = response.status(StatusCode::OK).header(header::CONTENT_LENGTH, file_size);
    }

    let stream = rx.into_stream();
    response.body(Body::from_stream(stream)).unwrap().into_response()
}
