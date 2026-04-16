use std::num::NonZeroUsize;

use axum::body::Body;
use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use camino::Utf8Path;
use http::{StatusCode, header};

use super::FILE_CACHE;
use crate::http::file_cache::CachedFile;

#[axum::debug_handler]
pub async fn serve_dir(
    Path(path): Path<String>,
    request: axum::extract::Request,
) -> impl IntoResponse {
    let (result_tx, result_rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);
    let path = Utf8Path::new("/").join(path);

    let path_clone = path.clone();
    compio::runtime::spawn(async move {
        let result = FILE_CACHE.open(path_clone).await.map(|file| file.size());
        _ = result_tx.send_async(result).await;
    })
    .detach();

    let file_size = match result_rx.recv_async().await.unwrap() {
        Ok(size) => size,
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
        let file = FILE_CACHE.open(path).await.expect("Failed to open file");

        let mut current_offset = start;
        let buffer_size: usize = CachedFile::CHUNK_SIZE as usize;

        while current_offset <= end {
            // Calculate how much we have left to read
            let read_size = (end - current_offset + 1).min(buffer_size as u64) as usize;

            match file.read_at(current_offset, read_size).await {
                Ok(buffer) => {
                    if buffer.is_empty() {
                        break;
                    }

                    current_offset += buffer.len() as u64;

                    let item = Ok::<_, std::io::Error>(buffer);
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
