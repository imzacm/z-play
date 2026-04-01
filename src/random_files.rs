use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rand::RngExt;

use crate::walkdir::walk_roots_filter;

pub async fn random_file(roots: &[PathBuf]) -> Option<PathBuf> {
    random_file_with_timeout(roots, Duration::from_secs(2)).await
}

pub async fn random_file_filter<F>(roots: &[PathBuf], filter: F) -> Option<PathBuf>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    random_file_with_timeout_filter(roots, Duration::from_secs(2), filter).await
}

pub async fn random_file_with_timeout(roots: &[PathBuf], timeout: Duration) -> Option<PathBuf> {
    random_file_with_timeout_filter(roots, timeout, |_, _| true).await
}

pub async fn random_file_with_timeout_filter<F>(
    roots: &[PathBuf],
    timeout: Duration,
    filter: F,
) -> Option<PathBuf>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    let deadline = Instant::now() + timeout;
    let path_rx = walk_roots_filter(roots, Some(deadline), filter)
        .await
        .expect("Failed to walk roots");

    let mut result = ScanResult::default();
    let mut rng = rand::rng();
    while let Ok(path) = path_rx.recv_async().await {
        let scan_result = ScanResult { selected: Some(path), count: 1 };
        result = reduce_scan_result(result, scan_result, &mut rng);
    }
    result.selected
}

#[derive(Debug)]
pub struct ScanResult<T> {
    pub selected: Option<T>,
    pub count: u64,
}

impl<T> Default for ScanResult<T> {
    fn default() -> Self {
        Self { selected: None, count: 0 }
    }
}

pub fn reduce_scan_result<T, R>(
    mut a: ScanResult<T>,
    b: ScanResult<T>,
    rng: &mut R,
) -> ScanResult<T>
where
    R: rand::Rng,
{
    let total_count = a.count.saturating_add(b.count);

    // If one side is empty, just return the other
    if total_count == 0 {
        return ScanResult::default();
    }
    if a.count == 0 {
        return b;
    }
    if b.count == 0 {
        return a;
    }

    // Weighted random choice to decide which "selected" item to keep.
    // Choose 'a's sample with probability a.count / total_count
    if rng.random_range(0..total_count) < a.count {
        a.count = total_count;
        a
    } else {
        // Need to create a new struct to take ownership of b.selected
        ScanResult { selected: b.selected, count: total_count }
    }
}
