use std::path::PathBuf;
use std::time::{Duration, Instant};

use rand::RngExt;

pub fn random_file(roots: &[PathBuf]) -> Option<PathBuf> {
    random_file_with_timeout(roots, Duration::from_secs(2))
}

pub fn random_file_with_timeout(roots: &[PathBuf], timeout: Duration) -> Option<PathBuf> {
    let (path_tx, path_rx) = z_queue::defaults::unbounded();

    let deadline = Instant::now() + timeout;
    for root in roots {
        let path_tx = path_tx.clone();
        let root = root.clone();
        rayon::spawn(move || {
            let rx = crate::walkdir::walkdir(root, Some(deadline));
            let mut result = ScanResult::default();
            for path_result in rx {
                match path_result {
                    Ok(path) => {
                        result = reduce_scan_result(
                            result,
                            ScanResult { selected: Some(path), count: 1 },
                        );
                    }
                    Err(error) => {
                        eprintln!("Error walking directory: {error:?}");
                    }
                }
            }

            _ = path_tx.send(result);
        });
    }
    drop(path_tx);

    let mut result = ScanResult::default();
    for scan_result in path_rx {
        result = reduce_scan_result(result, scan_result);
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

pub fn reduce_scan_result<T>(mut a: ScanResult<T>, b: ScanResult<T>) -> ScanResult<T> {
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
    let mut rng = rand::rng();
    if rng.random_range(0..total_count) < a.count {
        a.count = total_count;
        a
    } else {
        // Need to create a new struct to take ownership of b.selected
        ScanResult { selected: b.selected, count: total_count }
    }
}
