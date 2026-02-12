use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

pub fn random_file<R>(roots: R) -> Option<PathBuf>
where
    R: Into<Cow<'static, [PathBuf]>>,
{
    random_file_with_timeout(roots, Duration::from_secs(2), Duration::from_secs(1))
}

pub fn random_file_with_timeout<R>(
    roots: R,
    scan_timeout: Duration,
    busy_timeout: Duration,
) -> Option<PathBuf>
where
    R: Into<Cow<'static, [PathBuf]>>,
{
    let (path_tx, path_rx) = flume::unbounded();

    let roots = roots.into();
    std::thread::spawn(move || {
        let deadline = Instant::now() + scan_timeout;
        let cancel = Arc::new(AtomicBool::new(false));

        let path_tx = Mutex::new(Some(path_tx));
        roots
            .par_iter()
            .take_any_while(|_| !cancel.load(Ordering::Relaxed) && Instant::now() <= deadline)
            .map(|root| scan_root(root.as_ref(), deadline, busy_timeout, cancel.clone()))
            .for_each(|result| {
                if cancel.load(Ordering::Relaxed) || Instant::now() > deadline {
                    *path_tx.lock() = None;
                }
                if let Some(path_tx) = &*path_tx.lock() {
                    _ = path_tx.send(result);
                }
            });
    });

    let result = path_rx.into_iter().reduce(reduce_scan_result);

    result.and_then(|result| result.selected)
}

#[derive(Debug)]
struct ScanResult<T> {
    selected: Option<T>,
    count: u64,
}

impl<T> Default for ScanResult<T> {
    fn default() -> Self {
        Self { selected: None, count: 0 }
    }
}

fn scan_root(
    path: &Path,
    deadline: Instant,
    busy_timeout: Duration,
    cancel: Arc<AtomicBool>,
) -> ScanResult<PathBuf> {
    if cancel.load(Ordering::Relaxed) || Instant::now() > deadline {
        cancel.store(true, Ordering::Relaxed);
        return ScanResult::default();
    }

    let Ok(metadata) = std::fs::metadata(path) else { return ScanResult::default() };
    if !metadata.file_type().is_dir() {
        return ScanResult { selected: Some(path.to_path_buf()), count: 1 };
    }

    let walk_dir = jwalk::WalkDir::new(path)
        .parallelism(jwalk::Parallelism::RayonDefaultPool { busy_timeout });

    walk_dir
        .into_iter()
        .par_bridge()
        .take_any_while(|_| {
            if !cancel.load(Ordering::Relaxed) && Instant::now() <= deadline {
                true
            } else {
                cancel.store(true, Ordering::Relaxed);
                false
            }
        })
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().is_dir() {
                return None;
            }
            Some(ScanResult { selected: Some(entry.path()), count: 1 })
        })
        .reduce(ScanResult::default, reduce_scan_result)
}

fn reduce_scan_result(mut a: ScanResult<PathBuf>, b: ScanResult<PathBuf>) -> ScanResult<PathBuf> {
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
