use std::path::{Path, PathBuf};

use rand::Rng;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

#[derive(Debug, Clone)]
pub struct RandomFiles {
    roots: Vec<PathBuf>,
}

impl RandomFiles {
    pub fn new<I>(root_dirs: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let roots: Vec<_> = root_dirs.into_iter().map(Into::into).collect();
        Self { roots }
    }
}

impl Iterator for RandomFiles {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        self.roots.shuffle(&mut rand::rng());
        let results = self.roots.par_iter().map(|p| scan_root(p)).collect::<Vec<_>>();

        let total_files = results.iter().map(|r| r.count).sum();
        if total_files == 0 {
            return None;
        }

        let mut rng = rand::rng();
        let mut index = rng.random_range(0..total_files);
        for result in results {
            if index < result.count {
                return result.selected;
            }

            index -= result.count;
        }
        None
    }
}

struct ScanResult<T> {
    selected: Option<T>,
    count: u64,
}

fn scan_root(path: &Path) -> ScanResult<PathBuf> {
    let identity = || ScanResult { selected: None, count: 0 };

    let Ok(metadata) = std::fs::metadata(path) else { return identity() };
    if !metadata.file_type().is_dir() {
        return ScanResult { selected: Some(path.to_path_buf()), count: 1 };
    }

    let walk_dir = jwalk::WalkDir::new(path).parallelism(jwalk::Parallelism::RayonDefaultPool {
        busy_timeout: std::time::Duration::from_secs(1),
    });

    let reduce = |mut a: ScanResult<PathBuf>, b: ScanResult<PathBuf>| -> ScanResult<PathBuf> {
        let total_count = a.count.saturating_add(b.count);

        // If one side is empty, just return the other
        if total_count == 0 {
            return identity();
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
    };

    walk_dir
        .into_iter()
        .par_bridge()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().is_dir() {
                return None;
            }
            Some(ScanResult { selected: Some(entry.path()), count: 1 })
        })
        .reduce(identity, reduce)
}
