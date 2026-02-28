use std::collections::VecDeque;
use std::path::PathBuf;

use rustc_hash::FxHashSet;

const CACHE_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub struct PathCache {
    set: FxHashSet<PathBuf>,
    queue: VecDeque<PathBuf>,
}

impl PathCache {
    /// Returns true if the path was not already in the cache.
    ///
    /// Removes the value if it was already in the cache.
    pub fn insert_or_remove(&mut self, path: PathBuf) -> bool {
        let contains_value = self.set.contains(&path);
        if contains_value {
            self.set.remove(&path);
        } else {
            self.set.insert(path.clone());
            self.queue.push_back(path);
        }

        while self.queue.len() > CACHE_SIZE {
            let value = self.queue.pop_front().unwrap();
            self.set.remove(&value);
        }

        !contains_value
    }
}

impl Default for PathCache {
    fn default() -> Self {
        let mut set = FxHashSet::default();
        set.reserve(CACHE_SIZE);
        Self { set, queue: VecDeque::with_capacity(CACHE_SIZE) }
    }
}
