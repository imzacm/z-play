use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashSet;
use z_queue::ZQueue;

use crate::http::FileKind;

pub struct Queue {
    enabled_roots: RwLock<Vec<PathBuf>>,
    disabled_roots: RwLock<Vec<PathBuf>>,
    queue: ZQueue<(PathBuf, FileKind)>,
    stats: QueueStats<AtomicUsize>,
    queued_files: Mutex<FxHashSet<PathBuf>>,
}

impl Queue {
    pub const QUEUE_SIZE: usize = 1000;

    pub fn new(roots: Vec<PathBuf>) -> Self {
        let len = roots.len();
        let mut queued_files = FxHashSet::default();
        queued_files.reserve(Self::QUEUE_SIZE);
        Self {
            enabled_roots: RwLock::new(roots),
            disabled_roots: RwLock::new(Vec::with_capacity(len)),
            queue: ZQueue::bounded(Self::QUEUE_SIZE),
            stats: QueueStats::default(),
            queued_files: Mutex::new(queued_files),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn stats(&self) -> QueueStats<usize> {
        let video_count = self.stats.video_count.load(Ordering::Relaxed);
        let image_count = self.stats.image_count.load(Ordering::Relaxed);
        let audio_count = self.stats.audio_count.load(Ordering::Relaxed);
        QueueStats { video_count, image_count, audio_count }
    }

    pub fn enabled_roots(&self) -> &RwLock<Vec<PathBuf>> {
        &self.enabled_roots
    }

    pub fn disabled_roots(&self) -> &RwLock<Vec<PathBuf>> {
        &self.disabled_roots
    }

    pub async fn push_async(&self, path: PathBuf) {
        {
            let mut queued_files = self.queued_files.lock();
            if queued_files.contains(&path) {
                return;
            }
            queued_files.insert(path.clone());
        }
        let Some(file_kind) = FileKind::from_path(&path) else {
            eprintln!("Unknown file type: {}", path.display());
            return;
        };

        self.queue.push_async((path, file_kind)).await;
        match file_kind {
            FileKind::Video => self.stats.video_count.fetch_add(1, Ordering::Relaxed),
            FileKind::Image => self.stats.image_count.fetch_add(1, Ordering::Relaxed),
            FileKind::Audio => self.stats.audio_count.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn push(&self, path: PathBuf) {
        {
            let mut queued_files = self.queued_files.lock();
            if queued_files.contains(&path) {
                return;
            }
            queued_files.insert(path.clone());
        }
        let Some(file_kind) = FileKind::from_path(&path) else {
            eprintln!("Unknown file type: {}", path.display());
            return;
        };

        self.queue.push((path, file_kind));

        match file_kind {
            FileKind::Video => self.stats.video_count.fetch_add(1, Ordering::Relaxed),
            FileKind::Image => self.stats.image_count.fetch_add(1, Ordering::Relaxed),
            FileKind::Audio => self.stats.audio_count.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn reset(&self) {
        self.queue.clear();
        self.queued_files.lock().clear();
        self.stats.video_count.store(0, Ordering::Relaxed);
        self.stats.image_count.store(0, Ordering::Relaxed);
        self.stats.audio_count.store(0, Ordering::Relaxed);
    }

    pub fn refresh_roots(&self) {
        let enabled_roots = self.enabled_roots.read();
        let mut queued_files = self.queued_files.lock();

        self.queue.retain(|(path, file_kind)| {
            if enabled_roots.iter().any(|root| path.starts_with(root)) {
                return true;
            }

            match file_kind {
                FileKind::Video => self.stats.video_count.fetch_sub(1, Ordering::Relaxed),
                FileKind::Image => self.stats.image_count.fetch_sub(1, Ordering::Relaxed),
                FileKind::Audio => self.stats.audio_count.fetch_sub(1, Ordering::Relaxed),
            };
            queued_files.remove(path);
            false
        });
    }

    pub async fn pop_async(&self) -> (PathBuf, FileKind) {
        let (path, file_kind) = self.queue.pop_async().await;
        match file_kind {
            FileKind::Video => self.stats.video_count.fetch_sub(1, Ordering::Relaxed),
            FileKind::Image => self.stats.image_count.fetch_sub(1, Ordering::Relaxed),
            FileKind::Audio => self.stats.audio_count.fetch_sub(1, Ordering::Relaxed),
        };
        self.queued_files.lock().remove(&path);

        (path, file_kind)
    }

    pub async fn find_pop_async<F>(&self, mut find_fn: F) -> (PathBuf, FileKind)
    where
        F: FnMut(&Path, FileKind) -> bool,
    {
        let (path, file_kind) =
            self.queue.find_async(move |(path, file_kind)| find_fn(path, *file_kind)).await;

        match file_kind {
            FileKind::Video => self.stats.video_count.fetch_sub(1, Ordering::Relaxed),
            FileKind::Image => self.stats.image_count.fetch_sub(1, Ordering::Relaxed),
            FileKind::Audio => self.stats.audio_count.fetch_sub(1, Ordering::Relaxed),
        };
        self.queued_files.lock().remove(&path);

        (path, file_kind)
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct QueueStats<T> {
    pub video_count: T,
    pub image_count: T,
    pub audio_count: T,
}
