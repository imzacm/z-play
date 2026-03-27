use std::num::NonZeroUsize;
use std::path::PathBuf;

use rustc_hash::{FxBuildHasher, FxHashSet};
use z_queue::ZQueueMap;
use z_queue::container::CrossbeamArrayQueue;
use z_sync::Lock;

use crate::http::FileKind;

pub struct Queue {
    enabled_roots: Lock<Vec<PathBuf>>,
    disabled_roots: Lock<Vec<PathBuf>>,
    queue: ZQueueMap<FileKind, CrossbeamArrayQueue<PathBuf>, FxBuildHasher>,
    queued_files: Lock<FxHashSet<PathBuf>>,
}

impl Queue {
    pub const QUEUE_SIZE: usize = 100;

    pub fn new(roots: Vec<PathBuf>) -> Self {
        let len = roots.len();
        let mut queued_files = FxHashSet::default();
        queued_files.reserve(Self::QUEUE_SIZE);
        let queue_size = NonZeroUsize::new(Self::QUEUE_SIZE).unwrap();
        Self {
            enabled_roots: Lock::new(roots),
            disabled_roots: Lock::new(Vec::with_capacity(len)),
            queue: ZQueueMap::bounded(FileKind::NUM_VARIANTS, queue_size),
            queued_files: Lock::new(queued_files),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.total_len()
    }

    pub fn stats(&self) -> QueueStats {
        let video_count = self.queue.len(&FileKind::Video);
        let image_count = self.queue.len(&FileKind::Image);
        let audio_count = self.queue.len(&FileKind::Audio);

        QueueStats { video_count, image_count, audio_count }
    }

    pub fn enabled_roots(&self) -> &Lock<Vec<PathBuf>> {
        &self.enabled_roots
    }

    pub fn disabled_roots(&self) -> &Lock<Vec<PathBuf>> {
        &self.disabled_roots
    }

    pub async fn push_async(&self, path: PathBuf) {
        {
            let mut queued_files = self.queued_files.write_async().await;
            if queued_files.contains(&path) {
                return;
            }
            queued_files.insert(path.clone());
        }
        let Some(file_kind) = FileKind::from_path(&path) else {
            eprintln!("Unknown file type: {}", path.display());
            return;
        };

        self.queue.push_async(file_kind, path).await;
    }

    pub fn push(&self, path: PathBuf) {
        {
            let mut queued_files = self.queued_files.write();
            if queued_files.contains(&path) {
                return;
            }
            queued_files.insert(path.clone());
        }
        let Some(file_kind) = FileKind::from_path(&path) else {
            eprintln!("Unknown file type: {}", path.display());
            return;
        };

        self.queue.push(file_kind, path);
    }

    pub async fn reset(&self) {
        self.queue.clear_async().await;
        self.queued_files.write_async().await.clear();
    }

    pub async fn refresh_roots(&self) {
        let enabled_roots = self.enabled_roots.read();
        let mut queued_files = self.queued_files.write_async().await;

        self.queue
            .retain_async(
                |_| true,
                |path| {
                    if enabled_roots.iter().any(|root| path.starts_with(root)) {
                        return true;
                    }

                    queued_files.remove(path);
                    false
                },
            )
            .await;
    }

    pub async fn pop_async(&self, kinds: Option<&FxHashSet<FileKind>>) -> (PathBuf, FileKind) {
        let key_fn =
            |file_kind: &FileKind| -> bool { kinds.is_none_or(|kinds| kinds.contains(file_kind)) };

        let (file_kind, path) = self.queue.pop_async(key_fn).await;
        self.queued_files.write_async().await.remove(&path);

        (path, file_kind)
    }

    pub async fn find_pop_async(
        &self,
        kinds: Option<&FxHashSet<FileKind>>,
        roots: Option<&FxHashSet<String>>,
    ) -> (PathBuf, FileKind) {
        let Some(roots) = roots else { return self.pop_async(kinds).await };

        let key_fn =
            |file_kind: &FileKind| -> bool { kinds.is_none_or(|kinds| kinds.contains(file_kind)) };

        let find_fn = |path: &PathBuf| -> bool { roots.iter().any(|root| path.starts_with(root)) };

        let (file_kind, path) = self.queue.find_async(key_fn, find_fn).await;
        self.queued_files.write_async().await.remove(&path);

        (path, file_kind)
    }

    pub fn shuffle(&self) {
        let mut rng = rand::rng();
        self.queue.rand_shuffle(&mut rng);
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct QueueStats {
    pub video_count: usize,
    pub image_count: usize,
    pub audio_count: usize,
}
