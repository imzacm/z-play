use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashSet;

use crate::http::FileKind;

pub struct Queue {
    enabled_roots: RwLock<Vec<PathBuf>>,
    disabled_roots: RwLock<Vec<PathBuf>>,
    tx: flume::Sender<PathBuf>,
    rx: flume::Receiver<PathBuf>,
    stats: QueueStats<AtomicUsize>,
    queued_files: Mutex<FxHashSet<PathBuf>>,
}

impl Queue {
    pub const QUEUE_SIZE: usize = 1000;

    pub fn new(roots: Vec<PathBuf>) -> Self {
        let (tx, rx) = flume::bounded(Self::QUEUE_SIZE);
        let len = roots.len();
        let mut queued_files = FxHashSet::default();
        queued_files.reserve(Self::QUEUE_SIZE);
        Self {
            enabled_roots: RwLock::new(roots),
            disabled_roots: RwLock::new(Vec::with_capacity(len)),
            tx,
            rx,
            stats: QueueStats::default(),
            queued_files: Mutex::new(queued_files),
        }
    }

    pub fn len(&self) -> usize {
        self.rx.len()
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
        let file_kind = FileKind::from_path(&path);
        self.tx.send_async(path).await.expect("Queue channel disconnected");
        match file_kind {
            Some(FileKind::Video) => self.stats.video_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Image) => self.stats.image_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Audio) => self.stats.audio_count.fetch_add(1, Ordering::Relaxed),
            None => 0,
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
        let file_kind = FileKind::from_path(&path);
        self.tx.send(path).expect("Queue channel disconnected");
        match file_kind {
            Some(FileKind::Video) => self.stats.video_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Image) => self.stats.image_count.fetch_add(1, Ordering::Relaxed),
            Some(FileKind::Audio) => self.stats.audio_count.fetch_add(1, Ordering::Relaxed),
            None => 0,
        };
    }

    pub fn reset(&self) {
        for _ in self.rx.try_iter() {}
        self.queued_files.lock().clear();
        self.stats.video_count.store(0, Ordering::Relaxed);
        self.stats.image_count.store(0, Ordering::Relaxed);
        self.stats.audio_count.store(0, Ordering::Relaxed);
    }

    pub fn refresh_roots(&self) {
        let enabled_roots = self.enabled_roots.read();
        let mut queued_files = self.queued_files.lock();
        for path in self.rx.drain() {
            if !enabled_roots.iter().any(|root| path.starts_with(root)) {
                match FileKind::from_path(&path) {
                    Some(FileKind::Video) => self.stats.video_count.fetch_sub(1, Ordering::Relaxed),
                    Some(FileKind::Image) => self.stats.image_count.fetch_sub(1, Ordering::Relaxed),
                    Some(FileKind::Audio) => self.stats.audio_count.fetch_sub(1, Ordering::Relaxed),
                    None => 0,
                };
                queued_files.remove(&path);
                continue;
            }

            match self.tx.try_send(path) {
                Ok(()) => (),
                Err(flume::TrySendError::Disconnected(_)) => panic!("Queue channel disconnected"),
                Err(flume::TrySendError::Full(_)) => panic!("Queue channel full in refresh_roots"),
            }
        }
    }

    pub async fn pop_async(&self) -> PathBuf {
        let path = self.rx.recv_async().await.expect("Queue channel disconnected");
        match FileKind::from_path(&path) {
            Some(FileKind::Video) => self.stats.video_count.fetch_sub(1, Ordering::Relaxed),
            Some(FileKind::Image) => self.stats.image_count.fetch_sub(1, Ordering::Relaxed),
            Some(FileKind::Audio) => self.stats.audio_count.fetch_sub(1, Ordering::Relaxed),
            None => 0,
        };
        self.queued_files.lock().remove(&path);
        path
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct QueueStats<T> {
    pub video_count: T,
    pub image_count: T,
    pub audio_count: T,
}
