use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use compio::BufResult;
use compio::bytes::{Bytes, BytesMut};
use compio::io::AsyncReadAt;
use lru::LruCache;
use rustc_hash::{FxBuildHasher, FxHashMap};
use z_sync::Notify64;

#[derive(Clone)]
pub struct FileCache {
    inner: Rc<FileCacheInner>,
}

impl Default for FileCache {
    fn default() -> Self {
        Self::new(None)
    }
}

impl FileCache {
    pub fn new<T>(max_cache_size: T) -> Self
    where
        T: Into<Option<usize>>,
    {
        let inner = FileCacheInner {
            files: RefCell::new(FxHashMap::default()),
            active_ids: RefCell::new(FxHashMap::default()),
            next_file_id: Cell::new(0),
            max_cache_size: max_cache_size.into().unwrap_or(0),
            current_cache_size: RefCell::new(0),
            lru_chunks: RefCell::new(LruCache::with_hasher(
                NonZeroUsize::new(1000).unwrap(),
                FxBuildHasher,
            )),
        };
        let inner = Rc::new(inner);

        let weak = Rc::downgrade(&inner);
        compio::runtime::spawn(async move {
            loop {
                compio::time::sleep(Duration::from_secs(30)).await;
                let Some(inner) = weak.upgrade() else { break };
                let now = Instant::now();
                inner.files.borrow_mut().retain(|_, file| *file.expires_at.borrow() > now);
            }
        })
        .detach();

        Self { inner }
    }

    pub fn clear(&self) {
        self.inner.files.borrow_mut().clear();
    }

    pub async fn open<P>(&self, path: P) -> Result<Rc<CachedFile>, std::io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        if let Some(entry) = self.inner.files.borrow_mut().get_mut(path) {
            entry.expires_at.replace(Instant::now() + CachedFile::EXPIRES_AFTER);
            return Ok(entry.clone());
        }

        let mut new_entry = CachedFile::open(path, self.inner.clone()).await?;

        let mut files = self.inner.files.borrow_mut();

        // If it was created while we were waiting, use that.
        if let Some(entry) = files.get_mut(path) {
            entry.expires_at.replace(*new_entry.expires_at.borrow());
            return Ok(entry.clone());
        }

        new_entry.id = self.inner.next_file_id.get();
        self.inner.next_file_id.set(new_entry.id.wrapping_add(1));

        let new_entry = Rc::new(new_entry);
        files.insert(path.to_owned(), new_entry.clone());
        self.inner
            .active_ids
            .borrow_mut()
            .insert(new_entry.id, Rc::downgrade(&new_entry));
        Ok(new_entry)
    }

    pub fn close<P>(&self, path: P)
    where
        P: AsRef<Path>,
    {
        self.inner.files.borrow_mut().remove(path.as_ref());
    }
}

struct FileCacheInner {
    files: RefCell<FxHashMap<PathBuf, Rc<CachedFile>>>,
    active_ids: RefCell<FxHashMap<u64, Weak<CachedFile>>>,
    next_file_id: Cell<u64>,
    max_cache_size: usize,
    current_cache_size: RefCell<usize>,
    lru_chunks: RefCell<LruCache<(u64, u64), (), FxBuildHasher>>,
}

enum ChunkState {
    Loading(Rc<Notify64>),
    Loaded(Bytes),
}

pub struct CachedFile {
    id: u64,
    file: compio::fs::File,
    size: u64,
    expires_at: RefCell<Instant>,
    chunks: RefCell<FxHashMap<u64, ChunkState>>,
    file_cache: Rc<FileCacheInner>,
}

impl CachedFile {
    // 4 MiB
    pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;
    const EXPIRES_AFTER: Duration = Duration::from_mins(5);

    async fn open<P>(path: P, file_cache: Rc<FileCacheInner>) -> Result<Self, std::io::Error>
    where
        P: AsRef<Path>,
    {
        let file = compio::fs::File::open(path).await?;
        let metadata = file.metadata().await?;

        let expires_at = Instant::now() + Self::EXPIRES_AFTER;
        Ok(Self {
            id: 0,
            file,
            size: metadata.len(),
            expires_at: RefCell::new(expires_at),
            chunks: RefCell::new(FxHashMap::default()),
            file_cache,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes, std::io::Error> {
        if offset >= self.size || len == 0 {
            return Ok(Bytes::new());
        }

        let mut len = len.min((self.size - offset) as usize);
        let start_chunk = offset / Self::CHUNK_SIZE;
        let end_chunk = (offset + len as u64 - 1) / Self::CHUNK_SIZE;

        if start_chunk == end_chunk {
            let chunk = self.load_chunk(start_chunk).await?;
            let chunk_offset = (offset % Self::CHUNK_SIZE) as usize;
            return Ok(chunk.slice(chunk_offset..chunk_offset + len));
        }

        let mut offset = offset;
        let mut result = BytesMut::with_capacity(len);

        while len > 0 {
            let chunk_index = offset / Self::CHUNK_SIZE;
            let chunk_offset = (offset % Self::CHUNK_SIZE) as usize;

            let chunk = self.load_chunk(chunk_index).await?;
            let copy_len = len.min(chunk.len().saturating_sub(chunk_offset));

            if copy_len == 0 {
                break;
            }

            result.extend_from_slice(&chunk[chunk_offset..chunk_offset + copy_len]);
            offset += copy_len as u64;
            len -= copy_len;
        }

        Ok(result.freeze())
    }

    async fn load_chunk(&self, chunk_index: u64) -> Result<Bytes, std::io::Error> {
        loop {
            // Check current state and either return bytes, get a listener, or claim the read
            let listener = {
                let mut chunks = self.chunks.borrow_mut();
                match chunks.get(&chunk_index) {
                    Some(ChunkState::Loaded(chunk)) => {
                        self.file_cache.lru_chunks.borrow_mut().push((self.id, chunk_index), ());
                        return Ok(chunk.clone());
                    }
                    Some(ChunkState::Loading(notify)) => Notify64::rc_listener(notify),
                    None => {
                        let notify = Rc::new(Notify64::new());
                        chunks.insert(chunk_index, ChunkState::Loading(notify));
                        break;
                    }
                }
            };

            // Wait for the primary loader to finish, then loop to check the state again
            listener.await;
        }

        // Perform the actual I/O read
        let offset = chunk_index * Self::CHUNK_SIZE;
        let len = Self::CHUNK_SIZE.min(self.size.saturating_sub(offset)) as usize;
        let BufResult(result, buffer) = self.file.read_at(Vec::with_capacity(len), offset).await;

        match result {
            Ok(buffer_len) => {
                // Size Management & Eviction
                let max_cache_size = self.file_cache.max_cache_size;
                if max_cache_size > 0 {
                    let mut current_cache_size = self.file_cache.current_cache_size.borrow_mut();

                    while *current_cache_size + buffer_len > max_cache_size {
                        let mut lru = self.file_cache.lru_chunks.borrow_mut();

                        let Some(((id, chunk_index), ())) = lru.pop_lru() else { break };

                        if let Some(file) = self.file_cache.active_ids.borrow().get(&id)
                            && let Some(file) = file.upgrade()
                            && let Some(ChunkState::Loaded(chunk)) =
                                file.chunks.borrow_mut().remove(&chunk_index)
                        {
                            *current_cache_size = current_cache_size.saturating_sub(chunk.len());
                        }
                    }

                    *current_cache_size += buffer_len;
                }

                // Update state and notify parked waiters
                let chunk = Bytes::from(buffer);

                let old_state =
                    self.chunks.borrow_mut().insert(chunk_index, ChunkState::Loaded(chunk.clone()));

                self.file_cache.lru_chunks.borrow_mut().push((self.id, chunk_index), ());

                if let Some(ChunkState::Loading(notify)) = old_state {
                    notify.notify(usize::MAX);
                }

                Ok(chunk)
            }
            Err(error) => {
                // If the read fails, clear the Loading state and wake everyone so they can retry or
                // bubble the error
                let old_state = self.chunks.borrow_mut().remove(&chunk_index);
                if let Some(ChunkState::Loading(notify)) = old_state {
                    notify.notify(usize::MAX);
                }
                Err(error)
            }
        }
    }
}

impl Drop for CachedFile {
    fn drop(&mut self) {
        self.file_cache.active_ids.borrow_mut().remove(&self.id);

        if self.file_cache.max_cache_size == 0 {
            return;
        }

        let mut current_cache_size = self.file_cache.current_cache_size.borrow_mut();

        for state in self.chunks.borrow().values() {
            if let ChunkState::Loaded(chunk) = state {
                *current_cache_size = current_cache_size.saturating_sub(chunk.len());
            }
        }
    }
}
