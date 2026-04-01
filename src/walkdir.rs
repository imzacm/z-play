use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;
use rustix::fs::{AtFlags, Mode, OFlags};
use triomphe::Arc;
use z_sync::Notify;

use crate::storage_class::StorageClass;

type PathSender = z_queue::defaults::BoundedSender<PathBuf>;
pub type PathReceiver = z_queue::defaults::BoundedReceiver<PathBuf>;

type InternalPathSender = z_queue::defaults::UnboundedSender<(PathBuf, Option<OwnedFd>)>;

const BOUND: NonZeroUsize = NonZeroUsize::new(1000).unwrap();

pub async fn walk_dir(
    path: PathBuf,
    deadline: Option<Instant>,
) -> Result<PathReceiver, std::io::Error> {
    walk_dir_filter(path, deadline, |_, _| true).await
}

pub async fn walk_dir_filter<F>(
    path: PathBuf,
    deadline: Option<Instant>,
    filter: F,
) -> Result<PathReceiver, std::io::Error>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    let (walk, rx) = Walk::new(deadline, filter);
    walk.walk(path).await?;
    if walk.active.0.fetch_sub(1, Ordering::AcqRel) == 1 {
        walk.active.1.notify(usize::MAX);
    }
    Ok(rx)
}

pub async fn walk_roots(
    paths: &[PathBuf],
    deadline: Option<Instant>,
) -> Result<PathReceiver, std::io::Error> {
    walk_roots_filter(paths, deadline, |_, _| true).await
}

pub async fn walk_roots_filter<F>(
    paths: &[PathBuf],
    deadline: Option<Instant>,
    filter: F,
) -> Result<PathReceiver, std::io::Error>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    let (walk, rx) = Walk::new(deadline, filter);

    for path in paths {
        walk.walk(path.clone()).await?;
    }
    if walk.active.0.fetch_sub(1, Ordering::AcqRel) == 1 {
        walk.active.1.notify(usize::MAX);
    }
    Ok(rx)
}

type RequestWorkerSender =
    z_queue::defaults::BoundedSender<(u64, StorageClass, Arc<z_sync::Notify16>)>;

struct Walk<F> {
    result_tx: PathSender,
    device_class_map: z_sync::Lock16<FxHashMap<u64, StorageClass>>,
    storage_map: z_sync::Lock16<FxHashMap<StorageClass, InternalPathSender>>,
    deadline: Option<Instant>,
    // Notify will be notified when no more paths are being walked.
    active: (AtomicUsize, z_sync::Notify16),
    request_worker_tx: RequestWorkerSender,
    filter: F,
}

impl<F> Walk<F>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    fn new(deadline: Option<Instant>, filter: F) -> (std::sync::Arc<Self>, PathReceiver) {
        let (tx, rx) = z_queue::bounded(BOUND);
        let device_class_map = z_sync::Lock::new(FxHashMap::default());
        let storage_map = z_sync::Lock::new(FxHashMap::default());
        let active = (AtomicUsize::new(1), Notify::new());

        let (request_worker_tx, request_worker_rx) = z_queue::bounded(NonZeroUsize::MIN);

        let this = std::sync::Arc::new(Self {
            result_tx: tx,
            device_class_map,
            storage_map,
            deadline,
            active,
            request_worker_tx,
            filter,
        });

        let this_clone = this.clone();
        compio::runtime::spawn(async move {
            let mut listener = this_clone.active.1.listener();
            while this_clone.active.0.load(Ordering::Acquire) != 0 {
                listener.await;
                listener = this_clone.active.1.listener();
            }
            this_clone.storage_map.write_async().await.clear();
        })
        .detach();

        let this_weak = std::sync::Arc::downgrade(&this);
        compio::runtime::spawn(async move {
            while let Ok(item) = request_worker_rx.recv_async().await {
                let Some(this) = this_weak.upgrade() else { break };

                let (device, storage_class, notify): (u64, StorageClass, Arc<z_sync::Notify16>) =
                    item;
                let mut lock = this.storage_map.write_async().await;
                if !lock.contains_key(&storage_class) {
                    let (tx, rx) = z_queue::unbounded();
                    lock.insert(storage_class, tx);

                    let device = (device, storage_class);
                    let worker_count =
                        if matches!(storage_class, StorageClass::Hdd { .. }) { 1 } else { 4 };

                    for _ in 0..worker_count {
                        let this = this.clone();
                        let rx = rx.clone();

                        compio::runtime::spawn(async move {
                            while let Ok((path, dir_fd)) = rx.recv_async().await {
                                let this_clone = this.clone();
                                let result = compio::runtime::spawn_blocking(move || {
                                    read_dir(path, &this_clone, device, dir_fd)
                                })
                                .await;

                                match result {
                                    Ok(Ok(())) => (),
                                    Ok(Err(error)) => {
                                        eprintln!("Error reading directory: {error:?}")
                                    }
                                    Err(error) => {
                                        eprintln!("Panicked reading directory: {error:?}")
                                    }
                                }

                                if this.active.0.fetch_sub(1, Ordering::AcqRel) == 1 {
                                    this.active.1.notify(usize::MAX);
                                }
                            }
                        })
                        .detach();
                    }
                }
                notify.notify(usize::MAX);
            }
        })
        .detach();

        (this, rx)
    }

    async fn walk(&self, path: PathBuf) -> Result<(), std::io::Error> {
        let metadata = compio::fs::metadata(&path).await?;
        self.walk_inner(path, metadata.is_dir(), Some(metadata), None).await
    }

    async fn get_storage_class(
        &self,
        metadata: &compio::fs::Metadata,
    ) -> Result<StorageClass, std::io::Error> {
        let device = metadata.dev();
        if let Some(class) = self.device_class_map.read_async().await.get(&device).copied() {
            return Ok(class);
        }

        let class = StorageClass::from_compio_metadata(metadata).await?;
        self.device_class_map.write_async().await.insert(device, class);
        Ok(class)
    }

    fn get_storage_class_rustix(
        &self,
        stat: &rustix::fs::Stat,
    ) -> Result<StorageClass, std::io::Error> {
        if let Some(class) = self.device_class_map.read().get(&stat.st_dev).copied() {
            return Ok(class);
        }

        let class = StorageClass::from_stat(stat)?;
        self.device_class_map.write().insert(stat.st_dev, class);
        Ok(class)
    }

    async fn walk_inner(
        &self,
        path: PathBuf,
        is_dir: bool,
        compio_metadata: Option<compio::fs::Metadata>,
        parent_device: Option<(u64, StorageClass)>,
    ) -> Result<(), std::io::Error> {
        if !is_dir {
            _ = self.result_tx.send_async(path).await;
            return Ok(());
        }

        let metadata = if let Some(metadata) = compio_metadata {
            metadata
        } else {
            compio::fs::metadata(&path).await?
        };

        let (device, storage_class) = if let Some((parent_device, parent_class)) = parent_device
            && metadata.dev() == parent_device
        {
            (parent_device, parent_class)
        } else {
            (metadata.dev(), self.get_storage_class(&metadata).await?)
        };

        let mut tx = self.storage_map.read_async().await.get(&storage_class).cloned();
        if tx.is_none() {
            let notify = Arc::new(Notify::new());
            let listener = notify.listener();
            _ = self.request_worker_tx.send_async((device, storage_class, notify.clone())).await;
            listener.await;
            tx = self.storage_map.read_async().await.get(&storage_class).cloned();
        }
        let tx = tx.expect("Failed to get tx");

        self.active.0.fetch_add(1, Ordering::Release);

        _ = tx.send_async((path, None)).await;
        Ok(())
    }

    fn walk_inner_blocking(
        &self,
        path: PathBuf,
        stat: Option<rustix::fs::Stat>,
        parent_dir_fd: BorrowedFd<'_>,
        dir_fd: Option<OwnedFd>,
        parent_device: Option<(u64, StorageClass)>,
    ) -> Result<(), std::io::Error> {
        let Some(dir_fd) = dir_fd else {
            _ = self.result_tx.send(path);
            return Ok(());
        };

        let stat = if let Some(stat) = stat {
            stat
        } else {
            let file_name = path.file_name().expect("Path has no file name");
            rustix::fs::statat(parent_dir_fd, file_name, AtFlags::SYMLINK_NOFOLLOW)?
        };

        let (device, storage_class) = if let Some((parent_device, parent_class)) = parent_device
            && stat.st_dev == parent_device
        {
            (parent_device, parent_class)
        } else {
            (stat.st_dev, self.get_storage_class_rustix(&stat)?)
        };

        let mut tx = self.storage_map.read().get(&storage_class).cloned();
        if tx.is_none() {
            let notify = Arc::new(Notify::new());
            let listener = notify.listener();
            _ = self.request_worker_tx.send((device, storage_class, notify.clone()));
            listener.wait();
            tx = self.storage_map.read().get(&storage_class).cloned();
        }
        let tx = tx.expect("Failed to get tx");

        self.active.0.fetch_add(1, Ordering::Release);

        _ = tx.send((path, Some(dir_fd)));
        Ok(())
    }
}

fn read_dir<F>(
    mut path: PathBuf,
    walk: &Walk<F>,
    device: (u64, StorageClass),
    dir_fd: Option<OwnedFd>,
) -> Result<(), rustix::io::Errno>
where
    F: Fn(&Path, bool) -> bool + Send + Sync + 'static,
{
    let dir_fd = if let Some(dir_fd) = dir_fd {
        dir_fd
    } else {
        rustix::fs::open(
            &path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?
    };

    if let Some(deadline) = walk.deadline
        && Instant::now() > deadline
    {
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(64 * 1024);
    let mut iter = rustix::fs::RawDir::new(dir_fd.as_fd(), buffer.spare_capacity_mut());

    // Iterate over the entries.
    while let Some(entry_result) = iter.next() {
        if let Some(deadline) = walk.deadline
            && Instant::now() > deadline
        {
            return Ok(());
        }

        let entry = entry_result?;

        let name_bytes = entry.file_name().to_bytes();
        if matches!(name_bytes, b"." | b"..") {
            continue;
        }
        let file_name = OsStr::from_bytes(name_bytes);
        let mut file_type = entry.file_type();
        let mut stat = None;
        let mut descriptor = None;

        if file_type == rustix::fs::FileType::Unknown {
            let s =
                rustix::fs::statat(dir_fd.as_fd(), entry.file_name(), AtFlags::SYMLINK_NOFOLLOW)?;
            file_type = rustix::fs::FileType::from_raw_mode(s.st_mode);
            stat = Some(s);
        }
        if file_type.is_dir() {
            let fd = rustix::fs::openat(
                dir_fd.as_fd(),
                entry.file_name(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW,
                Mode::empty(),
            )?;
            descriptor = Some(fd);

            if stat.is_none() {
                let s = rustix::fs::statat(
                    dir_fd.as_fd(),
                    entry.file_name(),
                    AtFlags::SYMLINK_NOFOLLOW,
                )?;
                stat = Some(s);
            }
        }

        path.push(file_name);

        if (walk.filter)(&path, file_type.is_dir())
            && let Err(error) = walk.walk_inner_blocking(
                path.clone(),
                stat,
                dir_fd.as_fd(),
                descriptor,
                Some(device),
            )
        {
            eprintln!("Error walking directory: {error:?}");
        }

        if walk.result_tx.is_disconnected() {
            break;
        }

        path.pop();
    }

    Ok(())
}
