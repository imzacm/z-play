use std::ffi::OsStr;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use compio::BufResult;
use compio::buf::IntoInner;
use rustc_hash::FxHashMap;
use rustix::fs::inotify::{self, CreateFlags, ReadFlags, WatchFlags};
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use triomphe::Arc;
use z_sync::Lock16;

#[derive(Debug)]
pub struct Event<'a> {
    pub dir: PathBuf,
    pub name: Option<&'a OsStr>,
    pub mask: ReadFlags,
    pub cookie: u32,
}

#[derive(Debug)]
pub struct INotify {
    watch_fd: Arc<OwnedFd>,
    watching: Lock16<FxHashMap<i32, PathBuf>>,
}

impl INotify {
    pub fn new() -> Result<Self, std::io::Error> {
        let watch_fd = inotify::init(CreateFlags::CLOEXEC)?;
        Ok(Self {
            watch_fd: Arc::new(watch_fd),
            watching: Lock16::new(FxHashMap::default()),
        })
    }

    pub async fn new_async() -> Result<Self, std::io::Error> {
        let watch_fd = compio::runtime::spawn_blocking(|| inotify::init(CreateFlags::CLOEXEC))
            .await
            .unwrap()?;
        Ok(Self {
            watch_fd: Arc::new(watch_fd),
            watching: Lock16::new(FxHashMap::default()),
        })
    }

    fn check_event(&self, event: &RawEvent<'_>) {
        if event.mask.contains(ReadFlags::DELETE_SELF) {
            let mut lock = self.watching.write();
            lock.remove(&event.wd);
        }
    }

    async fn check_event_async(&self, event: &RawEvent<'_>) {
        if event.mask.contains(ReadFlags::DELETE_SELF) {
            let mut lock = self.watching.write_async().await;
            lock.remove(&event.wd);
        }
    }

    fn raw_event_to_event<'a>(&'a self, event: &RawEvent<'a>) -> Option<Event<'a>> {
        let lock = self.watching.read();
        if !lock.contains_key(&event.wd) {
            return None;
        }
        let dir = lock.get(&event.wd).unwrap();
        Some(Event {
            dir: dir.clone(),
            name: event.name,
            mask: event.mask,
            cookie: event.cookie,
        })
    }

    async fn raw_event_to_event_async<'a>(&'a self, event: &RawEvent<'a>) -> Option<Event<'a>> {
        let lock = self.watching.read_async().await;
        if !lock.contains_key(&event.wd) {
            return None;
        }
        let dir = lock.get(&event.wd).unwrap();
        Some(Event {
            dir: dir.clone(),
            name: event.name,
            mask: event.mask,
            cookie: event.cookie,
        })
    }

    pub fn add_watch(&self, path: PathBuf, flags: WatchFlags) -> Result<(), std::io::Error> {
        let wd = inotify::add_watch(self.watch_fd.as_fd(), &path, flags)?;
        self.watching.write().insert(wd, path);
        Ok(())
    }

    pub async fn add_watch_async(
        &self,
        path: PathBuf,
        flags: WatchFlags,
    ) -> Result<(), std::io::Error> {
        let watch_fd = self.watch_fd.clone();
        let (wd, path) = compio::runtime::spawn_blocking(move || {
            let wd = inotify::add_watch(watch_fd.as_fd(), &path, flags)?;
            Ok::<_, std::io::Error>((wd, path))
        })
        .await
        .unwrap()?;
        self.watching.write_async().await.insert(wd, path);
        Ok(())
    }

    pub fn remove_watch<P>(&self, path: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let wd = {
            let mut lock = self.watching.write();
            let wd = lock.iter().find(|(_, p)| p == &path).map(|(wd, _)| *wd);
            let Some(wd) = wd else { return Ok(()) };
            lock.remove(&wd);
            wd
        };
        inotify::remove_watch(self.watch_fd.as_fd(), wd)?;
        Ok(())
    }

    pub async fn remove_watch_async<P>(&self, path: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let wd = {
            let mut lock = self.watching.write_async().await;
            let wd = lock.iter().find(|(_, p)| p == &path).map(|(wd, _)| *wd);
            let Some(wd) = wd else { return Ok(()) };
            lock.remove(&wd);
            wd
        };

        let watch_fd = self.watch_fd.clone();
        compio::runtime::spawn_blocking(move || inotify::remove_watch(watch_fd.as_fd(), wd))
            .await
            .unwrap()?;

        inotify::remove_watch(self.watch_fd.as_fd(), wd)?;
        Ok(())
    }

    pub fn drain<V>(&self, mut visit: V) -> rustix::io::Result<()>
    where
        V: FnMut(Event<'_>),
    {
        // Get current file descriptor flags
        let current_flags = fcntl_getfl(&*self.watch_fd)?;

        // Temporarily set to O_NONBLOCK
        fcntl_setfl(&*self.watch_fd, current_flags | OFlags::NONBLOCK)?;

        // Standard buffer size for inotify events
        let mut buffer = [0u8; 4096];

        // Drain until EAGAIN (queue is empty)
        let drain_result = loop {
            let n = match rustix::io::read(&*self.watch_fd, &mut buffer) {
                Ok(n) => n,
                Err(rustix::io::Errno::AGAIN) => break Ok(()),
                Err(error) => break Err(error),
            };

            let buffer = &buffer[..n];
            for event in RawEventIter::new(buffer) {
                if let Some(event) = self.raw_event_to_event(&event) {
                    visit(event);
                }
                self.check_event(&event);
            }
        };

        // Restore original flags (blocking mode)
        fcntl_setfl(&*self.watch_fd, current_flags)?;

        drain_result
    }

    pub async fn drain_async<V>(&self, mut visit: V) -> std::io::Result<()>
    where
        V: FnMut(Event<'_>),
    {
        let watch_fd = self.watch_fd.clone();
        let current_flags = compio::runtime::spawn_blocking(move || {
            // Get current file descriptor flags
            let current_flags = fcntl_getfl(&*watch_fd)?;
            // Temporarily set to O_NONBLOCK
            fcntl_setfl(&*watch_fd, current_flags | OFlags::NONBLOCK)?;
            Ok::<_, std::io::Error>(current_flags)
        })
        .await
        .unwrap()?;

        let drain_result = loop {
            let fd = self.watch_fd.try_clone()?;
            let op = compio::driver::op::Read::new(fd, [0u8; 4096]);
            let BufResult(result, op) = compio::runtime::submit(op).await;
            let n = match result {
                Ok(n) => n,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break Ok(()),
                Err(error) => break Err(error),
            };
            let buffer = op.into_inner();

            let buffer = &buffer[..n];
            for event in RawEventIter::new(buffer) {
                if let Some(event) = self.raw_event_to_event_async(&event).await {
                    visit(event);
                }
                self.check_event_async(&event).await;
            }
        };

        // Restore original flags (blocking mode)
        let watch_fd = self.watch_fd.clone();
        compio::runtime::spawn_blocking(move || fcntl_setfl(&*watch_fd, current_flags))
            .await
            .unwrap()?;

        drain_result
    }

    pub fn wait<V>(&self, mut visit: V) -> std::io::Result<()>
    where
        V: FnMut(Event<'_>),
    {
        let mut buffer = [0u8; 4096];
        let n = rustix::io::read(&*self.watch_fd, &mut buffer)?;
        let buffer = &buffer[..n];
        for event in RawEventIter::new(buffer) {
            if let Some(event) = self.raw_event_to_event(&event) {
                visit(event);
            }
            self.check_event(&event);
        }
        Ok(())
    }

    pub async fn wait_async<V>(&self, mut visit: V) -> std::io::Result<()>
    where
        V: FnMut(Event<'_>),
    {
        let fd = self.watch_fd.try_clone()?;
        let op = compio::driver::op::Read::new(fd, [0u8; 4096]);
        let BufResult(result, op) = compio::runtime::submit(op).await;
        let n = result?;
        let buffer = op.into_inner();

        let buffer = &buffer[..n];
        for event in RawEventIter::new(buffer) {
            if let Some(event) = self.raw_event_to_event_async(&event).await {
                visit(event);
            }
            self.check_event_async(&event).await;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct RawEvent<'a> {
    wd: i32,
    mask: ReadFlags,
    cookie: u32,
    name: Option<&'a OsStr>,
}

#[derive(Debug)]
struct RawEventIter<'a> {
    buffer: &'a [u8],
    offset: usize,
}

impl<'a> RawEventIter<'a> {
    fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, offset: 0 }
    }
}

impl<'a> Iterator for RawEventIter<'a> {
    type Item = RawEvent<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // An inotify event base is exactly 16 bytes.
        if self.offset + 16 > self.buffer.len() {
            return None;
        }

        // Read the 4 base u32/i32 fields using native endianness.
        let wd = i32::from_ne_bytes(self.buffer[self.offset..self.offset + 4].try_into().unwrap());
        let mask_bits =
            u32::from_ne_bytes(self.buffer[self.offset + 4..self.offset + 8].try_into().unwrap());
        let cookie =
            u32::from_ne_bytes(self.buffer[self.offset + 8..self.offset + 12].try_into().unwrap());
        let len =
            u32::from_ne_bytes(self.buffer[self.offset + 12..self.offset + 16].try_into().unwrap())
                as usize;

        self.offset += 16;

        let mut name = None;
        if len > 0 && self.offset + len <= self.buffer.len() {
            let name_bytes = &self.buffer[self.offset..self.offset + len];
            // Inotify pads the name with null bytes to align the next event.
            // We find the first null byte to grab the actual string slice.
            if let Some(null_pos) = name_bytes.iter().position(|&b| b == 0)
                && null_pos > 0
            {
                name = Some(OsStr::from_bytes(&name_bytes[..null_pos]));
            }
        }

        self.offset += len;

        // Convert bitmask back to ReadFlags. using `from_bits_retain` ensures
        // we don't crash if a newer kernel passes an unknown flag.
        let mask = ReadFlags::from_bits_retain(mask_bits);

        Some(RawEvent { wd, mask, cookie, name })
    }
}
