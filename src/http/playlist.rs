use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use base64::Engine;
use rustc_hash::{FxHashMap, FxHashSet};
use z_play::inotify::{self, INotify};
use z_sync::Notify16;

use crate::http::transcode::{create_vod_playlist, get_video_duration, spawn_transcode_hls};

struct Playlist {
    dir: PathBuf,
    file_path: PathBuf,
    segments: RefCell<FxHashSet<u16>>,
    expires_at: Cell<Instant>,
    ffmpeg: RefCell<Option<std::process::Child>>,
}

impl Playlist {
    const EXPIRES_AFTER: Duration = Duration::from_mins(30);

    async fn new(output_dir: PathBuf, file_path: PathBuf) -> Result<Self, std::io::Error> {
        let duration = get_video_duration(&file_path)
            .await?
            .ok_or_else(|| std::io::Error::other("Failed to get duration"))?;

        let fake_playlist_path = output_dir.join("playlist.m3u8");
        create_vod_playlist(&fake_playlist_path, duration).await?;

        let file_path_clone = file_path.clone();
        let output_dir_clone = output_dir.clone();
        let ffmpeg = compio::runtime::spawn_blocking(move || {
            spawn_transcode_hls(file_path_clone, output_dir_clone, "_playlist.m3u8", None)
        })
        .await
        .unwrap()?;

        Ok(Self {
            dir: output_dir,
            file_path,
            segments: RefCell::new(FxHashSet::default()),
            expires_at: Cell::new(Instant::now() + Self::EXPIRES_AFTER),
            ffmpeg: RefCell::new(Some(ffmpeg)),
        })
    }

    fn playlist_file(&self) -> PathBuf {
        self.dir.join("playlist.m3u8")
    }

    fn ffmpeg_playlist_file(&self) -> PathBuf {
        self.dir.join("_playlist.m3u8")
    }

    async fn pre_read(
        &self,
        path: &Path,
        file_notify_map: &RefCell<FxHashMap<PathBuf, Rc<Notify16>>>,
    ) -> Result<(), std::io::Error> {
        let wait_for_file = async |path: &Path| {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let listener = {
                    let mut file_notify_map = file_notify_map.borrow_mut();
                    let notify = file_notify_map
                        .entry(path.to_owned())
                        .or_insert_with(|| Rc::new(Notify16::new()));
                    Notify16::rc_listener(&*notify)
                };

                if file_exists(path).await {
                    file_notify_map.borrow_mut().remove(path);
                    return Ok(());
                }

                if compio::time::timeout_at(deadline, listener).await.is_err() {
                    // One last try.
                    if file_exists(path).await {
                        return Ok(());
                    }

                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Timed out waiting for file",
                    ));
                }
            }
        };

        if !path.starts_with(&self.dir) {
            return Ok(());
        }
        let file_name = path.file_name().unwrap().to_str().unwrap();

        let segment_number = extract_segment_number(file_name)?;

        let Some(segment_number) = segment_number else {
            if self.segments.borrow().is_empty() {
                wait_for_file(&path).await?;
            }
            return Ok(());
        };

        if self.segments.borrow().contains(&segment_number) {
            return Ok(());
        }

        if file_exists(&path).await {
            self.segments.borrow_mut().insert(segment_number);
            return Ok(());
        }

        let playlist_path = self.ffmpeg_playlist_file();
        wait_for_file(&playlist_path).await?;

        let playlist_info = read_playlist_info(&playlist_path).await?;

        if let Some(first_segment) = playlist_info.first_segment
            && let Some(last_segment) = playlist_info.last_segment
        {
            self.segments.borrow_mut().extend(first_segment..=last_segment);
        }

        if let Some(last_segment) = playlist_info.last_segment
            && (segment_number < last_segment || segment_number > last_segment + 2)
        {
            // Restart ffmpeg at the request segment.
            let ffmpeg = self.ffmpeg.borrow_mut().take();
            if let Some(mut ffmpeg) = ffmpeg {
                compio::runtime::spawn_blocking(move || {
                    _ = ffmpeg.kill();
                    _ = ffmpeg.wait();
                })
                .await
                .unwrap();
            }

            _ = compio::fs::remove_file(&playlist_path).await;

            let file_path = self.file_path.clone();
            let output_dir = self.dir.clone();
            let ffmpeg = compio::runtime::spawn_blocking(move || {
                spawn_transcode_hls(file_path, output_dir, "_playlist.m3u8", Some(segment_number))
            })
            .await
            .unwrap()?;
            self.ffmpeg.replace(Some(ffmpeg));
        }

        wait_for_file(path).await?;

        Ok(())
    }

    async fn close(mut self) {
        compio::runtime::spawn_blocking(move || {
            if let Some(mut ffmpeg) = self.ffmpeg.take() {
                _ = ffmpeg.kill();
                _ = ffmpeg.wait();
            }
            _ = std::fs::remove_dir_all(std::mem::take(&mut self.dir));
        })
        .await
        .unwrap();
    }
}

impl Drop for Playlist {
    fn drop(&mut self) {
        let ffmpeg = self.ffmpeg.take();
        let dir = std::mem::take(&mut self.dir);

        if let Some(mut ffmpeg) = ffmpeg {
            _ = ffmpeg.kill();
            _ = ffmpeg.wait();
        }
        _ = std::fs::remove_dir_all(dir);
    }
}

pub struct PlaylistManager {
    root_dir: PathBuf,
    inotify: Rc<INotify>,
    // file_path -> playlist
    playlists: Rc<RefCell<FxHashMap<PathBuf, Rc<Playlist>>>>,
    file_notify_map: Rc<RefCell<FxHashMap<PathBuf, Rc<Notify16>>>>,
}

impl PlaylistManager {
    fn file_path_to_playlist_name(file_path: &Path) -> String {
        let bytes = file_path.as_os_str().as_bytes();
        let bytes = smaz::compress(bytes);
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(bytes)
    }

    fn playlist_path_to_file_path(playlist_path: &Path) -> Option<PathBuf> {
        let file_name = playlist_path.file_name()?.to_str()?;
        let is_playlist_file = file_name == "playlist.m3u8"
            || file_name == "_playlist.m3u8"
            || file_name == "init.mp4"
            || matches!(extract_segment_number(file_name), Ok(Some(_)));

        if !is_playlist_file {
            return None;
        }

        let parent_name = playlist_path.parent()?.file_name()?.to_str()?;
        let bytes = base64::prelude::BASE64_URL_SAFE_NO_PAD.decode(parent_name).ok()?;
        let bytes = smaz::decompress(&bytes).ok()?;

        let file_path = PathBuf::from(OsString::from_vec(bytes));
        Some(file_path)
    }

    pub async fn new(root_dir: PathBuf) -> Self {
        let inotify = INotify::new_async().await.expect("Failed to create inotify instance");
        let inotify = Rc::new(inotify);

        let playlists = Rc::new(RefCell::new(FxHashMap::default()));

        let playlists_weak = Rc::downgrade(&playlists);
        compio::runtime::spawn(async move {
            loop {
                compio::time::sleep(Playlist::EXPIRES_AFTER / 2).await;
                let Some(playlists) = playlists_weak.upgrade() else { break };

                let now = Instant::now();
                playlists
                    .borrow_mut()
                    .retain(|_, playlist: &mut Rc<Playlist>| playlist.expires_at.get() > now);
            }
        })
        .detach();

        let file_notify_map = Rc::new(RefCell::new(FxHashMap::default()));

        let inotify_clone = inotify.clone();
        let file_notify_map_weak = Rc::downgrade(&file_notify_map);
        let playlists_weak = Rc::downgrade(&playlists);
        compio::runtime::spawn(async move {
            let visit = |event: inotify::Event<'_>| {
                let is_dir = event.mask.contains(rustix::fs::inotify::ReadFlags::ISDIR);
                let is_create = event.mask.contains(rustix::fs::inotify::ReadFlags::CREATE);
                let is_move_to = event.mask.contains(rustix::fs::inotify::ReadFlags::MOVED_TO);

                if is_dir || !(is_create || is_move_to) {
                    return;
                }

                let Some(file_notify_map) = file_notify_map_weak.upgrade() else { return };

                let Some(name) = event.name else { return };
                let path = event.dir.join(name);

                if let Some(name) = name.to_str()
                    && let Ok(Some(segment_number)) = extract_segment_number(name)
                    && let Some(file_path) = Self::playlist_path_to_file_path(&path)
                    && let Some(playlists) = playlists_weak.upgrade()
                    && let Some(playlist) = playlists.borrow().get(&file_path)
                {
                    playlist.segments.borrow_mut().insert(segment_number);
                }

                let notify: Option<Rc<Notify16>> = file_notify_map.borrow_mut().remove(&path);
                if let Some(notify) = notify {
                    notify.notify(usize::MAX);
                }
            };

            loop {
                if file_notify_map_weak.strong_count() == 0 {
                    break;
                }

                inotify_clone
                    .wait_async(visit)
                    .await
                    .expect("Failed to wait for inotify events");
            }
        })
        .detach();

        Self { root_dir, inotify, playlists, file_notify_map }
    }

    pub async fn get(&self, file_path: &Path) -> Result<PathBuf, std::io::Error> {
        if let Some(playlist) = self.playlists.borrow_mut().get_mut(file_path) {
            playlist.expires_at.set(Instant::now() + Playlist::EXPIRES_AFTER);
            return Ok(playlist.playlist_file());
        }

        let mut file_path = file_path.to_owned();

        if let Some(path) = Self::playlist_path_to_file_path(&file_path) {
            file_path = path;
        }

        if let Some(playlist) = self.playlists.borrow_mut().get_mut(&file_path) {
            playlist.expires_at.set(Instant::now() + Playlist::EXPIRES_AFTER);
            return Ok(playlist.playlist_file());
        }

        let dir_name = Self::file_path_to_playlist_name(&file_path);
        let output_dir = self.root_dir.join(dir_name);

        match compio::fs::create_dir_all(&output_dir).await {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error),
        }

        self.inotify
            .add_watch_async(
                output_dir.clone(),
                rustix::fs::inotify::WatchFlags::DELETE_SELF
                    | rustix::fs::inotify::WatchFlags::CREATE
                    | rustix::fs::inotify::WatchFlags::MOVE,
            )
            .await?;

        let playlist = Playlist::new(output_dir, file_path.clone()).await?;

        let mut playlists = self.playlists.borrow_mut();

        if let Some(playlist) = playlists.get_mut(&file_path) {
            playlist.expires_at.set(Instant::now() + Playlist::EXPIRES_AFTER);
            return Ok(playlist.playlist_file());
        }

        let playlist_file = playlist.playlist_file();
        playlists.insert(file_path, Rc::new(playlist));
        Ok(playlist_file)
    }

    pub fn close(&self, file_path: &Path) {
        if self.playlists.borrow_mut().remove(file_path).is_some() {
            return;
        }

        if let Some(file_path) = Self::playlist_path_to_file_path(file_path) {
            self.playlists.borrow_mut().remove(&file_path);
        }
    }

    pub async fn contains_file(&self, path: &Path) -> bool {
        self.get(path).await.is_ok()
    }

    /// Returns the path to use for access.
    pub async fn pre_read<'p>(
        &self,
        path: &'p Path,
    ) -> Result<Option<Cow<'p, Path>>, std::io::Error> {
        let Some(file_path) = Self::playlist_path_to_file_path(path) else { return Ok(None) };
        let Some(playlist) = self.playlists.borrow().get(&file_path).cloned() else {
            return Ok(None);
        };

        let path = if path.parent() == Some(&playlist.dir) {
            Cow::Borrowed(path)
        } else {
            Cow::Owned(playlist.dir.join(path.file_name().unwrap()))
        };

        playlist.expires_at.set(Instant::now() + Playlist::EXPIRES_AFTER);
        playlist.pre_read(path.as_ref(), &self.file_notify_map).await?;

        Ok(Some(path))
    }
}

async fn file_exists(path: &Path) -> bool {
    let Ok(metadata) = compio::fs::metadata(path).await else { return false };
    metadata.is_file() && metadata.len() > 0
}

fn extract_segment_number(file_name: &str) -> Result<Option<u16>, std::io::Error> {
    if let Some(("", rest)) = file_name.split_once("seg_")
        && let Some((num, "")) = rest.rsplit_once(".m4s")
    {
        match num.parse::<u16>() {
            Ok(num) => Ok(Some(num)),
            Err(_) => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid segment number"))
            }
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
struct PlaylistInfo {
    first_segment: Option<u16>,
    last_segment: Option<u16>,
    // is_finished: bool,
}

async fn read_playlist_info(path: &Path) -> Result<PlaylistInfo, std::io::Error> {
    let buffer = compio::fs::read(path).await?;

    let needle = b".m4s";

    let extract_number = |index: usize| -> Option<u16> {
        let first_digit_index = index.checked_sub(3)?;
        let seg_index = first_digit_index.checked_sub(4)?;
        if &buffer[seg_index..first_digit_index] != b"seg_" {
            return None;
        }
        let num_str = &buffer[first_digit_index..index];
        assert_eq!(num_str.len(), 3);
        let num_str = std::str::from_utf8(&num_str).ok()?;
        num_str.parse::<u16>().ok()
    };

    let first_segment = memchr::memmem::find(&buffer, needle).and_then(extract_number);

    let mut reverse_iter = memchr::memmem::rfind_iter(&buffer, needle);
    let last_index = reverse_iter.next();

    let mut last_segment = last_index.and_then(extract_number);
    if last_index.is_some() && last_segment.is_none() {
        last_segment = reverse_iter.next().and_then(extract_number);
    }

    if last_segment.is_none() {
        last_segment = first_segment;
    }

    // let is_finished = memchr::memmem::rfind(&buffer, b"#EXT-X-ENDLIST").is_some();

    Ok(PlaylistInfo { first_segment, last_segment })
}
