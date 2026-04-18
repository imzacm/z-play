use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use base64::Engine;
use rustc_hash::{FxHashMap, FxHashSet};

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
        let create_future = compio::fs::create_dir(&output_dir);
        let duration_future = get_video_duration(&file_path);

        let (create_result, duration_result) = tokio::join!(create_future, duration_future);
        create_result?;
        let duration =
            duration_result?.ok_or_else(|| std::io::Error::other("Failed to get duration"))?;

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

    async fn pre_read(&self, path: &Path) -> Result<(), std::io::Error> {
        if !path.starts_with(&self.dir) {
            return Ok(());
        }
        let file_name = path.file_name().unwrap().to_str().unwrap();

        let mut segment_number: Option<u16> = None;
        if let Some(("", rest)) = file_name.split_once("seg_")
            && let Some((num, "")) = rest.rsplit_once(".m4s")
        {
            match num.parse::<u16>() {
                Ok(num) => segment_number = Some(num),
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid segment number",
                    ));
                }
            }
        }

        if file_name.ends_with(".m3u8") {
            let init_path = path.with_file_name("init.mp4");
            if self.segments.borrow().is_empty() {
                wait_for_file(&init_path).await?;
            }
            return Ok(());
        }

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
    // file_path -> playlist
    playlists: Rc<RefCell<FxHashMap<PathBuf, Rc<Playlist>>>>,
}

impl PlaylistManager {
    fn file_path_to_playlist_name(file_path: &Path) -> String {
        let bytes = file_path.as_os_str().as_bytes();
        let bytes = smaz::compress(bytes);
        base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(bytes)
    }

    fn playlist_path_to_file_path(playlist_path: &Path) -> Option<PathBuf> {
        let is_playlist_file = playlist_path
            .extension()
            .is_some_and(|ext| ext == "m3u8" || ext == "m4s" || ext == "mp4");
        if !is_playlist_file {
            return None;
        }

        let parent_name = playlist_path.parent()?.file_name()?.to_str()?;
        let bytes = base64::prelude::BASE64_URL_SAFE_NO_PAD.decode(parent_name).ok()?;
        let bytes = smaz::decompress(&bytes).ok()?;

        let file_path = PathBuf::from(OsString::from_vec(bytes));
        Some(file_path)
    }

    pub fn new(root_dir: PathBuf) -> Self {
        let playlists = Rc::new(RefCell::new(FxHashMap::default()));

        let weak = Rc::downgrade(&playlists);
        compio::runtime::spawn(async move {
            loop {
                compio::time::sleep(Playlist::EXPIRES_AFTER / 2).await;
                let Some(playlists) = weak.upgrade() else { break };

                let now = Instant::now();
                playlists
                    .borrow_mut()
                    .retain(|_, playlist: &mut Rc<Playlist>| playlist.expires_at.get() > now);
            }
        })
        .detach();

        Self { root_dir, playlists }
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

        let playlist = Playlist::new(self.root_dir.join(dir_name), file_path.clone()).await?;

        let mut playlists = self.playlists.borrow_mut();

        if let Some(playlist) = playlists.get_mut(&file_path) {
            playlist.expires_at.set(Instant::now() + Playlist::EXPIRES_AFTER);
            return Ok(playlist.playlist_file());
        }

        let playlist_file = playlist.playlist_file();
        playlists.insert(file_path, Rc::new(playlist));
        Ok(playlist_file)
    }

    pub async fn close(&self, file_path: &Path) {
        let mut playlist_to_close = None;

        if let Some(playlist) = self.playlists.borrow_mut().remove(file_path) {
            playlist_to_close = Some(playlist);
        }

        if playlist_to_close.is_none()
            && let Some(file_path) = Self::playlist_path_to_file_path(file_path)
            && let Some(playlist) = self.playlists.borrow_mut().remove(&file_path)
        {
            playlist_to_close = Some(playlist);
        }

        let Some(mut playlist) = playlist_to_close else { return };
        let playlist = loop {
            match Rc::try_unwrap(playlist) {
                Ok(playlist) => break playlist,
                Err(value) => playlist = value,
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        };
        playlist.close().await;
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
        playlist.pre_read(path.as_ref()).await?;

        Ok(Some(path))
    }
}

async fn file_exists(path: &Path) -> bool {
    let Ok(metadata) = compio::fs::metadata(path).await else { return false };
    metadata.is_file() && metadata.len() > 0
}

async fn wait_for_file(path: &Path) -> Result<(), std::io::Error> {
    let started_at = Instant::now();
    loop {
        if file_exists(path).await {
            return Ok(());
        }
        if started_at.elapsed() > Duration::from_secs(10) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Timed out waiting for file",
            ));
        }
        compio::time::sleep(Duration::from_millis(100)).await;
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
