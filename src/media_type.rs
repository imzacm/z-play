use std::path::{Path, PathBuf};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MediaType {
    Video,
    Image,
    Audio,
    Unknown,
}

impl MediaType {
    pub fn detect(path: &Path) -> Result<Self, ffmpeg_next::Error> {
        ffmpeg_next::init()?;

        let context = ffmpeg_next::format::input(path)?;

        let is_image =
            matches!(context.format().name(), "image2" | "png_pipe" | "mjpeg" | "bmp_pipe" | "gif");

        eprintln!("{path:?} - {}", context.format().name());

        if is_image {
            return Ok(Self::Image);
        }

        let mut has_video = false;
        let mut has_audio = false;

        for stream in context.streams() {
            let medium = stream.parameters().medium();
            match medium {
                ffmpeg_next::media::Type::Video => has_video = true,
                ffmpeg_next::media::Type::Audio => has_audio = true,
                _ => {}
            }
        }

        if has_video {
            Ok(Self::Video)
        } else if has_audio {
            Ok(Self::Audio)
        } else {
            Ok(Self::Unknown)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaFile {
    pub path: PathBuf,
    pub media_type: MediaType,
}
