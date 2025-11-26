use std::path::Path;

use gstreamer_pbutils::prelude::DiscovererStreamInfoExt;
use gstreamer_pbutils::{Discoverer, DiscovererInfo};

use crate::Error;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MediaType {
    VideoWithAudio,
    VideoNoAudio,
    Image,
    Audio,
    Unknown,
}

impl MediaType {
    pub fn detect(path: &Path) -> Result<Self, Error> {
        detect_media_type(path)
    }
}

fn detect_media_type(path: &Path) -> Result<MediaType, Error> {
    gstreamer::init()?;

    let uri = glib::filename_to_uri(path, None)?;
    let timeout = 5 * gstreamer::ClockTime::SECOND;
    let discoverer = Discoverer::new(timeout)?;

    let info = discoverer.discover_uri(uri.as_str())?;
    Ok(info_to_media_type(&info))
}

fn info_to_media_type(info: &DiscovererInfo) -> MediaType {
    let mut media_type = MediaType::Unknown;

    for stream in info.stream_list() {
        let stream_nick = stream.stream_type_nick();
        let is_image = stream_nick == "video(image)";
        let is_video = stream_nick == "video";
        let is_audio = stream_nick == "audio";

        media_type = match media_type {
            MediaType::VideoNoAudio if is_audio => MediaType::VideoWithAudio,
            MediaType::Audio if is_video => MediaType::VideoWithAudio,
            _ => {
                if is_video {
                    MediaType::VideoNoAudio
                } else if is_audio {
                    MediaType::Audio
                } else if is_image {
                    MediaType::Image
                } else {
                    let caps_str = if let Some(caps) = stream.caps() {
                        if caps.is_fixed() {
                            gstreamer_pbutils::pb_utils_get_codec_description(&caps)
                        } else {
                            glib::GString::from(caps.to_string())
                        }
                    } else {
                        glib::GString::from("")
                    };
                    eprintln!("Unhandled stream type: stream_nick={stream_nick} caps={caps_str}");
                    media_type
                }
            }
        };

        // If we already know it's got both video and audio, there's no point continuing.
        if let MediaType::VideoWithAudio = media_type {
            break;
        }
    }

    media_type
}
