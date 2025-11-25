use std::path::Path;
use std::sync::Arc;

use gstreamer::prelude::*;
use gstreamer_pbutils::prelude::DiscovererStreamInfoExt;
use gstreamer_pbutils::{
    Discoverer, DiscovererContainerInfo, DiscovererResult, DiscovererStreamInfo,
};
use parking_lot::Mutex;

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

fn add_stream_info(info: &DiscovererStreamInfo, media_type: &Mutex<MediaType>) {
    let stream_nick = info.stream_type_nick();

    if stream_nick == "container" {
        return;
    }

    let caps_str = if let Some(caps) = info.caps() {
        if caps.is_fixed() {
            gstreamer_pbutils::pb_utils_get_codec_description(&caps)
        } else {
            glib::GString::from(caps.to_string())
        }
    } else {
        glib::GString::from("")
    };

    let mut media_type = media_type.lock();

    let is_image = stream_nick == "video(image)";
    let is_video = stream_nick == "video";
    let is_audio = stream_nick == "audio";

    *media_type = match *media_type {
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
                eprintln!("Unhandled stream type: stream_nick={stream_nick} caps={caps_str}");
                *media_type
            }
        }
    };
}

fn add_topology(info: &DiscovererStreamInfo, media_type: &Mutex<MediaType>) {
    add_stream_info(info, media_type);

    if let Some(next) = info.next() {
        add_topology(&next, media_type);
    } else if let Some(container_info) = info.downcast_ref::<DiscovererContainerInfo>() {
        for stream in container_info.streams() {
            add_topology(&stream, media_type);
        }
    }
}

fn detect_media_type(path: &Path) -> Result<MediaType, Error> {
    gstreamer::init()?;

    let loop_ = glib::MainLoop::new(None, false);
    let timeout = 5 * gstreamer::ClockTime::SECOND;

    let uri = glib::filename_to_uri(path, None)?;
    let discoverer = Discoverer::new(timeout)?;

    let media_type = Arc::new(Mutex::new(MediaType::Unknown));

    let media_type_clone = media_type.clone();
    discoverer.connect_discovered(move |_discoverer, info, error| {
        let uri = info.uri();
        match info.result() {
            DiscovererResult::Ok => {
                // println!("Discovered {uri}");
            }
            DiscovererResult::UriInvalid => eprintln!("Invalid uri {uri}"),
            DiscovererResult::Error => {
                if let Some(msg) = error {
                    eprintln!("{msg}");
                } else {
                    eprintln!("Unknown error")
                }
            }
            DiscovererResult::Timeout => eprintln!("Timeout"),
            DiscovererResult::Busy => eprintln!("Busy"),
            DiscovererResult::MissingPlugins => {
                if let Some(s) = info.misc() {
                    eprintln!("{s}");
                }
            }
            _ => eprintln!("Unknown result"),
        }

        if info.result() != DiscovererResult::Ok {
            return;
        }

        if let Some(stream_info) = info.stream_info() {
            add_topology(&stream_info, &media_type_clone);
        }
    });

    let loop_clone = loop_.clone();
    discoverer.connect_finished(move |_| loop_clone.quit());
    discoverer.start();
    discoverer.discover_uri_async(&uri)?;

    loop_.run();
    discoverer.stop();

    let media_type = *media_type.lock();
    Ok(media_type)
}
