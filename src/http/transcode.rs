use std::cell::LazyCell;
use std::fmt::Write;
use std::path::{Path, PathBuf};

use compio::BufResult;
use nvml_wrapper::Nvml;

use crate::http::FileKind;

#[thread_local]
static GPU_MONITOR: LazyCell<GpuMonitor> = LazyCell::new(GpuMonitor::new);

enum GpuMonitor {
    Nvidia(Nvml),
    Amd { device_path: PathBuf },
    Unknown,
}

impl GpuMonitor {
    fn new() -> Self {
        match Nvml::init() {
            Ok(nvml) => return Self::Nvidia(nvml),
            Err(error) => {
                eprintln!("Failed to initialize NVML: {error}");
            }
        }

        if let Ok(entries) = std::fs::read_dir("/sys/class/drm") {
            for entry in entries {
                let Ok(entry) = entry else { continue };
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Look for "card0", "card1", etc. Skip specific outputs like "card0-DP-1"
                if name_str.starts_with("card") && !name_str.contains('-') {
                    let device_path = entry.path().join("device");

                    // If this device has AMD memory info nodes, we found our GPU.
                    if device_path.join("mem_info_vram_total").exists() {
                        println!("Detected AMD GPU at {:?}", device_path);
                        return Self::Amd { device_path };
                    }
                }
            }
        }

        Self::Unknown
    }

    async fn available_vram_mb(&self) -> Option<u64> {
        match self {
            Self::Nvidia(nvml) => {
                let device = nvml
                    .device_by_index(0)
                    .inspect_err(|error| eprintln!("Failed to get device: {error}"))
                    .ok()?;

                let memory_info = device
                    .memory_info()
                    .inspect_err(|error| eprintln!("Failed to get memory info: {error}"))
                    .ok()?;

                Some(memory_info.free / (1024 * 1024))
            }
            Self::Amd { device_path } => {
                let total_path = device_path.join("mem_info_vram_total");
                let used_path = device_path.join("mem_info_vram_used");

                let total_bytes_future = read_file_u64(total_path);
                let used_bytes_future = read_file_u64(used_path);

                let (total_bytes, used_bytes) = tokio::join!(total_bytes_future, used_bytes_future);
                let total_bytes = total_bytes
                    .inspect_err(|error| eprintln!("Failed to read total VRAM: {error}"))
                    .ok()?;
                let used_bytes = used_bytes
                    .inspect_err(|error| eprintln!("Failed to read used VRAM: {error}"))
                    .ok()?;

                let free_bytes = total_bytes.saturating_sub(used_bytes);
                Some(free_bytes / (1024 * 1024))
            }
            Self::Unknown => None,
        }
    }
}

async fn read_file_u64<P>(path: P) -> Result<u64, std::io::Error>
where
    P: AsRef<Path>,
{
    let bytes = compio::fs::read(path).await?;
    let str = std::str::from_utf8(&bytes).map_err(|error| std::io::Error::other(error))?;
    let vram_bytes = str
        .trim()
        .parse::<u64>()
        .inspect_err(|error| eprintln!("Failed to parse VRAM value: {error}"))
        .map_err(|error| std::io::Error::other(error))?;
    Ok(vram_bytes)
}

async fn estimate_vram_required_mb<P>(path: P) -> Result<Option<u64>, std::io::Error>
where
    P: AsRef<Path>,
{
    let output = compio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "json",
        ])
        .arg(path.as_ref())
        .stdout(std::process::Stdio::piped())
        .unwrap()
        .stderr(std::process::Stdio::inherit())
        .unwrap()
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout)?;

    if let Some(stream) = json.get("streams").and_then(|v| v.get(0))
        && let Some(width) = stream.get("width").and_then(serde_json::Value::as_u64)
        && let Some(height) = stream.get("height").and_then(serde_json::Value::as_u64)
    {
        let pixels = width * height;

        // Approx 12 bytes per pixel for 8-bit YUV420 buffers
        // (1.5 bytes * ~8 frames of decode/encode/reference surfaces)
        let surface_vram_bytes = pixels * 12;

        // Base context overhead
        let base_overhead_bytes = match &*GPU_MONITOR {
            GpuMonitor::Nvidia(_) => {
                // ~40 MiB
                40 * 1024 * 1024
            }
            GpuMonitor::Amd { .. } => {
                // ~20 MiB
                20 * 1024 * 1024
            }
            GpuMonitor::Unknown => 0,
        };

        let total_bytes = base_overhead_bytes + surface_vram_bytes;

        // Add a 10% safety buffer and convert to MiB
        Ok(Some((total_bytes as f64 * 1.10) as u64 / (1024 * 1024)))
    } else {
        Ok(None)
    }
}

pub async fn should_transcode<P>(path: P) -> bool
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let Some(file_kind) = FileKind::from_path(path) else { return false };
    let is_gif = path.ends_with(".gif");

    if !matches!(file_kind, FileKind::Video) && !is_gif {
        return false;
    }

    let required_vram = match estimate_vram_required_mb(path).await {
        Ok(value) => value,
        Err(error) => {
            eprintln!("Failed to estimate VRAM required: {error}");
            return false;
        }
    };

    if let Some(required_vram) = required_vram
        && let Some(available_vram) = GPU_MONITOR.available_vram_mb().await
        && available_vram < required_vram
    {
        eprintln!(
            "Insufficient VRAM available for transcoding {}: {required_vram} MiB required, {available_vram} MiB available",
            path.display()
        );
        return false;
    }

    true
}

pub async fn get_video_duration<P>(path: P) -> Result<Option<f64>, std::io::Error>
where
    P: AsRef<Path>,
{
    let output = compio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(path.as_ref())
        .stdout(std::process::Stdio::piped())
        .unwrap()
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim().parse::<f64>().ok())
}

pub const FRAMES_PER_SECOND: u32 = 30;
pub const SEGMENT_SECS_U32: u32 = 2;
pub const SEGMENT_SECS_F64: f64 = 2.0;

fn vod_playlist_content(duration_sec: f64) -> String {
    let mut playlist = String::with_capacity(1024);
    playlist.push_str("#EXTM3U\n");
    playlist.push_str("#EXT-X-VERSION:7\n");
    writeln!(playlist, "#EXT-X-TARGETDURATION:{SEGMENT_SECS_U32}\n").unwrap();
    playlist.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
    playlist.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n"); // Required for fMP4 HLS

    let full_segments = (duration_sec / SEGMENT_SECS_F64).floor() as usize;
    let remainder = duration_sec % SEGMENT_SECS_F64;

    for i in 0..full_segments {
        // HLS spec requires max 6 decimal places
        write!(&mut playlist, "#EXTINF:{SEGMENT_SECS_F64:.6},\n").unwrap();
        write!(&mut playlist, "seg_{i:03}.m4s\n").unwrap();
    }

    if remainder > 0.0 {
        write!(&mut playlist, "#EXTINF:{remainder:.6},\n").unwrap();
        write!(&mut playlist, "seg_{full_segments:03}.m4s\n").unwrap();
    }

    playlist.push_str("#EXT-X-ENDLIST\n");
    playlist
}

pub async fn create_vod_playlist<P>(
    playlist_path: P,
    duration_secs: f64,
) -> Result<(), std::io::Error>
where
    P: AsRef<Path>,
{
    let playlist_content = vod_playlist_content(duration_secs);
    let BufResult(result, _) = compio::fs::write(playlist_path, playlist_content).await;
    result?;
    Ok(())
}

pub fn spawn_transcode_hls<P, OP>(
    path: P,
    output_path: OP,
    playlist_name: &str,
    start_segment: Option<u16>,
) -> Result<std::process::Child, std::io::Error>
where
    P: AsRef<Path>,
    OP: AsRef<Path>,
{
    let mut command = std::process::Command::new("ffmpeg");

    command.args(["-analyzeduration", "10000000", "-probesize", "5000000"]);

    match &*GPU_MONITOR {
        GpuMonitor::Nvidia(_) => {
            command.args(["-hwaccel", "cuda"]);
        }
        GpuMonitor::Amd { .. } => {
            command.args([
                "-hwaccel",
                "vaapi",
                "-hwaccel_device",
                "/dev/dri/renderD128",
                "-hwaccel_output_format",
                "vaapi",
            ]);
        }
        GpuMonitor::Unknown => (),
    }

    command.arg("-noautorotate");

    if let Some(start_segment) = start_segment {
        let start_time_secs = SEGMENT_SECS_F64 * start_segment as f64;
        command.args(["-ss", &start_time_secs.to_string()]);
    }

    command.arg("-i").arg(path.as_ref());

    if start_segment.is_some() {
        command.arg("-copyts");
    }

    command.args(["-map", "0:v:0", "-map", "0:a:0?"]);

    match &*GPU_MONITOR {
        GpuMonitor::Nvidia(_) => {
            command.args(["-c:v", "h264_nvenc", "-preset", "p3", "-no-scenecut", "1"]);
        }
        GpuMonitor::Amd { .. } => {
            command.args(["-c:v", "h264_vaapi", "-vf", "scale_vaapi=format=nv12"]);
        }
        GpuMonitor::Unknown => {
            command.args([
                "-c:v",
                "libx264",
                // Disable scene change detection so the 60-frame interval is strict
                "-sc_threshold",
                "0",
            ]);
        }
    }

    let gop_size = (FRAMES_PER_SECOND * SEGMENT_SECS_U32).to_string();

    command.args([
        "-r",
        &FRAMES_PER_SECOND.to_string(),
        // Force a keyframe every 60 frames for consistent MP4 fragmentation
        "-g",
        &gop_size,
        "-maxrate",
        "8M",
        "-bufsize",
        "16M",
    ]);

    command.args([
        "-c:a",
        "aac",
        "-ac",
        "2",
        "-f",
        "hls",
        "-hls_time",
        &SEGMENT_SECS_U32.to_string(),
        "-hls_list_size",
        "0",
        "-hls_segment_type",
        "fmp4",
        "-hls_flags",
        "independent_segments",
    ]);

    if let Some(start_segment) = start_segment {
        command.args(["-start_number", &start_segment.to_string()]);
    }

    let output_path = output_path.as_ref();
    command.arg("-hls_segment_filename").arg(output_path.join("seg_%03d.m4s"));

    let playlist_path = output_path.join(playlist_name);
    command.arg(&playlist_path);

    let child = command
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    Ok(child)
}
