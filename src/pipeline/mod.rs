use gstreamer::prelude::{PadExt, PadExtManual};
mod event;
mod state;
mod worker;

use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use event::Event;
use glib::object::{Cast, ObjectExt};
use gstreamer::prelude::{ElementExt, ElementExtManual, GstBinExt, GstBinExtManual, GstObjectExt};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
pub use state::Frame;
use state::State;

use crate::Error;

pub struct Pipeline {
    pipeline: worker::PipelineHandle,
    state: Arc<Mutex<State>>,
    path: PathBuf,
}

impl Pipeline {
    pub fn new<F>(path: PathBuf, on_sample: F) -> Result<Self, Error>
    where
        F: FnMut() + Send + 'static,
    {
        gstreamer::init().expect("Failed to initialize GStreamer");

        let state = Arc::new(Mutex::new(State::default()));
        let pipeline = create_pipeline(path.clone(), on_sample, state.clone())?;
        let pipeline = worker::PipelineHandle::new(pipeline);
        Ok(Self { pipeline, state, path })
    }

    pub fn event_rx(&self) -> &flume::Receiver<Event> {
        &self.pipeline.event_rx
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn frame(&self) -> MappedMutexGuard<'_, Option<Frame>> {
        let lock = self.state.lock();
        MutexGuard::map(lock, |state| &mut state.frame)
    }

    pub fn take_audio_buffer(&self) -> Vec<f32> {
        let mut lock = self.state.lock();
        std::mem::take(&mut lock.audio_buffer)
    }

    pub fn is_image(&self) -> bool {
        self.state.lock().is_image
    }

    pub fn duration(&self) -> gstreamer::ClockTime {
        self.state.lock().duration
    }

    pub fn position(&self) -> gstreamer::ClockTime {
        self.state.lock().position
    }

    pub fn state(&self) -> gstreamer::State {
        self.pipeline.state()
    }

    pub fn set_state(
        &self,
        state: gstreamer::State,
    ) -> flume::Receiver<Result<(), gstreamer::StateChangeError>> {
        self.pipeline.set_state(state)
    }

    pub fn seek(
        &self,
        mut time: gstreamer::ClockTime,
        rate: Option<f64>,
    ) -> flume::Receiver<Result<(), glib::BoolError>> {
        {
            let mut state_lock = self.state.lock();
            time = state_lock.duration.min(time);
            state_lock.position = time;
        }

        self.pipeline.seek(time, rate)
    }
}

fn create_pipeline<F>(
    path: PathBuf,
    mut on_sample: F,
    state: Arc<Mutex<State>>,
) -> Result<gstreamer::Pipeline, Error>
where
    F: FnMut() + Send + 'static,
{
    let pipeline = gstreamer::Pipeline::builder().name("player-pipeline").build();

    let file_src = gstreamer::ElementFactory::make("filesrc")
        .name("file_src")
        .property("location", path.as_path())
        .build()?;

    let decode_bin = gstreamer::ElementFactory::make("decodebin3").build()?;

    pipeline.add_many([&file_src, &decode_bin])?;
    file_src.link(&decode_bin)?;

    let (video_bin, video_app_sink) = create_video_bin()?;
    let audio_bin = create_audio_bin()?;

    // Dynamic linking
    let pipeline_weak = pipeline.downgrade();
    let state_clone = state.clone();

    decode_bin.connect_pad_added(move |_decode_bin, src_pad| {
        let pad_name = src_pad.name();
        log::info!("Decoder: New pad added: {pad_name} - {}", path.display());

        let Some(pipeline) = pipeline_weak.upgrade() else { return };

        if pad_name.starts_with("video_") {
            if let Err(error) = pipeline.add(&video_bin) {
                log::error!("Failed to add video bin: {error}");
                return;
            }
            if let Err(error) = video_bin.sync_state_with_parent() {
                log::error!("Failed to sync video bin: {error}");
                return;
            }

            let is_image = src_pad
                .query_duration::<gstreamer::ClockTime>()
                .is_none_or(|d| d == gstreamer::ClockTime::ZERO);

            let mut video_sink_pad = video_bin.static_pad("sink").unwrap();

            if is_image {
                let mut state_lock = state_clone.lock();
                assert!(!state_lock.is_image);
                state_lock.is_image = true;
                video_sink_pad = match add_image_elements(&pipeline, &video_bin) {
                    Ok(pad) => pad,
                    Err(error) => {
                        log::error!("Failed to add image elements: {error}");
                        return;
                    }
                }
            }

            if video_sink_pad.is_linked() {
                log::info!("Video pad already linked");
                return;
            }

            if let Err(error) = src_pad.link(&video_sink_pad) {
                log::error!("Failed to link video pad: {error}");
            }
        } else if pad_name.starts_with("audio_") {
            let audio_sink_pad = audio_bin.static_pad("sink").unwrap();

            if audio_sink_pad.is_linked() {
                log::info!("Audio pad already linked");
                return;
            }

            if let Err(error) = pipeline.add(&audio_bin) {
                log::error!("Failed to add audio bin: {error}");
                return;
            }
            if let Err(error) = audio_bin.sync_state_with_parent() {
                log::error!("Failed to sync audio bin: {error}");
                return;
            }

            if let Err(error) = src_pad.link(&audio_sink_pad) {
                log::error!("Failed to link audio pad: {error}");
            }
        } else {
            log::info!("Unknown pad type: {pad_name}");
        }
    });

    let pipeline_weak = pipeline.downgrade();
    let state_clone = state.clone();
    video_app_sink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let state = &state_clone;

                let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Eos)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                let caps = sample.caps().ok_or(gstreamer::FlowError::Error)?;
                let info = gstreamer_video::VideoInfo::from_caps(caps)
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let map = buffer.map_readable().map_err(|_| gstreamer::FlowError::Error)?;

                let frame =
                    Frame { width: info.width(), height: info.height(), data: map.to_vec() };

                {
                    let mut state_lock = state.lock();
                    state_lock.frame = Some(frame);

                    if state_lock.duration == gstreamer::ClockTime::ZERO
                        && let Some(pipeline) = pipeline_weak.upgrade()
                    {
                        if let Some(duration) = pipeline.query_duration::<gstreamer::ClockTime>() {
                            state_lock.duration = duration;
                        } else if state_lock.is_image {
                            state_lock.duration = gstreamer::ClockTime::from_seconds(10);
                        }
                    }

                    if let Some(pts) = buffer.pts()
                        && state_lock.position < pts
                    {
                        state_lock.position = state_lock.duration.min(pts);
                    }

                    if state_lock.is_image
                        && state_lock.position == state_lock.duration
                        && let Some(pipeline) = pipeline_weak.upgrade()
                    {
                        let bus = pipeline.bus().unwrap();
                        let msg = gstreamer::message::Eos::new();
                        bus.post(msg).expect("Failed to post EOS message on bus");
                    }
                }

                on_sample();
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    Ok(pipeline)
}

fn add_image_elements(
    pipeline: &gstreamer::Pipeline,
    video_bin: &gstreamer::Bin,
) -> Result<gstreamer::Pad, Error> {
    let convert = gstreamer::ElementFactory::make("videoconvert").build()?;
    let image_freeze = gstreamer::ElementFactory::make("imagefreeze")
        // 300 frames at 30 fps = 10 seconds of video
        // .property("num-buffers", 300)
        .build()?;

    let caps = gstreamer::Caps::builder("video/x-raw")
        .field("framerate", gstreamer::Fraction::new(30, 1))
        .build();
    let caps = gstreamer::ElementFactory::make("capsfilter").property("caps", caps).build()?;

    pipeline.add_many([&convert, &image_freeze, &caps])?;
    gstreamer::Element::link_many([&convert, &image_freeze, &caps, video_bin.upcast_ref()])?;

    for element in [&convert, &image_freeze, &caps] {
        element.sync_state_with_parent()?;
    }

    Ok(convert.static_pad("sink").unwrap())
}

fn create_video_bin() -> Result<(gstreamer::Bin, gstreamer_app::AppSink), Error> {
    let bin = gstreamer::Bin::builder().name("video_bin").build();

    let convert = gstreamer::ElementFactory::make("videoconvert").build()?;
    let scale = gstreamer::ElementFactory::make("videoscale").build()?;
    let app_sink = gstreamer_app::AppSink::builder()
        .drop(true)
        .max_buffers(1)
        .caps(
            &gstreamer::Caps::builder("video/x-raw")
                .field("format", gstreamer_video::VideoFormat::Rgba.to_str())
                .build(),
        )
        .build();

    bin.add_many([&convert, &scale, app_sink.upcast_ref()])?;
    gstreamer::Element::link_many([&convert, &scale, app_sink.upcast_ref()])?;

    let sink_pad = convert.static_pad("sink").expect("no videoconvert sink pad");
    let ghost_pad = gstreamer::GhostPad::with_target(&sink_pad)?;
    bin.add_pad(&ghost_pad)?;

    Ok((bin, app_sink))
}

fn create_audio_bin() -> Result<gstreamer::Bin, Error> {
    let bin = gstreamer::Bin::builder().name("audio_bin").build();

    let convert_in = gstreamer::ElementFactory::make("audioconvert").build()?;
    let scale_tempo = gstreamer::ElementFactory::make("scaletempo").build()?;
    let rate = gstreamer::ElementFactory::make("audiorate").build()?;
    let resample = gstreamer::ElementFactory::make("audioresample").build()?;
    let sink = gstreamer::ElementFactory::make("autoaudiosink").build()?;

    bin.add_many([&convert_in, &scale_tempo, &rate, &resample, &sink])?;
    gstreamer::Element::link_many([&convert_in, &scale_tempo, &rate, &resample, &sink])?;

    let sink_pad = convert_in.static_pad("sink").expect("no audioconvert sink pad");
    let ghost_pad = gstreamer::GhostPad::with_target(&sink_pad)?;
    bin.add_pad(&ghost_pad)?;

    Ok(bin)
}
