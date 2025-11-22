use std::path::Path;
use std::sync::Arc;

use glib::object::{Cast, ObjectExt};
use gstreamer::MessageView;
use gstreamer::prelude::{
    ElementExt, ElementExtManual, GstBinExt, GstBinExtManual, GstObjectExt, PadExt,
};
use parking_lot::Mutex;

use crate::Error;

#[derive(Debug)]
pub enum Event {
    EofOfStream,
    Error(String),
    StateChanged(gstreamer::State),
}

#[derive(Clone)]
pub struct Frame {
    pub width: u32,
    pub height: u32,
    /// RGBA
    pub data: Vec<u8>,
}

#[derive(Default, Clone)]
pub struct State {
    pub frame: Option<Frame>,
    pub egui_context: Option<eframe::egui::Context>,
}

pub struct Player {
    pipeline: gstreamer::Pipeline,
    state: Arc<Mutex<State>>,
    event_rx: flume::Receiver<Event>,
    main_loop: glib::MainLoop,
}

impl Player {
    pub fn new() -> Result<Self, Error> {
        let state = Arc::new(Mutex::new(State::default()));
        let pipeline = create_pipeline(state.clone())?;

        let main_loop = glib::MainLoop::new(None, false);

        let (event_tx, event_rx) = flume::unbounded();
        let main_loop_clone = main_loop.clone();
        let bus = pipeline.bus().unwrap();
        std::thread::spawn(move || {
            let _bus_watch = bus
                .add_watch_local(move |_, msg| {
                    match msg.view() {
                        MessageView::Eos(_) => {
                            if event_tx.send(Event::EofOfStream).is_err() {
                                main_loop_clone.quit();
                            }
                        }
                        MessageView::Error(error) => {
                            let error = format!(
                                "Error on pipeline: {} (debug: {:?})",
                                error.error(),
                                error.debug()
                            );
                            if event_tx.send(Event::Error(error)).is_err() {
                                main_loop_clone.quit();
                            }
                        }
                        // We only care about pipeline state changes.
                        MessageView::StateChanged(state) => {
                            let is_pipeline =
                                state.src().is_some_and(|src| src.is::<gstreamer::Pipeline>());
                            if is_pipeline
                                && event_tx.send(Event::StateChanged(state.current())).is_err()
                            {
                                main_loop_clone.quit();
                            }
                        }
                        _ => (),
                    }

                    glib::ControlFlow::Continue
                })
                .expect("Failed to add bus watch");
        });

        Ok(Self { pipeline, state, event_rx, main_loop })
    }

    pub fn state(&self) -> &Arc<Mutex<State>> {
        &self.state
    }

    pub fn event_rx(&self) -> &flume::Receiver<Event> {
        &self.event_rx
    }

    pub fn main_loop(&self) -> &glib::MainLoop {
        &self.main_loop
    }

    pub fn duration(&self) -> Option<gstreamer::ClockTime> {
        self.pipeline.query_duration()
    }

    pub fn position(&self) -> Option<gstreamer::ClockTime> {
        self.pipeline.query_position()
    }

    pub fn uri(&self) -> Option<glib::GString> {
        let src = self.pipeline.by_name("uri_decode").unwrap();
        src.property("uri")
    }

    pub fn set_path(&self, path: &Path) -> Result<(), Error> {
        let uri = glib::filename_to_uri(path, None)?;
        self.set_uri(uri)
    }

    pub fn set_uri(&self, uri: glib::GString) -> Result<(), Error> {
        _ = self.pipeline.set_state(gstreamer::State::Paused);

        let video_bin = self.pipeline.by_name("video_bin").unwrap();
        let video_sink_pad = video_bin.static_pad("sink").unwrap();

        if video_sink_pad.is_linked() {
            let peer = video_sink_pad.peer().unwrap();
            peer.unlink(&video_sink_pad)?;
        }

        let audio_bin = self.pipeline.by_name("audio_bin").unwrap();
        let audio_sink_pad = audio_bin.static_pad("sink").unwrap();

        if audio_sink_pad.is_linked() {
            let peer = audio_sink_pad.peer().unwrap();
            peer.unlink(&audio_sink_pad)?;
        }

        let src = self.pipeline.by_name("uri_decode").unwrap();
        src.set_property("uri", uri);

        self.pipeline.set_state(gstreamer::State::Ready)?;

        Ok(())
    }

    pub fn is_playing(&self) -> bool {
        let state = self.pipeline.current_state();
        println!("Pipeline state: {state:?}");
        self.pipeline.current_state() == gstreamer::State::Playing
    }

    pub fn play(&self) -> Result<(), Error> {
        println!("Playing...");
        self.pipeline.set_state(gstreamer::State::Playing)?;
        Ok(())
    }

    pub fn pause(&self) -> Result<(), Error> {
        println!("Pausing...");
        self.pipeline.set_state(gstreamer::State::Paused)?;
        Ok(())
    }

    pub fn stop(&self) -> Result<(), Error> {
        println!("Stopping...");
        self.pipeline.set_state(gstreamer::State::Null)?;
        Ok(())
    }
}

fn create_pipeline(state: Arc<Mutex<State>>) -> Result<gstreamer::Pipeline, Error> {
    let pipeline = gstreamer::Pipeline::builder().name("player-pipeline").build();

    let decode_bin = gstreamer::ElementFactory::make("uridecodebin3").name("uri_decode").build()?;
    let video_bin = create_video_bin(state.clone())?;
    let audio_bin = create_audio_bin()?;

    pipeline.add_many([&decode_bin, video_bin.upcast_ref(), audio_bin.upcast_ref()])?;

    video_bin.sync_state_with_parent()?;
    audio_bin.sync_state_with_parent()?;

    // Dynamic linking
    let video_sink_pad = video_bin.static_pad("sink").unwrap();
    let video_sink_pad_weak = video_sink_pad.downgrade();

    let audio_sink_pad = audio_bin.static_pad("sink").unwrap();
    let audio_sink_pad_weak = audio_sink_pad.downgrade();

    decode_bin.connect_pad_added(move |_, src_pad| {
        let pad_name = src_pad.name();
        println!("Decoder: New pad added: {pad_name}");

        if pad_name.starts_with("video_") {
            let Some(video_sink_pad) = video_sink_pad_weak.upgrade() else { return };
            if !video_sink_pad.is_linked()
                && let Err(error) = src_pad.link(&video_sink_pad)
            {
                eprintln!("Failed to link video pad: {error}");
            }
        } else if pad_name.starts_with("audio_") {
            let Some(audio_sink_pad) = audio_sink_pad_weak.upgrade() else { return };
            if !audio_sink_pad.is_linked()
                && let Err(error) = src_pad.link(&audio_sink_pad)
            {
                eprintln!("Failed to link audio pad: {error}");
            }
        } else {
            eprintln!("Unknown pad type: {pad_name}");
        }
    });

    Ok(pipeline)
}

fn create_video_bin(state: Arc<Mutex<State>>) -> Result<gstreamer::Bin, Error> {
    let bin = gstreamer::Bin::builder().name("video_bin").build();

    let convert = gstreamer::ElementFactory::make("videoconvert").build()?;
    let scale = gstreamer::ElementFactory::make("videoscale").build()?;
    let caps = gstreamer::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gstreamer::Caps::builder("video/x-raw")
                .field("format", gstreamer_video::VideoFormat::Rgba.to_str())
                .build(),
        )
        .build()?;

    let app_sink = gstreamer_app::AppSink::builder().drop(true).max_buffers(1).build();

    bin.add_many([&convert, &scale, &caps, app_sink.upcast_ref()])?;
    gstreamer::Element::link_many([&convert, &scale, &caps, app_sink.upcast_ref()])?;

    let sink_pad = convert.static_pad("sink").expect("no videoconvert sink pad");
    let ghost_pad = gstreamer::GhostPad::with_target(&sink_pad)?;
    bin.add_pad(&ghost_pad)?;

    app_sink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Eos)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                let caps = sample.caps().ok_or(gstreamer::FlowError::Error)?;
                let info = gstreamer_video::VideoInfo::from_caps(caps)
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let map = buffer.map_readable().map_err(|_| gstreamer::FlowError::Error)?;

                let frame =
                    Frame { width: info.width(), height: info.height(), data: map.to_vec() };

                let mut state_lock = state.lock();
                state_lock.frame = Some(frame);

                if let Some(ctx) = &state_lock.egui_context {
                    ctx.request_repaint();
                }

                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    Ok(bin)
}

fn create_audio_bin() -> Result<gstreamer::Bin, Error> {
    let bin = gstreamer::Bin::builder().name("audio_bin").build();

    let convert = gstreamer::ElementFactory::make("audioconvert").build()?;
    let resample = gstreamer::ElementFactory::make("audioresample").build()?;
    let sink = gstreamer::ElementFactory::make("autoaudiosink").build()?;

    bin.add_many([&convert, &resample, &sink])?;
    gstreamer::Element::link_many([&convert, &resample, &sink])?;

    let sink_pad = convert.static_pad("sink").expect("no audioconvert sink pad");
    let ghost_pad = gstreamer::GhostPad::with_target(&sink_pad)?;
    bin.add_pad(&ghost_pad)?;

    Ok(bin)
}
