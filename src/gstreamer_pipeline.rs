use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};

use glib::object::{Cast, ObjectExt};
use gstreamer::MessageView;
use gstreamer::prelude::{
    ElementExt, ElementExtManual, GstBinExt, GstBinExtManual, GstObjectExt, PadExt, PadExtManual,
};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

use crate::Error;

#[derive(Debug)]
pub enum Event {
    EndOfStream,
    Error(String),
    StateChanged { from: gstreamer::State, to: gstreamer::State },
}

#[derive(Clone)]
pub struct Frame {
    pub width: u32,
    pub height: u32,
    /// RGBA
    pub data: Vec<u8>,
}

#[derive(Default, Clone)]
struct State {
    frame: Option<Frame>,
    audio_buffer: Vec<f32>,
    duration: gstreamer::ClockTime,
    position: gstreamer::ClockTime,
    is_image: bool,
}

pub struct Pipeline {
    pipeline: gstreamer::Pipeline,
    state: Arc<Mutex<State>>,
    event_rx: flume::Receiver<Event>,
    bus_id: BusId,
}

impl Pipeline {
    pub fn new<F>(path: PathBuf, on_sample: F) -> Result<Self, Error>
    where
        F: FnMut() + Send + 'static,
    {
        gstreamer::init().expect("Failed to initialize GStreamer");

        let state = Arc::new(Mutex::new(State::default()));
        let pipeline = create_pipeline(path, on_sample, state.clone())?;
        let (event_tx, event_rx) = flume::unbounded();

        let bus = pipeline.bus().unwrap();
        let bus_id = BusId::new();
        send_worker_command(WorkerCommand::AddBus(bus_id, bus, event_tx));

        Ok(Self { pipeline, state, event_rx, bus_id })
    }

    pub fn event_rx(&self) -> &flume::Receiver<Event> {
        &self.event_rx
    }

    pub fn path(&self) -> PathBuf {
        let file_src = self.pipeline.by_name("file_src").unwrap();
        file_src.property("location")
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
        self.pipeline.current_state()
    }

    pub fn set_state(&self, state: gstreamer::State) -> Result<(), Error> {
        self.pipeline.set_state(state)?;
        Ok(())
    }

    pub fn seek(&self, mut time: gstreamer::ClockTime, rate: Option<f64>) -> Result<(), Error> {
        {
            let mut state_lock = self.state.lock();
            time = state_lock.duration.min(time);
            state_lock.position = time;
        }

        if let Some(rate) = rate {
            self.pipeline.seek(
                rate,
                gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE,
                gstreamer::SeekType::Set,
                time,
                gstreamer::SeekType::None,
                gstreamer::ClockTime::ZERO,
            )?;
        } else {
            self.pipeline
                .seek_simple(gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE, time)?;
        }
        Ok(())
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        let pipeline = std::mem::replace(&mut self.pipeline, gstreamer::Pipeline::new());
        rayon::spawn(move || {
            _ = pipeline.set_state(gstreamer::State::Null);
        });
        send_worker_command(WorkerCommand::RemoveBus(self.bus_id));
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct BusId(u64);

impl BusId {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        Self(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

enum WorkerCommand {
    AddBus(BusId, gstreamer::Bus, flume::Sender<Event>),
    RemoveBus(BusId),
}

// Note: The worker thread will never end because `SENDER` is static.
fn send_worker_command(command: WorkerCommand) {
    static SENDER: LazyLock<flume::Sender<WorkerCommand>> = LazyLock::new(|| {
        let (sender, receiver) = flume::unbounded();
        std::thread::spawn(move || worker_thread(receiver));
        sender
    });

    SENDER.send(command).unwrap();
}

fn worker_thread(command_rx: flume::Receiver<WorkerCommand>) {
    let context = glib::MainContext::new();
    context.spawn_local(async move {
        let map = Rc::new(RefCell::new(HashMap::new()));

        loop {
            let map_weak = Rc::downgrade(&map);
            match command_rx.recv_async().await {
                Ok(WorkerCommand::AddBus(id, bus, event_tx)) => {
                    let guard = bus
                        .add_watch_local(move |_bus, msg| {
                            let mut remove_guard = false;
                            match msg.view() {
                                MessageView::Eos(_) => {
                                    if event_tx.send(Event::EndOfStream).is_err() {
                                        remove_guard = true;
                                    }
                                }
                                MessageView::Error(error) => {
                                    let error = format!(
                                        "Error on pipeline: {} (debug: {:?})",
                                        error.error(),
                                        error.debug()
                                    );
                                    if event_tx.send(Event::Error(error)).is_err() {
                                        remove_guard = true;
                                    }
                                }
                                // We only care about pipeline state changes.
                                MessageView::StateChanged(state) => {
                                    let is_pipeline = state
                                        .src()
                                        .is_some_and(|src| src.is::<gstreamer::Pipeline>());

                                    let from = state.old();
                                    let to = state.current();

                                    if is_pipeline
                                        && event_tx.send(Event::StateChanged { from, to }).is_err()
                                    {
                                        remove_guard = true;
                                    }
                                }
                                _ => (),
                            }

                            if remove_guard {
                                if let Some(map) = map_weak.upgrade() {
                                    map.borrow_mut().remove(&id);
                                }
                                glib::ControlFlow::Break
                            } else {
                                glib::ControlFlow::Continue
                            }
                        })
                        .expect("Failed to add bus watch");

                    map.borrow_mut().insert(id, guard);
                }
                Ok(WorkerCommand::RemoveBus(id)) => {
                    map.borrow_mut().remove(&id);
                }
                Err(flume::RecvError::Disconnected) => break,
            }
        }
    });

    let loop_ = glib::MainLoop::new(Some(&context), false);
    context
        .with_thread_default(move || {
            loop_.run();
        })
        .unwrap();
}
