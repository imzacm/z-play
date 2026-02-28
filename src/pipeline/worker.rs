use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use glib::object::ObjectExt;
use gstreamer::MessageView;
use gstreamer::prelude::{ElementExt, ElementExtManual, GstBinExt, PadExtManual};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;

use super::Event;

static WORKER_POOL: LazyLock<WorkerPool<3>> = LazyLock::new(WorkerPool::new);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PipelineId(u64);

impl PipelineId {
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

struct State {
    current_state: gstreamer::State,
}

#[derive(Clone)]
pub struct PipelineHandle {
    pub id: PipelineId,
    pub event_rx: flume::Receiver<Event>,
    state: Arc<Mutex<State>>,
}

impl PipelineHandle {
    pub fn new(pipeline: gstreamer::Pipeline) -> Self {
        let state = State { current_state: pipeline.current_state() };
        let state = Arc::new(Mutex::new(state));

        let id = PipelineId::new();
        let (event_tx, event_rx) = flume::unbounded();
        let worker = PipelineWorker { pipeline, event_tx, state: state.clone(), guard: None };
        WORKER_POOL.send(Command::AddPipeline(id, worker));
        Self { id, event_rx, state }
    }

    pub fn state(&self) -> gstreamer::State {
        self.state.lock().current_state
    }

    pub fn set_state(
        &self,
        state: gstreamer::State,
    ) -> flume::Receiver<Result<(), gstreamer::StateChangeError>> {
        let (result_tx, result_rx) = flume::bounded(1);
        WORKER_POOL.send(Command::SetState(self.id, state, result_tx));
        result_rx
    }

    pub fn seek(
        &self,
        time: gstreamer::ClockTime,
        rate: Option<f64>,
    ) -> flume::Receiver<Result<(), glib::BoolError>> {
        let (result_tx, result_rx) = flume::bounded(1);
        WORKER_POOL.send(Command::Seek(self.id, time, rate, result_tx));
        result_rx
    }

    pub fn set_video_size(&self, width: i32, height: i32) {
        WORKER_POOL.send(Command::SetVideoSize(self.id, width, height));
    }
}

impl Drop for PipelineHandle {
    fn drop(&mut self) {
        if self.event_rx.receiver_count() == 1 {
            WORKER_POOL.send(Command::RemovePipeline(self.id));
        }
    }
}

struct PipelineWorker {
    pipeline: gstreamer::Pipeline,
    event_tx: flume::Sender<Event>,
    state: Arc<Mutex<State>>,
    guard: Option<gstreamer::bus::BusWatchGuard>,
}

enum Command {
    AddPipeline(PipelineId, PipelineWorker),
    RemovePipeline(PipelineId),
    SetState(PipelineId, gstreamer::State, flume::Sender<Result<(), gstreamer::StateChangeError>>),
    Seek(PipelineId, gstreamer::ClockTime, Option<f64>, flume::Sender<Result<(), glib::BoolError>>),
    SetVideoSize(PipelineId, i32, i32),
}

struct WorkerPool<const N: usize> {
    command_senders: [flume::Sender<Command>; N],
    next_index: AtomicUsize,
    pipeline_worker_map: Arc<Mutex<FxHashMap<PipelineId, usize>>>,
}

impl<const N: usize> WorkerPool<N> {
    fn new() -> Self {
        assert!(N > 0, "Worker pool size must be greater than 0");

        let mut command_senders: [MaybeUninit<flume::Sender<Command>>; N] =
            [const { MaybeUninit::uninit() }; N];
        for elem in &mut command_senders[..] {
            let (tx, rx) = flume::unbounded();
            std::thread::spawn(move || worker_thread(rx));
            elem.write(tx);
        }
        // See https://www.reddit.com/r/rust/comments/kai7i5/cant_transmute_array_of_maybeuninitt_to_t_for/
        let command_senders =
            unsafe { command_senders.as_ptr().cast::<[flume::Sender<Command>; N]>().read() };

        let next_index = AtomicUsize::new(0);
        let pipeline_worker_map = Arc::new(Mutex::new(FxHashMap::default()));
        Self { command_senders, next_index, pipeline_worker_map }
    }

    fn next_index(&self) -> usize {
        let mut index = self.next_index.fetch_add(1, Ordering::Relaxed);
        if index >= self.command_senders.len() {
            index = 0;
            self.next_index.store(1, Ordering::Relaxed);
        }
        index
    }

    fn send(&self, command: Command) {
        let index = match &command {
            Command::AddPipeline(id, _) => {
                let index = self.next_index();
                self.pipeline_worker_map.lock().insert(*id, index);
                index
            }
            Command::RemovePipeline(id) => {
                self.pipeline_worker_map.lock().remove(id).expect("Pipeline not found")
            }
            Command::SetState(id, _, _)
            | Command::Seek(id, _, _, _)
            | Command::SetVideoSize(id, _, _) => {
                self.pipeline_worker_map.lock().get(id).copied().expect("Pipeline not found")
            }
        };

        self.command_senders[index].send(command).unwrap();
    }
}

fn worker_thread(command_rx: flume::Receiver<Command>) {
    let context = glib::MainContext::new();
    context.spawn_local(async move {
        let map = Rc::new(RefCell::new(FxHashMap::default()));

        loop {
            let map_weak = Rc::downgrade(&map);
            match command_rx.recv_async().await {
                Ok(Command::AddPipeline(id, mut worker)) => {
                    let bus = worker.pipeline.bus().unwrap();
                    let event_tx = worker.event_tx.clone();
                    let worker_state = worker.state.clone();
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

                                    if is_pipeline {
                                        worker_state.lock().current_state = to;
                                    }

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

                    worker.guard = Some(guard);
                    map.borrow_mut().insert(id, worker);
                }
                Ok(Command::RemovePipeline(id)) => {
                    let worker = map.borrow_mut().remove(&id).expect("Pipeline not found");
                    _ = worker.pipeline.set_state(gstreamer::State::Null);
                }
                Ok(Command::SetState(id, state, result_tx)) => {
                    let map = map.borrow();
                    let worker = map.get(&id).expect("Pipeline not found");
                    let result = worker.pipeline.set_state(state).map(|_| ());
                    _ = result_tx.send(result);
                }
                Ok(Command::Seek(id, time, rate, result_tx)) => {
                    let map = map.borrow();
                    let worker = map.get(&id).expect("Pipeline not found");

                    let result = if let Some(rate) = rate {
                        worker.pipeline.seek(
                            rate,
                            gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE,
                            gstreamer::SeekType::Set,
                            time,
                            gstreamer::SeekType::None,
                            gstreamer::ClockTime::ZERO,
                        )
                    } else {
                        worker.pipeline.seek_simple(
                            gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE,
                            time,
                        )
                    };
                    _ = result_tx.send(result);
                }
                Ok(Command::SetVideoSize(id, width, height)) => {
                    let map = map.borrow();
                    let worker = map.get(&id).expect("Pipeline not found");

                    let Some(caps_filter) = worker.pipeline.by_name("video_caps") else {
                        log::warn!("video_caps not found; ignoring resize to {width}x{height}");
                        continue;
                    };

                    let caps = gstreamer::Caps::builder("video/x-raw")
                        .field("format", gstreamer_video::VideoFormat::Rgba.to_str())
                        .field("pixel-aspect-ratio", gstreamer::Fraction::new(1, 1))
                        .field("width", width)
                        .field("height", height)
                        .build();

                    caps_filter.set_property("caps", &caps);

                    // Force downstream renegotiation ASAP.
                    if let Some(pad) = caps_filter.static_pad("src") {
                        pad.send_event(gstreamer::event::Reconfigure::new());
                    }

                    // Force an immediate new output frame by flushing at the current position.
                    if worker.pipeline.current_state() != gstreamer::State::Playing
                        && let Some(position) =
                            worker.pipeline.query_position::<gstreamer::ClockTime>()
                    {
                        _ = worker.pipeline.seek_simple(
                            gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE,
                            position,
                        );
                    }
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
