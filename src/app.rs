use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;

use eframe::egui;
use glib::clone::Downgrade;
use keepawake::KeepAwake;
use parking_lot::Mutex;

use crate::path_cache::PathCache;
use crate::pipeline::{Event, Pipeline};
use crate::playback_speed::PlaybackSpeed;
use crate::random_files::random_file_with_timeout;
use crate::ui::PipelineRecv;
use crate::{Error, ui};

const MAX_QUEUE_SIZE: usize = 20;
const MAX_PRE_ROLL_QUEUE_SIZE: usize = 10;

pub struct App {
    root_paths: Arc<Mutex<Vec<PathBuf>>>,
    player: ui::PlayerUi,
    error: Option<Error>,
    queue: Arc<Mutex<VecDeque<Pipeline>>>,
    fullscreen: bool,
    playback_speed: PlaybackSpeed,
    keep_awake: Option<KeepAwake>,
    pipeline_receivers: Vec<PipelineRecv>,
}

impl App {
    pub fn new<I>(root_paths: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let root_paths = root_paths.into_iter().map(Into::into).collect();

        Self {
            root_paths: Arc::new(Mutex::new(root_paths)),
            player: ui::PlayerUi::default(),
            error: None,
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_QUEUE_SIZE))),
            fullscreen: false,
            playback_speed: PlaybackSpeed::default(),
            keep_awake: None,
            pipeline_receivers: Vec::with_capacity(2),
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if Arc::weak_count(&self.queue) == 0 {
            let queue = self.queue.downgrade();
            start_file_feeder(ctx, queue, self.root_paths.clone());
        }

        self.pipeline_receivers.retain(|rx| {
            if let Some(Err(error)) = rx.try_recv() {
                self.error = Some(error);
                return false;
            }
            !rx.is_disconnected()
        });

        let mut load_next_file = self.player.pipeline().is_none();

        egui::CentralPanel::default().show(ctx, |ui| {
            if !self.fullscreen {
                let mut root_paths = self.root_paths.lock();
                let roots_response = ui::roots_ui(ui, &mut root_paths);

                if roots_response.changed {
                    let mut queue_lock = self.queue.lock();
                    queue_lock.retain(|pipeline| {
                        let path = pipeline.path();

                        // Retain where root exists.
                        root_paths.iter().any(|root| path.starts_with(root))
                    });
                }

                ui::queue_ui(ui, &mut self.queue.lock());
            }

            ui.horizontal(|ui| {
                if let Some(error) = &self.error {
                    ui.label(format!("Error: {error}"));
                }
            });

            let mut toggle_fullscreen_button = if self.fullscreen {
                ctx.input(|i| i.key_released(egui::Key::Escape) || i.key_released(egui::Key::F11))
            } else {
                ctx.input(|i| i.key_released(egui::Key::F11))
            };

            ui.horizontal(|ui| {
                let next_button = ui.button("Next");
                if next_button.clicked() {
                    load_next_file = true;
                }

                let fullscreen_text =
                    if self.fullscreen { "Exit fullscreen" } else { "Fullscreen" };
                let fullscreen_button = ui.button(fullscreen_text);
                if fullscreen_button.clicked() {
                    toggle_fullscreen_button = true;
                }

                egui::ComboBox::from_id_salt("playback_speed")
                    .selected_text(self.playback_speed.as_str())
                    .show_ui(ui, |ui| {
                        for speed in PlaybackSpeed::all() {
                            ui.selectable_value(&mut self.playback_speed, speed, speed.as_str());
                        }
                    });
            });

            if self.playback_speed.rate() != self.player.rate()
                && let Some(rx) = self.player.set_rate(self.playback_speed.rate())
            {
                self.pipeline_receivers.push(rx);
            }

            if toggle_fullscreen_button {
                self.fullscreen = !self.fullscreen;
                ui.ctx().send_viewport_cmd(egui::ViewportCommand::Fullscreen(self.fullscreen));
                // ui.ctx().request_repaint();
            }

            let player_response = self.player.ui(ui, self.fullscreen);
            if let Some(error) = player_response.error {
                self.error = Some(error);
                ui.ctx().request_repaint();
            }

            if player_response.finished {
                load_next_file = true;
            }
        });

        if load_next_file && let Some(pipeline) = self.queue.lock().pop_front() {
            let result_rx = pipeline.set_state(gstreamer::State::Playing);
            self.pipeline_receivers.push(PipelineRecv::SetState(result_rx));
            self.player.clear();
            self.player.swap_pipeline(pipeline);
            if let Some(rx) = self.player.set_rate(self.player.rate()) {
                self.pipeline_receivers.push(rx);
            }
        }

        // ctx.request_repaint();

        let playing = self.player.is_playing();
        if playing && self.keep_awake.is_none() {
            let keep_awake_result = keepawake::Builder::default()
                .display(true)
                .reason("Video playback")
                .app_name("Z-Play")
                .app_reverse_domain("io.github.imzacm.z-play")
                .create();

            match keep_awake_result {
                Ok(keep_awake) => self.keep_awake = Some(keep_awake),
                Err(error) => {
                    log::error!("Failed to create keep awake: {error}");
                }
            }
        } else if !playing {
            self.keep_awake = None;
        }
    }
}

fn pre_roll_loop(
    pipeline_rx: flume::Receiver<Pipeline>,
    ctx: egui::Context,
    out_queue: Weak<Mutex<VecDeque<Pipeline>>>,
) {
    let mut queue = VecDeque::<Pipeline>::with_capacity(MAX_PRE_ROLL_QUEUE_SIZE);
    loop {
        if pipeline_rx.is_disconnected() {
            log::info!("Pipeline rx disconnected, stopping pre-roll loop");
            break;
        }

        let Some(out_queue) = out_queue.upgrade() else {
            log::info!("Queue dropped, stopping pre_roll loop");
            break;
        };

        let queue_len = queue.len();
        if queue_len != 0 {
            'pre_roll: for _ in 0..queue_len {
                let pipeline = queue.pop_front().unwrap();
                let path = pipeline.path();

                let mut is_paused = pipeline.state() == gstreamer::State::Paused;

                let event_rx = pipeline.event_rx();
                let timeout = std::time::Duration::from_millis(100);

                let iter = event_rx.recv_timeout(timeout).into_iter().chain(event_rx.try_iter());
                for event in iter {
                    match event {
                        Event::Error(error) => {
                            log::error!("Error on player for {path:?}: {error}");
                            continue 'pre_roll;
                        }
                        Event::StateChanged { from: _, to: gstreamer::State::Ready } => (),
                        Event::StateChanged { from: _, to: gstreamer::State::Paused } => {
                            is_paused = true;
                        }
                        event => log::info!("Unhandled event on player for {path:?} - {event:?}"),
                    }
                }

                if is_paused && out_queue.lock().len() < MAX_QUEUE_SIZE {
                    let path = pipeline.path();
                    log::info!("Queueing {}", path.display());

                    out_queue.lock().push_back(pipeline);
                    ctx.request_repaint();
                } else {
                    queue.push_back(pipeline);
                }
            }
        }

        if queue.len() >= MAX_PRE_ROLL_QUEUE_SIZE {
            continue;
        }

        let timeout = std::time::Duration::from_millis(100);
        let pipeline = match pipeline_rx.recv_timeout(timeout) {
            Ok(pipeline) => pipeline,
            Err(flume::RecvTimeoutError::Timeout) => continue,
            Err(flume::RecvTimeoutError::Disconnected) => {
                log::info!("Pipeline rx disconnected, stopping pre-roll loop");
                break;
            }
        };
        queue.push_back(pipeline);

        for pipeline in pipeline_rx.try_iter() {
            queue.push_back(pipeline);
            if queue.len() >= MAX_PRE_ROLL_QUEUE_SIZE {
                break;
            }
        }
    }
}

fn pipeline_loop(
    ctx: egui::Context,
    root_paths: Arc<Mutex<Vec<PathBuf>>>,
    pipeline_tx: flume::Sender<Pipeline>,

    // Used for dynamic timeout.
    queue: Weak<Mutex<VecDeque<Pipeline>>>,
) {
    let mut cache = PathCache::default();

    loop {
        if pipeline_tx.is_disconnected() {
            log::info!("Pipeline tx disconnected, stopping pipeline loop");
            break;
        }

        log::info!("Starting pipeline loop");
        let roots = root_paths.lock().clone();

        let queue_len = match queue.upgrade() {
            Some(queue) => queue.lock().len(),
            None => {
                log::info!("Queue dropped, stopping pipeline loop");
                break;
            }
        };

        // Start at 100ms and scale up to 10s based on queue length.
        let timeout_ms = 100 + (9900 * queue_len / MAX_QUEUE_SIZE);
        let busy_timeout = Duration::from_millis(timeout_ms as u64);
        let scan_timeout = busy_timeout * 2;

        let path = match random_file_with_timeout(roots, scan_timeout, busy_timeout) {
            Some(path) => path,
            None => {
                log::info!("No files found, stopping pipeline loop");
                break;
            }
        };

        if !cache.insert_or_remove(path.clone()) {
            continue;
        }

        println!("Loading {path:?}");
        let ctx_clone = ctx.clone();
        let on_sample = move || ctx_clone.request_repaint();
        let pipeline = match Pipeline::new(path.clone(), on_sample) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                log::error!("Failed to set path for {}: {error}", path.display());
                continue;
            }
        };

        let result_rx = pipeline.set_state(gstreamer::State::Paused);
        if let Err(error) = result_rx.recv().unwrap() {
            log::error!("Failed to set state for {}: {error}", path.display());
            continue;
        }

        if pipeline_tx.send(pipeline).is_err() {
            log::info!("Pipeline tx disconnected, stopping pipeline loop");
            break;
        }
    }
}

fn start_file_feeder(
    ctx: &egui::Context,
    queue: Weak<Mutex<VecDeque<Pipeline>>>,
    root_paths: Arc<Mutex<Vec<PathBuf>>>,
) {
    log::info!("Starting file feeder");

    let (pipeline_tx, pipeline_rx) = flume::bounded(1);
    let ctx_clone = ctx.clone();
    let queue_clone = queue.clone();
    std::thread::spawn(move || pipeline_loop(ctx_clone, root_paths, pipeline_tx, queue_clone));

    let ctx_clone = ctx.clone();
    std::thread::spawn(move || pre_roll_loop(pipeline_rx, ctx_clone, queue));
}
