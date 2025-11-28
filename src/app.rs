use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use eframe::egui;
use glib::clone::Downgrade;
use parking_lot::Mutex;

use crate::gstreamer_pipeline::{Event, Pipeline};
use crate::media_type::MediaType;
use crate::random_files::RandomFiles;
use crate::{Error, ui};

const MAX_QUEUE_SIZE: usize = 20;
const MAX_PRE_ROLL_QUEUE_SIZE: usize = 10;

pub struct App {
    root_paths: Vec<PathBuf>,
    player: ui::PlayerUi,
    error: Option<Error>,
    queue: Arc<Mutex<VecDeque<Pipeline>>>,
    files: Arc<Mutex<RandomFiles>>,
}

impl App {
    pub fn new<I>(root_paths: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let root_paths = root_paths.into_iter().map(Into::into).collect();
        let files = RandomFiles::new(&root_paths);

        Self {
            root_paths,
            player: ui::PlayerUi::default(),
            error: None,
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_QUEUE_SIZE))),
            files: Arc::new(Mutex::new(files)),
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if Arc::weak_count(&self.queue) == 0 {
            let queue = self.queue.downgrade();
            let files = self.files.clone();
            start_file_feeder(ctx, queue, files);
        }

        let mut load_next_file = self.player.pipeline().is_none();

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Z-Play");

            let roots_response = ui::roots_ui(ui, &mut self.root_paths);
            if roots_response.changed {
                *self.files.lock() = RandomFiles::new(&self.root_paths);

                let mut queue_lock = self.queue.lock();
                queue_lock.retain(|pipeline| {
                    let path = pipeline.path();

                    // Retain where root exists.
                    self.root_paths.iter().any(|root| path.starts_with(root))
                });
            }

            ui::queue_ui(ui, &mut self.queue.lock());

            ui.horizontal(|ui| {
                if let Some(error) = &self.error {
                    ui.label(format!("Error: {error}"));
                }
            });

            ui.horizontal(|ui| {
                let next_button = ui.button("Next");
                if next_button.clicked() {
                    load_next_file = true;
                }
            });

            let player_response = self.player.ui(ui);
            if let Some(error) = player_response.error {
                self.error = Some(error);
                ui.ctx().request_repaint();
            }

            if player_response.finished {
                // TODO: Not if image.
                load_next_file = true;
            }
        });

        if load_next_file && let Some(pipeline) = self.queue.lock().pop_front() {
            if let Err(error) = pipeline.set_state(gstreamer::State::Playing) {
                self.error = Some(error);
                return;
            }
            self.player.clear();
            self.player.swap_pipeline(pipeline);
        }

        ctx.request_repaint();
    }
}

fn queue_loop(
    ctx: egui::Context,
    queue: Weak<Mutex<VecDeque<Pipeline>>>,
    pipeline_rx: flume::Receiver<Pipeline>,
) {
    loop {
        let Some(queue) = queue.upgrade() else {
            log::info!("Queue dropped, stopping queue loop");
            break;
        };

        let Ok(pipeline) = pipeline_rx.recv() else {
            log::info!("Pipeline rx disconnected, stopping queue loop");
            break;
        };

        while queue.lock().len() >= MAX_QUEUE_SIZE {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        queue.lock().push_back(pipeline);

        {
            let mut queue_lock = queue.lock();
            for pipeline in pipeline_rx.try_iter() {
                if queue_lock.len() >= MAX_QUEUE_SIZE {
                    break;
                }

                let path = pipeline.path();
                log::info!("Queueing {}", path.display());

                queue_lock.push_back(pipeline);
            }
        }

        ctx.request_repaint();
    }
}

fn pre_roll_loop(pipeline_rx: flume::Receiver<Pipeline>, pipeline_tx: flume::Sender<Pipeline>) {
    let mut queue = VecDeque::<Pipeline>::with_capacity(MAX_PRE_ROLL_QUEUE_SIZE);
    'main: loop {
        if pipeline_tx.is_disconnected() {
            log::info!("Pipeline tx disconnected, stopping pre-roll loop");
            break;
        }

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

                if is_paused {
                    if pipeline_tx.send(pipeline).is_err() {
                        log::info!("Pipeline tx disconnected, stopping pre-roll loop");
                        break 'main;
                    }
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
    files: Arc<Mutex<RandomFiles>>,
    pipeline_tx: flume::Sender<Pipeline>,
) {
    loop {
        if pipeline_tx.is_disconnected() {
            log::info!("Pipeline tx disconnected, stopping pipeline loop");
            break;
        }

        let Some(path) = files.lock().next() else {
            log::info!("No files found, stopping pre-roll loop");
            break;
        };

        match MediaType::detect(&path) {
            Ok(MediaType::Unknown) => {
                log::info!("Unknown media type for {path:?}");
                continue;
            }
            Ok(media_type) => {
                println!("Loading {path:?} as {media_type:?}");
                let pipeline = match Pipeline::new(path.clone(), media_type, ctx.clone()) {
                    Ok(pipeline) => pipeline,
                    Err(error) => {
                        log::error!("Failed to set path for {}: {error}", path.display());
                        continue;
                    }
                };

                if let Err(error) = pipeline.set_state(gstreamer::State::Paused) {
                    log::error!("Failed to set state for {}: {error}", path.display());
                    continue;
                }

                if pipeline_tx.send(pipeline).is_err() {
                    log::info!("Pipeline tx disconnected, stopping pipeline loop");
                    break;
                }
            }
            Err(error) => {
                log::warn!("Error detecting media type: {error}");
            }
        }
    }
}

fn start_file_feeder(
    ctx: &egui::Context,
    queue: Weak<Mutex<VecDeque<Pipeline>>>,
    files: Arc<Mutex<RandomFiles>>,
) {
    log::info!("Starting file feeder");

    let (initial_pipeline_tx, initial_pipeline_rx) = flume::bounded(MAX_PRE_ROLL_QUEUE_SIZE);
    let ctx_clone = ctx.clone();
    std::thread::spawn(move || pipeline_loop(ctx_clone, files, initial_pipeline_tx));

    let (pipeline_tx, pipeline_rx) = flume::bounded(MAX_PRE_ROLL_QUEUE_SIZE);

    std::thread::spawn(move || pre_roll_loop(initial_pipeline_rx, pipeline_tx));

    let ctx_clone = ctx.clone();
    std::thread::spawn(move || queue_loop(ctx_clone, queue, pipeline_rx));
}
