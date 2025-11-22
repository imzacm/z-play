use std::path::PathBuf;

use eframe::egui;
use eframe::egui::Widget;

use crate::Error;
use crate::media_type::{MediaFile, MediaType};
use crate::player::{Event, Player};
use crate::random_files::RandomFiles;

const FILE_BUFFER_SIZE: usize = 10;

enum FileState {
    NotStarted,
    Started(flume::Receiver<Result<MediaFile, Error>>),
    Ended,
}

impl FileState {
    fn file_rx(&self) -> Option<&flume::Receiver<Result<MediaFile, Error>>> {
        match self {
            FileState::NotStarted => None,
            FileState::Started(file_rx) => Some(file_rx),
            FileState::Ended => None,
        }
    }
}

pub struct App {
    root_paths: Vec<PathBuf>,
    file_state: FileState,
    player: Player,
    error: Option<Error>,
    current_file: Option<MediaFile>,
    texture: Option<egui::TextureHandle>,

    duration: gstreamer::ClockTime,
    position: gstreamer::ClockTime,
}

impl App {
    pub fn new<I>(root_paths: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let player = Player::new().expect("Failed to create player");
        Self {
            root_paths: root_paths.into_iter().map(Into::into).collect(),
            file_state: FileState::NotStarted,
            player,
            error: None,
            current_file: None,
            texture: None,

            duration: gstreamer::ClockTime::ZERO,
            position: gstreamer::ClockTime::ZERO,
        }
    }

    fn next_file(&mut self, ctx: &egui::Context) {
        self.file_state = match std::mem::replace(&mut self.file_state, FileState::Ended) {
            FileState::NotStarted => {
                let (file_tx, file_rx) = flume::bounded(FILE_BUFFER_SIZE);
                let ctx = ctx.clone();
                let files = RandomFiles::new(&self.root_paths);
                std::thread::spawn(move || file_feeder(ctx, file_tx, files));
                FileState::Started(file_rx)
            }
            FileState::Started(file_rx) => FileState::Started(file_rx),
            FileState::Ended => return,
        };

        let Some(file_rx) = self.file_state.file_rx() else { return };

        let file = match file_rx.try_recv() {
            Ok(Ok(file)) => file,
            Ok(Err(error)) => {
                self.error = Some(error);
                return;
            }
            Err(flume::TryRecvError::Disconnected) => {
                eprintln!("File channel disconnected");
                return;
            }
            Err(flume::TryRecvError::Empty) => return,
        };

        if let Err(error) = self.player.set_path(&file.path) {
            self.error = Some(error);
            return;
        }

        if let Err(error) = self.player.play() {
            self.error = Some(error);
            return;
        }

        self.error = None;
        self.current_file = Some(file);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let event_rx = self.player.event_rx().clone();
        for event in event_rx.try_iter() {
            match event {
                Event::EofOfStream => {
                    self.next_file(ctx);
                }
                Event::Error(error) => {
                    self.error = Some(Error::Any(error));
                }
                Event::StateChanged(state) => {
                    // TODO: Play/pause text.
                }
            }
            ctx.request_repaint();
        }

        self.duration = self.player.duration().unwrap_or(gstreamer::ClockTime::ZERO);
        self.position = self.player.position().unwrap_or(gstreamer::ClockTime::ZERO);

        let frame = {
            let mut state_lock = self.player.state().lock();
            let frame = state_lock.frame.take();
            state_lock.egui_context = Some(ctx.clone());
            frame
        };

        if let Some(frame) = frame {
            let image = egui::ColorImage::from_rgba_unmultiplied(
                [frame.width as usize, frame.height as usize],
                &frame.data,
            );
            let texture = ctx.load_texture("video-frame", image, egui::TextureOptions::LINEAR);
            self.texture = Some(texture);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Z-Play");
            ui.collapsing("Roots", |ui| {
                let mut roots_changed = false;

                ui.horizontal(|ui| {
                    let mut add_paths = None;

                    let add_file_button = ui.button("Add files");
                    if add_file_button.clicked() {
                        add_paths = rfd::FileDialog::new().pick_files();
                    }

                    let add_folder_button = ui.button("Add folders");
                    if add_folder_button.clicked() {
                        add_paths = rfd::FileDialog::new().pick_folders();
                    }

                    if let Some(add_paths) = add_paths {
                        self.root_paths.reserve(add_paths.len());
                        let old_len = self.root_paths.len();
                        for path in add_paths {
                            if !self.root_paths.contains(&path) {
                                self.root_paths.push(path);
                            }
                        }
                        if self.root_paths.len() != old_len {
                            roots_changed = true;
                        }
                    }
                });

                egui::ScrollArea::vertical().show(ui, |ui| {
                    let mut remove_index = None;
                    for (index, path) in self.root_paths.iter().enumerate() {
                        let path = path.to_string_lossy();
                        ui.horizontal(|ui| {
                            ui.label(path);
                            let remove_button = ui.button("-");
                            if remove_button.clicked() {
                                remove_index = Some(index);
                            }
                        });
                    }

                    if let Some(index) = remove_index {
                        roots_changed = true;
                        self.root_paths.remove(index);
                    }
                });

                if roots_changed {
                    let mut ready = Vec::new();
                    if let FileState::Started(file_rx) = &self.file_state {
                        ready.reserve(file_rx.len());
                        for file in file_rx.try_iter().flatten() {
                            ready.push(file);
                        }
                    }

                    assert!(ready.len() <= FILE_BUFFER_SIZE);

                    let (file_tx, file_rx) = flume::bounded(FILE_BUFFER_SIZE);
                    for file in ready {
                        file_tx.send(Ok(file)).unwrap();
                    }

                    let ctx = ui.ctx().clone();
                    let files = RandomFiles::new(&self.root_paths);
                    std::thread::spawn(move || file_feeder(ctx, file_tx, files));
                    self.file_state = FileState::Started(file_rx);
                }
            });

            ui.horizontal(|ui| {
                if let Some(error) = &self.error {
                    ui.label(format!("Error: {error}"));
                }
            });

            ui.horizontal(|ui| {
                let next_button = ui.button("Next");
                if next_button.clicked() || self.current_file.is_none() {
                    self.next_file(ui.ctx());
                }

                if let FileState::Started(file_rx) = &self.file_state {
                    let len = file_rx.len();
                    ui.label(format!("File queue length: {len}"));

                    let clear_button = ui.button("Clear queue");
                    if clear_button.clicked() {
                        for _ in file_rx.try_iter() {}
                        ui.ctx().request_repaint();
                    }
                }
            });

            if let Some(file) = &self.current_file {
                ui.label(format!("Playing: {}", file.path.display()));
            }

            let mut toggle_play_pause = false;
            ui.ctx().input(|i| {
                toggle_play_pause = i.key_released(egui::Key::Space);
            });

            if toggle_play_pause && self.current_file.is_some() {
                if self.player.is_playing() {
                    if let Err(error) = self.player.pause() {
                        self.error = Some(error);
                    }
                } else if let Err(error) = self.player.play() {
                    self.error = Some(error);
                }
                ctx.request_repaint();
            }

            ui.centered_and_justified(|ui| {
                if let Some(texture) = &self.texture {
                    let video_size = texture.size();
                    let video_size = egui::Vec2::new(video_size[0] as f32, video_size[1] as f32);
                    let available_size = ui.available_size();

                    let width_ratio = available_size.x / video_size.x;
                    let height_ratio = available_size.y / video_size.y;

                    let scale = width_ratio.min(height_ratio);

                    let target_size = video_size * scale;

                    let texture = egui::load::SizedTexture::new(texture.id(), target_size);
                    ui.add(egui::Image::new(texture));
                } else if let Some(file) = &self.current_file
                    && let MediaType::Image = file.media_type
                {
                    let uri = format!("file://{}", file.path.display());
                    let image = egui::Image::new(uri).maintain_aspect_ratio(true);

                    match image.load_for_size(ui.ctx(), ui.available_size()) {
                        Ok(egui::load::TexturePoll::Ready { .. }) => (),
                        Ok(egui::load::TexturePoll::Pending { .. }) => {
                            ui.spinner();
                        }
                        Err(error) => {
                            self.error = Some(error.into());
                            ctx.request_repaint();
                        }
                    }

                    image.ui(ui);
                } else {
                    ui.spinner();
                }
            });

            let elapsed_secs = self.position.seconds_f32();
            let duration_secs = self.duration.seconds_f32();

            let progress = if elapsed_secs > 0.0 && duration_secs > 0.0 {
                Some((elapsed_secs / duration_secs).clamp(0.0, 1.0))
            } else {
                None
            };

            println!("Position: {:.2}s, Elapsed: {elapsed_secs:.2}s, Duration: {duration_secs:.2}s, Progress: {:.2}", self.position.seconds_f32(), progress.unwrap_or(0.0));

            if let Some(progress) = progress {
                egui::ProgressBar::new(progress).ui(ui);
            }
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        _ = self.player.stop();
        self.player.main_loop().quit();
    }
}

fn file_feeder(
    ctx: egui::Context,
    file_tx: flume::Sender<Result<MediaFile, Error>>,
    mut files: RandomFiles,
) {
    eprintln!("Starting file feeder");
    loop {
        if file_tx.is_disconnected() {
            eprintln!("File channel disconnected, stopping feeder");
            break;
        }

        let Some(path) = files.next() else {
            eprintln!("No files found, stopping feeder");
            _ = file_tx.send(Err(Error::NoFilesFound));
            ctx.request_repaint();
            break;
        };

        match MediaType::detect(&path) {
            Ok(MediaType::Unknown) => {
                eprintln!("Unknown media type for {path:?}");
            }
            Ok(media_type) => {
                eprintln!("[file_feeder] Sending file: {} - {media_type:?}", path.display());
                _ = file_tx.send(Ok(MediaFile { path, media_type }));
                ctx.request_repaint();
            }
            Err(error) => eprintln!("Error detecting media type: {error}"),
        }
    }
}
