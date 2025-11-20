use std::path::PathBuf;
use std::time::{Duration, Instant};

use eframe::egui;
use eframe::egui::Widget;

use crate::media_type::{MediaFile, MediaType};
use crate::random_files::RandomFiles;

enum FileState {
    NotStarted,
    Started(flume::Receiver<Result<MediaFile, String>>),
    Ended,
}

impl FileState {
    fn file_rx(&self) -> Option<&flume::Receiver<Result<MediaFile, String>>> {
        match self {
            FileState::NotStarted => None,
            FileState::Started(file_rx) => Some(file_rx),
            FileState::Ended => None,
        }
    }
}

enum PlayerState {
    None,
    Error(String),
    Video { file: MediaFile, player: egui_video::Player },
    Audio { file: MediaFile, player: egui_player::player::Player },
    Image { file: MediaFile, started: Option<Instant>, duration: Duration },
}

impl PlayerState {
    fn file(&self) -> Option<&MediaFile> {
        match self {
            Self::None | Self::Error(_) => None,
            Self::Video { file, .. } | Self::Audio { file, .. } | Self::Image { file, .. } => {
                Some(file)
            }
        }
    }

    fn is_ended(&self) -> bool {
        match self {
            Self::None | Self::Error(_) => true,
            Self::Video { player, .. } => {
                player.player_state.get() == egui_video::PlayerState::EndOfFile
            }
            Self::Audio { player, .. } => {
                player.player_state == egui_player::player::PlayerState::Ended
            }
            Self::Image { started, duration, .. } => {
                started.is_some_and(|s| s.elapsed() >= *duration)
            }
        }
    }

    fn progress(&self) -> Option<f32> {
        let (elapsed, duration) = match self {
            Self::None | Self::Error(_) => return None,
            Self::Video { player, .. } => (player.elapsed_ms() as f32, player.duration_ms as f32),
            Self::Audio { player, .. } => {
                (player.elapsed_time.as_secs_f32(), player.total_time.as_secs_f32())
            }
            Self::Image { started, duration, .. } => {
                let started = started.as_ref()?;
                (started.elapsed().as_secs_f32(), duration.as_secs_f32())
            }
        };
        if elapsed > 0.0 && duration > 0.0 {
            Some((elapsed / duration).clamp(0.0, 1.0))
        } else {
            None
        }
    }
}

pub struct App {
    root_paths: Vec<PathBuf>,
    audio_device: egui_video::AudioDevice,
    player_state: PlayerState,
    file_state: FileState,
}

impl App {
    pub fn new<I>(root_paths: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let audio_device = egui_video::AudioDevice::new().unwrap();
        Self {
            root_paths: root_paths.into_iter().map(Into::into).collect(),
            audio_device,
            player_state: PlayerState::None,
            file_state: FileState::NotStarted,
        }
    }

    fn next_file(&mut self, ctx: &egui::Context) {
        if !matches!(self.player_state, PlayerState::Error(_)) {
            self.player_state = PlayerState::None;
        }

        self.file_state = match std::mem::replace(&mut self.file_state, FileState::Ended) {
            FileState::NotStarted => {
                let (file_tx, file_rx) = flume::bounded(2);
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
                self.player_state = PlayerState::Error(error);
                return;
            }
            Err(_) => return,
        };

        let path = &file.path;
        match file.media_type {
            MediaType::Video => {
                let path = path.to_string_lossy().into_owned();
                let player = match egui_video::Player::new(ctx, &path) {
                    Ok(player) => player,
                    Err(error) => {
                        eprintln!("Error creating player: {error}");
                        return;
                    }
                };
                let mut player = match player.with_audio(&mut self.audio_device) {
                    Ok(player) => player,
                    Err(error) => {
                        eprintln!("Error creating player with audio: {error}");
                        return;
                    }
                };
                player.options.looping = false;
                player.start();
                self.player_state = PlayerState::Video { file, player };
            }
            MediaType::Image => {
                let duration = Duration::from_secs(10);
                self.player_state = PlayerState::Image { file, started: None, duration };
            }
            MediaType::Audio => {
                let path = path.to_string_lossy();
                let player = egui_player::player::Player::from_path(path.as_ref());
                self.player_state = PlayerState::Audio { file, player };
            }
            MediaType::Unknown => unreachable!(),
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Z-Play");
            ui.collapsing("Roots", |ui| {
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
                        for path in add_paths {
                            if !self.root_paths.contains(&path) {
                                self.root_paths.push(path);
                            }
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
                        self.root_paths.remove(index);
                    }
                });
            });
            ui.horizontal(|ui| {
                if let PlayerState::Error(error) = &self.player_state {
                    ui.label(format!("Error: {error}"));
                }
            });

            let next_button = ui.button("Next");

            if next_button.clicked() || self.player_state.is_ended() {
                self.next_file(ui.ctx());
            }

            if let Some(file) = self.player_state.file() {
                ui.label(format!("Playing: {}", file.path.display()));
            }

            let mut toggle_play_pause = false;
            ui.ctx().input(|i| {
                toggle_play_pause = i.key_released(egui::Key::Space);
            });

            ui.centered_and_justified(|ui| match &mut self.player_state {
                PlayerState::None | PlayerState::Error(_) => {
                    ui.spinner();
                }
                PlayerState::Video { player, .. } => {
                    if toggle_play_pause {
                        if player.player_state.get() == egui_video::PlayerState::Paused {
                            player.resume();
                        } else {
                            player.pause();
                        }
                    }

                    let available_size = ui.available_size();
                    let video_size = player.size;
                    if video_size.x > 0.0 && video_size.y > 0.0 && video_size != available_size {
                        let aspect_ratio = video_size.x / video_size.y;
                        let mut width = available_size.x;
                        let mut height = available_size.x / aspect_ratio;

                        if height > available_size.y {
                            height = available_size.y;
                            width = height * aspect_ratio;
                        }

                        player.size = egui::Vec2::new(width, height);
                    }
                    player.ui(ui, player.size);
                }
                PlayerState::Audio { player, .. } => {
                    // TODO: Expose audio control functions in egui_player.
                    player.ui(ui);
                }
                PlayerState::Image { file, started, .. } => {
                    let uri = format!("file://{}", file.path.display());
                    let image = egui::Image::new(uri).maintain_aspect_ratio(true);

                    // TODO: Figure out a way to pause image timer.

                    if started.is_none() {
                        match image.load_for_size(ui.ctx(), ui.available_size()) {
                            Ok(egui::load::TexturePoll::Ready { .. }) => {
                                *started = Some(Instant::now());
                            }
                            Ok(egui::load::TexturePoll::Pending { .. }) => (),
                            Err(error) => {
                                self.player_state = PlayerState::Error(error.to_string());
                                ctx.request_repaint();
                            }
                        }
                    }

                    image.ui(ui);
                }
            });

            if let Some(progress) = self.player_state.progress() {
                egui::ProgressBar::new(progress).ui(ui);
                ctx.request_repaint_after(Duration::from_millis(10));
            }
        });
    }
}

fn file_feeder(
    ctx: egui::Context,
    file_tx: flume::Sender<Result<MediaFile, String>>,
    mut files: RandomFiles,
) {
    loop {
        if file_tx.is_disconnected() {
            break;
        }

        let Some(path) = files.next() else {
            _ = file_tx.send(Err("No files found".to_string()));
            ctx.request_repaint();
            break;
        };

        match MediaType::detect(&path) {
            Ok(MediaType::Unknown) => {
                eprintln!("Unknown media type for {path:?}");
            }
            Ok(media_type) => {
                _ = file_tx.send(Ok(MediaFile { path, media_type }));
                ctx.request_repaint();
            }
            Err(error) => eprintln!("Error detecting media type: {error}"),
        }
    }
}
