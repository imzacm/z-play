use std::path::PathBuf;
use std::time::{Duration, Instant};

use eframe::egui;

use crate::media_type::{MediaFile, MediaType};
use crate::random_files::RandomFiles;

enum FileState {
    NotStarted(RandomFiles),
    Started(flume::Receiver<Result<MediaFile, String>>),
    Ended,
}

impl FileState {
    fn file_rx(&self) -> Option<&flume::Receiver<Result<MediaFile, String>>> {
        match self {
            FileState::NotStarted(_) => None,
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
    Image { file: MediaFile, next_at: Instant },
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
            Self::Image { next_at, .. } => *next_at <= Instant::now(),
        }
    }
}

pub struct App {
    audio_device: egui_video::AudioDevice,
    player_state: PlayerState,
    file_state: FileState,
}

impl App {
    pub fn new<I>(files: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let audio_device = egui_video::AudioDevice::new().unwrap();
        Self {
            audio_device,
            player_state: PlayerState::None,
            file_state: FileState::NotStarted(RandomFiles::new(files)),
        }
    }

    fn next_file(&mut self, ctx: &egui::Context) {
        self.file_state = match std::mem::replace(&mut self.file_state, FileState::Ended) {
            FileState::NotStarted(files) => {
                let (file_tx, file_rx) = flume::bounded(2);
                let ctx = ctx.clone();
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
                // TODO: Maintain aspect ratio.
                player.start();
                self.player_state = PlayerState::Video { file, player };
            }
            MediaType::Image => {
                let next_at = Instant::now() + Duration::from_secs(10);
                self.player_state = PlayerState::Image { file, next_at };
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
            ui.horizontal(|ui| {
                if let PlayerState::Error(error) = &self.player_state {
                    ui.label(format!("Error: {error}"));
                }
            });

            let next_button = ui.button("Next");

            // TODO: Seeking starts next file.
            if next_button.clicked() || self.player_state.is_ended() {
                self.next_file(ctx);
            }

            if let Some(file) = self.player_state.file() {
                ui.label(format!("Playing: {}", file.path.display()));
            }

            match &mut self.player_state {
                PlayerState::None | PlayerState::Error(_) => (),
                PlayerState::Video { player, .. } => {
                    player.size = ui.available_size();
                    player.ui(ui, player.size);
                }
                PlayerState::Audio { player, .. } => {
                    player.ui(ui);
                }
                PlayerState::Image { file, .. } => {
                    let uri = format!("file://{}", file.path.display());
                    ui.image(uri);
                }
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
