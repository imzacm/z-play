use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eframe::egui;
use parking_lot::Mutex;

use crate::media_type::{MediaFile, MediaType};
use crate::random_files::RandomFiles;

pub struct App {
    files: Arc<Mutex<RandomFiles>>,
    audio_device: egui_video::AudioDevice,
    video_player: Option<egui_video::Player>,
    audio_player: Option<egui_player::player::Player>,
    error_message: Option<String>,
    current: Option<MediaFile>,
    next_at: Option<Instant>,

    next_file: Option<Arc<Mutex<Option<Result<MediaFile, String>>>>>,
}

impl App {
    pub fn new<I>(files: I) -> Self
    where
        I: IntoIterator<Item: Into<PathBuf>>,
    {
        let audio_device = egui_video::AudioDevice::new().unwrap();
        Self {
            files: Arc::new(Mutex::new(RandomFiles::new(files))),
            audio_device,
            video_player: None,
            audio_player: None,
            error_message: None,
            current: None,
            next_at: None,

            next_file: None,
        }
    }

    fn start_next_file(&mut self, ctx: &egui::Context) {
        let ctx = ctx.clone();
        let files = self.files.clone();
        let arc_mutex = Arc::new(Mutex::new(None));
        self.next_file = Some(arc_mutex.clone());

        rayon::spawn(move || {
            loop {
                let Some(path) = files.lock().next() else {
                    *arc_mutex.lock() = Some(Err("No files found".to_string()));
                    break;
                };

                match MediaType::detect(&path) {
                    Ok(MediaType::Unknown) => {
                        eprintln!("Unknown media type for {path:?}");
                    }
                    Ok(media_type) => {
                        *arc_mutex.lock() = Some(Ok(MediaFile { path, media_type }));
                        break;
                    }
                    Err(error) => eprintln!("Error detecting media type: {error}"),
                }
            }

            debug_assert!(arc_mutex.lock().is_some());
            ctx.request_repaint();
        });
    }

    fn next_file(&mut self, ctx: &egui::Context) {
        let Some(arc_mutex) = self.next_file.take() else {
            self.start_next_file(ctx);
            return;
        };

        let mutex = match Arc::try_unwrap(arc_mutex) {
            Ok(mutex) => mutex,
            Err(arc_mutex) => {
                self.next_file = Some(arc_mutex);
                return;
            }
        };

        // The task always sets to Some, so unwrap is safe here.
        let result = mutex.into_inner().unwrap();
        let file = match result {
            Ok(file) => file,
            Err(error) => {
                self.error_message = Some(error);
                return;
            }
        };

        self.error_message = None;
        self.video_player = None;
        self.audio_player = None;
        self.current = None;
        self.next_at = None;

        let path = &file.path;
        match file.media_type {
            MediaType::Video => {
                let path = path.to_string_lossy().into_owned();
                let player = match egui_video::Player::new(ctx, &path) {
                    Ok(player) => player,
                    Err(error) => {
                        eprintln!("Error creating player: {error}");
                        self.start_next_file(ctx);
                        return;
                    }
                };
                let mut player = match player.with_audio(&mut self.audio_device) {
                    Ok(player) => player,
                    Err(error) => {
                        eprintln!("Error creating player with audio: {error}");
                        self.start_next_file(ctx);
                        return;
                    }
                };
                player.options.looping = false;
                // TODO: Maintain aspect ratio.
                player.start();
                self.video_player = Some(player);
            }
            MediaType::Image => {
                self.next_at = Some(Instant::now() + Duration::from_secs(10));
            }
            MediaType::Audio => {
                let path = path.to_string_lossy();
                let player = egui_player::player::Player::from_path(path.as_ref());
                self.audio_player = Some(player);
            }
            MediaType::Unknown => unreachable!(),
        }

        self.current = Some(file);
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Z-Play");
            ui.horizontal(|ui| {
                if let Some(error) = &self.error_message {
                    ui.label(format!("Error: {error}"));
                }
            });

            let next_button = ui.button("Next");

            // TODO: Seeking starts next file.
            if next_button.clicked()
                // If already loading, poll it.
                || self.next_file.is_some()
                || self.next_at.is_some_and(|i| i <= Instant::now())
                || self.video_player.as_ref().is_some_and(|p| p.player_state.get() == egui_video::PlayerState::EndOfFile)
                || self.audio_player.as_ref().is_some_and(|p| p.player_state == egui_player::player::PlayerState::Ended)
            {
                self.next_file(ctx);
            }

            if let Some(file) = &self.current {
                ui.label(format!("Playing: {}", file.path.display()));

                if let Some(player) = &mut self.video_player {
                    player.size = ui.available_size();
                    player.ui(ui, player.size);
                }

                if let Some(player) = &mut self.audio_player {
                    player.ui(ui);
                }

                if let MediaType::Image = file.media_type {
                    let uri = format!("file://{}", file.path.display());
                    ui.image(uri);
                }
            }
        });
    }
}
