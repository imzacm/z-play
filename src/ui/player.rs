use eframe::egui;
use eframe::egui::Widget;

use crate::Error;
use crate::gstreamer_pipeline::{Event, Pipeline};

#[derive(Debug)]
pub struct Response {
    pub finished: bool,
    pub error: Option<Error>,
}

#[derive(Default)]
pub struct PlayerUi {
    pipeline: Option<Pipeline>,
    texture: Option<egui::TextureHandle>,
    rate: f64,
}

impl PlayerUi {
    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn set_rate(&mut self, rate: f64) -> Result<(), Error> {
        self.rate = rate;
        if let Some(pipeline) = &self.pipeline {
            let position = pipeline.position();
            pipeline.seek(position, Some(self.rate))?;
        }
        Ok(())
    }

    pub fn pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref()
    }

    pub fn swap_pipeline<P>(&mut self, pipeline: P) -> Option<Pipeline>
    where
        P: Into<Option<Pipeline>>,
    {
        self.texture = None;
        let pipeline = pipeline.into();
        if let Some(pipeline) = &pipeline {
            let position = pipeline.position();
            pipeline.seek(position, Some(self.rate)).expect("Failed to set playback rate");
        }
        std::mem::replace(&mut self.pipeline, pipeline)
    }

    pub fn clear(&mut self) {
        self.texture = None;
        self.pipeline = None;
    }

    pub fn ui(&mut self, ui: &mut egui::Ui) -> Response {
        let mut response = Response { finished: false, error: None };

        let Some(pipeline) = &self.pipeline else { return response };
        let path = pipeline.path();

        {
            let event_rx = pipeline.event_rx().clone();
            for event in event_rx.try_iter() {
                match event {
                    Event::EndOfStream => {
                        response.finished = true;
                    }
                    Event::Error(error) => {
                        response.error = Some(Error::Any(error));
                    }
                    Event::StateChanged { from, to } => {
                        // TODO: Play/pause text.
                        log::info!(
                            "Pipeline state changed {} - {from:?} -> {to:?}",
                            path.display()
                        );
                    }
                }
            }
        }

        ui.label(format!("Playing: {}", path.display()));

        let mut toggle_play_pause = ui.ctx().input(|i| i.key_released(egui::Key::Space));

        if let Some(frame) = pipeline.frame().take() {
            let image = egui::ColorImage::from_rgba_unmultiplied(
                [frame.width as usize, frame.height as usize],
                &frame.data,
            );
            let texture = ui.ctx().load_texture("video-frame", image, egui::TextureOptions::LINEAR);
            self.texture = Some(texture);
        }

        let duration = pipeline.duration();
        let position = pipeline.position();

        let elapsed_secs = position.seconds_f32();
        let duration_secs = duration.seconds_f32();

        let progress = if elapsed_secs > 0.0 && duration_secs > 0.0 {
            Some((elapsed_secs / duration_secs).clamp(0.0, 1.0))
        } else {
            None
        };

        ui.with_layout(egui::Layout::bottom_up(egui::Align::Min), |ui| {
            ui.horizontal(|ui| {
                let show_hours = duration.hours() != 0;

                ui.label(display_clocktime(position, show_hours));

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(display_clocktime(duration, show_hours));

                    if let Some(progress) = progress {
                        let progress_response = egui::ProgressBar::new(progress).ui(ui);

                        let interact = ui.interact(
                            progress_response.rect,
                            progress_response.id,
                            egui::Sense::click_and_drag(),
                        );

                        if (interact.hovered() || interact.dragged())
                            && let Some(hover_pos) = ui.ctx().input(|i| i.pointer.hover_pos())
                        {
                            let rect = progress_response.rect;

                            // Calculate the width of the overlay (from start to mouse X)
                            // Clamp it so it doesn't draw outside the bar
                            let overlay_width = (hover_pos.x - rect.min.x).clamp(0.0, rect.width());

                            let overlay_rect = egui::Rect::from_min_size(
                                rect.min,
                                egui::vec2(overlay_width, rect.height()),
                            );

                            // Draw a semi-transparent white layer on top
                            ui.painter().rect_filled(
                                overlay_rect,
                                egui::CornerRadius::from(overlay_rect.height() / 2.0),
                                egui::Color32::from_white_alpha(100),
                            );
                        }

                        if (interact.clicked() || interact.drag_stopped())
                            && let Some(hover_pos) = ui.ctx().input(|i| i.pointer.hover_pos())
                        {
                            let rect = progress_response.rect;
                            // value between 0.0 and 1.0
                            let relative_x =
                                ((hover_pos.x - rect.min.x) / rect.width()).clamp(0.0, 1.0);

                            let target_seconds = duration_secs * relative_x;
                            let target = gstreamer::ClockTime::from_seconds_f32(target_seconds);
                            if let Err(error) = pipeline.seek(target, Some(self.rate)) {
                                response.error = Some(error);
                            }
                        }
                    }
                });
            });

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
                    let response = ui.add(egui::Image::new(texture).sense(egui::Sense::click()));

                    if response.clicked() {
                        toggle_play_pause = true;
                    }
                } else {
                    ui.spinner();
                }
            });
        });

        // TODO: Play on video does nothing.
        // Seek and then play works.
        if toggle_play_pause {
            if pipeline.state() == gstreamer::State::Playing {
                if let Err(error) = pipeline.set_state(gstreamer::State::Paused) {
                    log::error!("Error pausing player: {error}");
                    response.error = Some(error);
                }
            } else if let Err(error) = pipeline.set_state(gstreamer::State::Playing) {
                log::error!("Error playing player: {error}");
                response.error = Some(error);
            }
        }

        ui.ctx().request_repaint();
        response
    }
}

fn display_clocktime(time: gstreamer::ClockTime, show_hours: bool) -> String {
    let hours = time.hours();
    let mut minutes = time.minutes();

    if show_hours {
        minutes %= 60;
    }

    let seconds = time.seconds() % 60;
    if show_hours {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}
