use eframe::egui;

#[derive(Default, Clone)]
pub struct State {
    pub frames: [egui::ColorImage; 2],
    pub active_frame: usize,
    pub duration: gstreamer::ClockTime,
    pub position: gstreamer::ClockTime,
    pub is_image: bool,
}

impl State {
    pub fn active_frame_mut(&mut self) -> &mut egui::ColorImage {
        &mut self.frames[self.active_frame]
    }

    pub fn inactive_frame_mut(&mut self) -> &mut egui::ColorImage {
        &mut self.frames[1 - self.active_frame]
    }

    pub fn swap_frames(&mut self) {
        if self.active_frame == self.frames.len() - 1 {
            self.active_frame = 0;
        } else {
            self.active_frame += 1;
        }
    }
}
