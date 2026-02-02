#[derive(Default, Clone)]
pub struct Frame {
    pub width: u32,
    pub height: u32,
    /// RGBA
    pub data: Vec<u8>,
}

#[derive(Default, Clone)]
pub struct State {
    pub frame: Frame,
    pub audio_buffer: Vec<f32>,
    pub duration: gstreamer::ClockTime,
    pub position: gstreamer::ClockTime,
    pub is_image: bool,
}
