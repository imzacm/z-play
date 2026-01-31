#[derive(Debug)]
pub enum Event {
    EndOfStream,
    Error(String),
    StateChanged { from: gstreamer::State, to: gstreamer::State },
}
