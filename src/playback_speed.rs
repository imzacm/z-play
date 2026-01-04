#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum PlaybackSpeed {
    X0_5,
    #[default]
    X1,
    X2,
    X4,
    X8,
    X16,
    X32,
}

impl PlaybackSpeed {
    pub fn all() -> impl IntoIterator<Item = Self> {
        [Self::X0_5, Self::X1, Self::X2, Self::X4, Self::X8, Self::X16, Self::X32]
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            PlaybackSpeed::X0_5 => "x0.5",
            PlaybackSpeed::X1 => "x1",
            PlaybackSpeed::X2 => "x2",
            PlaybackSpeed::X4 => "x4",
            PlaybackSpeed::X8 => "x8",
            PlaybackSpeed::X16 => "x16",
            PlaybackSpeed::X32 => "x32",
        }
    }

    pub const fn rate(&self) -> f64 {
        match self {
            PlaybackSpeed::X0_5 => 0.5,
            PlaybackSpeed::X1 => 1.0,
            PlaybackSpeed::X2 => 2.0,
            PlaybackSpeed::X4 => 4.0,
            PlaybackSpeed::X8 => 8.0,
            PlaybackSpeed::X16 => 16.0,
            PlaybackSpeed::X32 => 32.0,
        }
    }
}
