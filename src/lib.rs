#![deny(unused_imports, clippy::all)]

#[cfg(feature = "app")]
pub mod app;
pub mod path_cache;
#[cfg(feature = "app")]
pub mod pipeline;
#[cfg(feature = "app")]
pub mod playback_speed;
pub mod random_files;
#[cfg(feature = "app")]
pub mod ui;

#[cfg(feature = "app")]
pub use app::Error;
