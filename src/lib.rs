#![deny(unused_imports, clippy::all)]

#[cfg(feature = "app")]
pub mod app;
pub mod path_cache;
#[cfg(feature = "app")]
pub mod pipeline;
#[cfg(feature = "app")]
pub mod playback_speed;
pub mod random_files;
#[cfg(feature = "immich")]
pub mod random_files_immich;
#[cfg(feature = "app")]
pub mod ui;
pub mod walkdir;

#[cfg(feature = "app")]
pub use app::Error;
