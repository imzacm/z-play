#![deny(unused_imports, clippy::all)]

use eframe::egui;

pub mod app;
pub mod gstreamer_pipeline;
pub mod path_cache;
pub mod playback_speed;
pub mod random_files;
pub mod ui;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    EguiLoad(#[from] egui::load::LoadError),

    #[error("{0}")]
    Any(String),

    #[error(transparent)]
    Glib(#[from] glib::Error),
    #[error(transparent)]
    GlibBool(#[from] glib::BoolError),
    #[error(transparent)]
    StateChange(#[from] gstreamer::StateChangeError),
}
