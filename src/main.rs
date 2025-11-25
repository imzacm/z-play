#![deny(unused_imports, clippy::all)]

use std::path::PathBuf;

use eframe::egui;

mod app;
mod gstreamer_pipeline;
mod media_type;
mod random_files;
mod ui;

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

fn main() -> eframe::Result {
    gstreamer::init().expect("Failed to initialize GStreamer");

    let args = std::env::args_os().skip(1);
    let root_dirs = args.map(PathBuf::from).collect::<Vec<_>>();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
        ..Default::default()
    };
    eframe::run_native("Z-Play", options, Box::new(|_| Ok(Box::new(app::App::new(root_dirs)))))
}
