#![deny(unused_imports, clippy::all)]

use std::path::PathBuf;

use eframe::egui;
use z_play_rs::app::App;

fn main() -> eframe::Result {
    if std::env::var_os("RUST_LOG").is_none() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    env_logger::init();

    let args = std::env::args_os().skip(1);
    let root_dirs = args.map(PathBuf::from).collect::<Vec<_>>();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
        ..Default::default()
    };
    eframe::run_native("Z-Play", options, Box::new(|_| Ok(Box::new(App::new(root_dirs)))))
}
