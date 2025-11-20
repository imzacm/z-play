#![deny(unused_imports, clippy::all)]

use std::path::PathBuf;

use eframe::egui;

mod app;
mod media_type;
mod random_files;

fn main() -> eframe::Result {
    let args = std::env::args_os().skip(1);
    let root_dirs = args.map(PathBuf::from).collect::<Vec<_>>();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Z-Play",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);

            Ok(Box::new(app::App::new(root_dirs)))
        }),
    )
}
