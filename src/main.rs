#![deny(unused_imports, clippy::all)]

mod http;

use std::path::PathBuf;

use eframe::egui;
use z_play::app::App;

fn main() -> eframe::Result {
    if std::env::var_os("RUST_LOG").is_none() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    env_logger::init();

    let mut args = std::env::args_os().skip(1).peekable();
    let mut http_port = None;

    if let Some(arg) = args.peek()
        && let Some(arg) = arg.to_str()
        && let Some(("--http", port)) = arg.split_once('=')
    {
        let port = port.parse::<u16>().expect("Invalid port number");
        http_port = Some(port);

        args.next();
    }

    let root_dirs = args.map(PathBuf::from).collect::<Vec<_>>();

    if let Some(port) = http_port {
        http::start_server(port, root_dirs);
        return Ok(());
    }

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
        ..Default::default()
    };
    eframe::run_native("Z-Play", options, Box::new(|_| Ok(Box::new(App::new(root_dirs)))))
}
