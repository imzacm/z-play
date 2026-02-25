#![deny(unused_imports, clippy::all)]

#[cfg(feature = "http")]
mod http;

use std::path::PathBuf;

#[cfg(feature = "app")]
use eframe::egui;
#[cfg(feature = "app")]
use z_play::app::App;

#[allow(unreachable_code)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("RUST_LOG").is_none() {
        unsafe { std::env::set_var("RUST_LOG", "z_play=info") };
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

    #[cfg(feature = "http")]
    if let Some(port) = http_port {
        http::start_server(port, root_dirs);
        return Ok(());
    }

    #[cfg(feature = "app")]
    {
        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default().with_inner_size([320.0, 240.0]),
            ..Default::default()
        };
        return eframe::run_native(
            "Z-Play",
            options,
            Box::new(|_| Ok(Box::new(App::new(root_dirs)))),
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>);
    }

    panic!("No feature selected");
}
