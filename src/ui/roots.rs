use std::path::PathBuf;

use eframe::egui;

#[derive(Debug, Clone)]
pub struct Response {
    pub changed: bool,
}

pub fn roots_ui(ui: &mut egui::Ui, paths: &mut Vec<PathBuf>) -> Response {
    let mut response = Response { changed: false };

    ui.collapsing("Roots", |ui| {
        ui.horizontal(|ui| {
            let mut add_paths = None;

            let add_file_button = ui.button("Add files");
            if add_file_button.clicked() {
                add_paths = rfd::FileDialog::new().pick_files();
            }

            let add_folder_button = ui.button("Add folders");
            if add_folder_button.clicked() {
                add_paths = rfd::FileDialog::new().pick_folders();
            }

            if let Some(add_paths) = add_paths {
                paths.reserve(add_paths.len());
                let old_len = paths.len();
                for path in add_paths {
                    if !paths.contains(&path) {
                        paths.push(path);
                    }
                }
                if paths.len() != old_len {
                    response.changed = true;
                }
            }
        });

        egui::ScrollArea::vertical().show(ui, |ui| {
            let mut remove_index = None;
            for (index, path) in paths.iter().enumerate() {
                let path = path.to_string_lossy();
                ui.horizontal(|ui| {
                    ui.label(path);
                    let remove_button = ui.button("-");
                    if remove_button.clicked() {
                        remove_index = Some(index);
                    }
                });
            }

            if let Some(index) = remove_index {
                response.changed = true;
                paths.remove(index);
            }
        });
    });

    if response.changed {
        ui.ctx().request_repaint();
    }

    response
}
