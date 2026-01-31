use std::collections::VecDeque;

use eframe::egui;

use crate::pipeline::Pipeline;

pub fn queue_ui(ui: &mut egui::Ui, queue: &mut VecDeque<Pipeline>) {
    let len = queue.len();
    let title = format!("Queue ({len})");
    ui.collapsing(title, |ui| {
        let clear_button = ui.button("Clear");
        if clear_button.clicked() {
            queue.clear();
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            let mut remove_index = None;
            for (index, pipeline) in queue.iter().enumerate() {
                let path = pipeline.path();
                ui.horizontal(|ui| {
                    ui.label(path.to_string_lossy());

                    // TODO: On label hover, darken
                    // TODO: On label click, play

                    let remove_button = ui.button("-");
                    if remove_button.clicked() {
                        remove_index = Some(index);
                    }
                });
            }

            if let Some(index) = remove_index {
                queue.remove(index);
            }
        });
    });

    if queue.len() != len {
        ui.ctx().request_repaint();
    }
}
