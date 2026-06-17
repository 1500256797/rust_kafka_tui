use ratatui::Frame;
use ratatui::layout::Rect;

use crate::app::DialogChoice;
use crate::ui::components::confirm_dialog::{self, ConfirmDialogOptions};

pub fn render(frame: &mut Frame, area: Rect, preview: &str, choice: DialogChoice, readonly: bool) {
    confirm_dialog::render(
        frame,
        area,
        "Reply 预览",
        preview,
        choice,
        readonly,
        ConfirmDialogOptions {
            confirm_label: "✓ 重发",
            cancel_label: "✕ 取消",
        },
    );
}
