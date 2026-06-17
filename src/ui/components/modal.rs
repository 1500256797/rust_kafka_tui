use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::app::DialogChoice;
use crate::ui::components::confirm_dialog::{self, ConfirmDialogOptions};
use crate::ui::theme;

pub fn render_input_modal(frame: &mut Frame, area: Rect, title: &str, input: &str) {
    let popup = centered_rect(60, 20, area);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::MODAL_BORDER)
            .title(title),
        popup,
    );

    let inner = popup.inner(ratatui::layout::Margin::new(2, 1));
    frame.render_widget(Paragraph::new(input), inner);
}

pub fn render_confirm_modal(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    message: &str,
    choice: DialogChoice,
) {
    confirm_dialog::render(
        frame,
        area,
        title,
        message,
        choice,
        false,
        ConfirmDialogOptions {
            confirm_label: "✓ 发送",
            cancel_label: "✕ 取消",
        },
    );
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    use ratatui::layout::{Constraint, Flex, Layout};
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)])
        .flex(Flex::Center)
        .split(r);
    Layout::horizontal([Constraint::Percentage(percent_x)])
        .flex(Flex::Center)
        .split(vertical[0])[0]
}
