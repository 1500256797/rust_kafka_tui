use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::app::App;
use crate::ui::text::sanitize_display;
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let Some(notification) = &app.notification else {
        return;
    };

    if area.width < 12 || area.height < 3 {
        return;
    }

    let message = truncate_chars(&sanitize_display(&notification.message), area.width as usize);
    let icon = if notification.is_error { "×" } else { "✓" };
    let border_color = if notification.is_error {
        Color::Red
    } else {
        Color::Green
    };
    let text_style = if notification.is_error {
        theme::STATUS_ERR
    } else {
        theme::STATUS_OK
    };

    let width = area.width.saturating_sub(4).max(10);
    let height = 3u16;
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + 1;

    let toast_area = Rect {
        x,
        y,
        width,
        height,
    };

    if toast_area.x.saturating_add(toast_area.width) > area.x.saturating_add(area.width)
        || toast_area.y.saturating_add(toast_area.height) > area.y.saturating_add(area.height)
    {
        return;
    }

    frame.render_widget(Clear, toast_area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(" "),
            Span::styled(
                icon,
                Style::new()
                    .fg(border_color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(message, text_style),
        ]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::new().fg(border_color))
                .style(Style::new().bg(Color::Black)),
        ),
        toast_area,
    );
}

fn truncate_chars(text: &str, max_cols: usize) -> String {
    let usable = max_cols.saturating_sub(8);
    if text.chars().count() <= usable {
        text.to_string()
    } else {
        format!(
            "{}…",
            text.chars().take(usable.saturating_sub(1)).collect::<String>()
        )
    }
}
