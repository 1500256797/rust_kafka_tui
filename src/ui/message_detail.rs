use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::service::MessageBrowserState;
use crate::ui::text::sanitize_display;
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, browser: &MessageBrowserState) {
    let Some(msg) = browser.messages.get(browser.selected) else {
        frame.render_widget(
            Block::default()
                .borders(Borders::ALL)
                .title(" Message Detail "),
            area,
        );
        return;
    };

    let headers = msg
        .raw
        .headers
        .iter()
        .map(|(k, v)| format!("{}: {}", k, String::from_utf8_lossy(v)))
        .collect::<Vec<_>>()
        .join("\n");

    let key = msg
        .raw
        .key
        .as_ref()
        .map(|k| String::from_utf8_lossy(k).to_string())
        .unwrap_or_else(|| "(null)".to_string());

    let text = sanitize_display(&format!(
        "Partition: {}  Offset: {}  Format: {:?}\nKey: {}\nHeaders:\n{}\n\nValue:\n{}",
        msg.raw.partition,
        msg.raw.offset,
        msg.format,
        key,
        if headers.is_empty() { "(none)" } else { &headers },
        msg.formatted_value
    ));

    let widget = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Message Detail ")
                .border_style(theme::MODAL_BORDER),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(widget, area);
}
