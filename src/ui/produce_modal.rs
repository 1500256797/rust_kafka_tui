use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::app::state::ProduceDraft;
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, draft: &ProduceDraft, readonly: bool) {
    let title = if draft.is_replay {
        " Replay Message "
    } else {
        " Produce Message "
    };

    let fields = [
        ("Target Topic", &draft.target_topic, 0),
        ("Partition", &draft.partition, 1),
        ("Key", &draft.key, 2),
        ("Headers", &draft.headers, 3),
        ("Value", &draft.value, 4),
    ];

    let mut lines = Vec::new();
    for (label, value, idx) in &fields {
        let prefix = if draft.focused_field == *idx { "> " } else { "  " };
        lines.push(format!("{}{}: {}", prefix, label, value));
    }

    if readonly {
        lines.push("\n[只读模式，写入已禁用]".to_string());
    } else {
        lines.push("\nCtrl+S: Send  Esc: Cancel".to_string());
    }

    if let Some(err) = &draft.error {
        lines.push(format!("\nError: {}", err));
    }

    let widget = Paragraph::new(lines.join("\n"))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(theme::MODAL_BORDER),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(widget, area);
}
