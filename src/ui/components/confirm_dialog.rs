use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::app::DialogChoice;
use crate::ui::text::sanitize_display;
use crate::ui::theme;

pub struct ConfirmDialogOptions<'a> {
    pub confirm_label: &'a str,
    pub cancel_label: &'a str,
}

impl Default for ConfirmDialogOptions<'_> {
    fn default() -> Self {
        Self {
            confirm_label: "✓ 确认",
            cancel_label: "✕ 取消",
        }
    }
}

pub fn render(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    body: &str,
    choice: DialogChoice,
    readonly: bool,
    options: ConfirmDialogOptions<'_>,
) {
    let popup = centered_rect(72, 55, area);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::MODAL_BORDER)
            .title(format!(" {} ", title)),
        popup,
    );

    let inner = popup.inner(ratatui::layout::Margin::new(2, 1));
    let [body_area, action_area] =
        Layout::vertical([Constraint::Min(4), Constraint::Length(4)]).areas(inner);

    frame.render_widget(
        Paragraph::new(sanitize_display(body)).wrap(Wrap { trim: false }),
        body_area,
    );

    if readonly {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "[只读模式，写入已禁用]",
                theme::STATUS_ERR,
            ))),
            action_area,
        );
        return;
    }

    let action_line = choice_line(choice, options.confirm_label, options.cancel_label);
    let hint_line = Line::from(Span::styled(
        "  ← → 切换选项    Enter 确认    Esc 取消",
        theme::FOOTER,
    ));

    frame.render_widget(
        Paragraph::new(vec![action_line, hint_line]),
        action_area,
    );
}

fn choice_line(choice: DialogChoice, confirm_label: &str, cancel_label: &str) -> Line<'static> {
    let cancel_spans = option_spans(cancel_label, choice == DialogChoice::Cancel);
    let confirm_spans = option_spans(confirm_label, choice == DialogChoice::Confirm);

    let mut spans = vec![Span::styled("  ◀  ", theme::HEADER)];
    spans.extend(cancel_spans);
    spans.push(Span::raw("    "));
    spans.extend(confirm_spans);
    spans.push(Span::styled("  ▶", theme::HEADER));
    Line::from(spans)
}

fn option_spans(label: &str, selected: bool) -> Vec<Span<'static>> {
    if selected {
        vec![
            Span::styled(" ⟨ ", theme::HEADER),
            Span::styled(label.to_string(), theme::ACTION),
            Span::styled(" ⟩ ", theme::HEADER),
        ]
    } else {
        vec![Span::styled(format!("  {}  ", label), theme::DISABLED)]
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    use ratatui::layout::{Flex, Layout};
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)])
        .flex(Flex::Center)
        .split(r);
    Layout::horizontal([Constraint::Percentage(percent_x)])
        .flex(Flex::Center)
        .split(vertical[0])[0]
}
