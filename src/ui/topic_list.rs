use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::app::{App, ListHitArea};
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let filtered = app.filtered_topics();
    let filter_hint = if app.filter_mode || !app.topic_filter.is_empty() {
        format!(" 搜索: {} ", app.topic_filter)
    } else {
        " Topic 列表 ".to_string()
    };

    let rows: Vec<Row> = filtered
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let style = if i == app.topic_selected {
                theme::SELECTED
            } else {
                theme::FOOTER
            };
            Row::new(vec![
                Cell::from(t.name.clone()),
                Cell::from(t.partitions.len().to_string()),
                Cell::from(if t.is_internal { "yes" } else { "no" }),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            ratatui::layout::Constraint::Min(30),
            ratatui::layout::Constraint::Length(12),
            ratatui::layout::Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Topic", "Partitions", "Internal"])
            .style(theme::HEADER)
            .bottom_margin(1),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(filter_hint)
            .border_style(theme::HEADER),
    );

    frame.render_widget(table, area);

    let inner = inner_rect(area);
    let body = Rect {
        x: inner.x,
        y: inner.y.saturating_add(2),
        width: inner.width,
        height: inner.height.saturating_sub(2),
    };
    app.hit_areas.list = Some(ListHitArea {
        body,
        row_count: filtered.len(),
        scroll_offset: 0,
        reply_buttons: vec![],
    });
}

fn inner_rect(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}
