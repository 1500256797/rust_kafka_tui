use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, List, ListItem};

use crate::app::{App, ListHitArea};
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let items: Vec<ListItem> = app
        .cluster_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let marker = if i == app.cluster_selected { "▶ " } else { "  " };
            ListItem::new(format!("{}{}", marker, name))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" 选择 Cluster ")
        .border_style(theme::HEADER);

    let list = List::new(items).block(block).highlight_style(theme::SELECTED);
    frame.render_stateful_widget(list, area, &mut ratatui::widgets::ListState::default().with_selected(Some(app.cluster_selected)));

    let inner = inner_rect(area);
    app.hit_areas.list = Some(ListHitArea {
        body: inner,
        row_count: app.cluster_names.len(),
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
