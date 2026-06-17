use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Clear};

use crate::app::App;
use crate::ui::theme;

pub fn render(frame: &mut Frame, anchor: Rect, app: &mut App, selected: usize) {
    let row_count = app.cluster_names.len().min(8) as u16;
    let width = anchor.width.max(24);
    let height = row_count.saturating_add(2);

    let mut dropdown = Rect {
        x: anchor.x,
        y: anchor.y.saturating_add(anchor.height),
        width,
        height,
    };

    let screen = frame.area();
    if dropdown.y.saturating_add(dropdown.height) > screen.y.saturating_add(screen.height) {
        dropdown.y = anchor.y.saturating_sub(dropdown.height);
    }
    if dropdown.x.saturating_add(dropdown.width) > screen.x.saturating_add(screen.width) {
        dropdown.x = screen
            .x
            .saturating_add(screen.width)
            .saturating_sub(dropdown.width);
    }

    frame.render_widget(Clear, dropdown);

    let items: Vec<ListItem> = app
        .cluster_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let current = app.current_cluster.as_deref() == Some(name.as_str());
            let prefix = if i == selected {
                "▶ "
            } else if current {
                "● "
            } else {
                "  "
            };
            ListItem::new(format!("{}{}", prefix, name))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" 切换 Cluster ")
        .border_style(theme::MODAL_BORDER);

    let list = List::new(items)
        .block(block)
        .highlight_style(theme::SELECTED);

    let mut state = ListState::default().with_selected(Some(selected));
    frame.render_stateful_widget(list, dropdown, &mut state);

    let inner = inner_rect(dropdown);
    app.hit_areas.list = Some(crate::app::ListHitArea {
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
