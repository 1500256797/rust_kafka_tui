use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState};

use crate::app::{App, PAGE_SIZE_OPTIONS};
use crate::ui::theme;

pub fn render(frame: &mut Frame, anchor: Rect, app: &mut App, selected: usize) {
    let row_count = PAGE_SIZE_OPTIONS.len() as u16;
    let width = anchor.width.max(12);
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

    frame.render_widget(Clear, dropdown);

    let items: Vec<ListItem> = PAGE_SIZE_OPTIONS
        .iter()
        .enumerate()
        .map(|(i, size)| {
            let marker = if i == selected {
                "▶ "
            } else if *size == app.page_size {
                "● "
            } else {
                "  "
            };
            ListItem::new(format!("{}{} 条/页", marker, size))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Page Size ")
        .border_style(theme::MODAL_BORDER);

    let list = List::new(items)
        .block(block)
        .highlight_style(theme::SELECTED);

    let mut state = ListState::default().with_selected(Some(selected));
    frame.render_stateful_widget(list, dropdown, &mut state);

    let inner = inner_rect(dropdown);
    app.hit_areas.list = Some(crate::app::ListHitArea {
        body: inner,
        row_count: PAGE_SIZE_OPTIONS.len(),
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
