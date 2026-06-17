use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::app::{App, ListHitArea};
use crate::ui::components::pager_bar;
use crate::ui::message_detail;
use crate::ui::theme;

const REPLY_COL_WIDTH: u16 = 7;
const PAGER_BAR_HEIGHT: u16 = 1;

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let Some(browser) = app.message_browser.clone() else {
        frame.render_widget(
            Block::default()
                .borders(Borders::ALL)
                .title(" 无消息数据 "),
            area,
        );
        return;
    };

    let format_label = app.format_override.label();
    let readonly = app.is_readonly();
    let hit_area = match browser.mode {
        crate::service::BrowseMode::Merged => {
            if browser.detail_expanded {
                let [table_area, detail_area] =
                    Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                        .areas(area);
                let hit = render_table(
                    frame,
                    table_area,
                    app,
                    &browser,
                    "Merged",
                    format_label,
                    readonly,
                );
                message_detail::render(frame, detail_area, &browser);
                hit
            } else {
                render_table(frame, area, app, &browser, "Merged", format_label, readonly)
            }
        }
        crate::service::BrowseMode::SinglePartition { partition } => {
            let mode_label = format!("Part-{}", partition);
            if browser.detail_expanded {
                let [table_area, detail_area] =
                    Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)])
                        .areas(area);
                let hit = render_table(
                    frame,
                    table_area,
                    app,
                    &browser,
                    &mode_label,
                    format_label,
                    readonly,
                );
                message_detail::render(frame, detail_area, &browser);
                hit
            } else {
                render_table(
                    frame,
                    area,
                    app,
                    &browser,
                    &mode_label,
                    format_label,
                    readonly,
                )
            }
        }
    };
    app.hit_areas.list = Some(hit_area);
}

fn render_table(
    frame: &mut Frame,
    area: Rect,
    app: &mut App,
    browser: &crate::service::MessageBrowserState,
    mode: &str,
    format: &str,
    readonly: bool,
) -> ListHitArea {
    let show_part = matches!(browser.mode, crate::service::BrowseMode::Merged);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            " {} ({}/{}) ",
            browser.topic,
            browser.messages.len(),
            browser.total_messages_estimate
        ))
        .border_style(theme::HEADER);
    frame.render_widget(block, area);

    let inner = inner_rect(area);
    if inner.height < PAGER_BAR_HEIGHT + 2 {
        return ListHitArea {
            body: inner,
            row_count: 0,
            scroll_offset: 0,
            reply_buttons: vec![],
        };
    }

    let [pager_area, table_area] =
        Layout::vertical([Constraint::Length(PAGER_BAR_HEIGHT), Constraint::Min(0)])
            .areas(inner);

    app.hit_areas.pager = Some(pager_bar::render(
        frame, pager_area, app, browser, mode, format,
    ));

    let table_body = Rect {
        x: table_area.x,
        y: table_area.y.saturating_add(2),
        width: table_area.width,
        height: table_area.height.saturating_sub(2),
    };
    let visible_rows = table_body.height as usize;
    let scroll = table_scroll(browser.selected, visible_rows, browser.messages.len());

    let header_cells = if show_part {
        vec!["#", "Part", "Offset", "Timestamp", "Preview", "Reply"]
    } else {
        vec!["#", "Offset", "Timestamp", "Preview", "Reply"]
    };

    let rows: Vec<Row> = browser
        .messages
        .iter()
        .enumerate()
        .skip(scroll)
        .take(visible_rows.max(1))
        .map(|(i, msg)| {
            let ts = chrono::DateTime::from_timestamp_millis(msg.raw.timestamp.millis)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| msg.raw.timestamp.millis.to_string());

            let style = if i == browser.selected {
                theme::SELECTED
            } else {
                theme::FOOTER
            };

            let reply_cell = if readonly {
                Cell::from("  --  ").style(theme::DISABLED)
            } else {
                Cell::from("[Reply]").style(theme::ACTION)
            };

            let cells = if show_part {
                vec![
                    Cell::from((i + 1).to_string()),
                    Cell::from(msg.raw.partition.to_string()),
                    Cell::from(msg.raw.offset.to_string()),
                    Cell::from(ts),
                    Cell::from(msg.preview.clone()),
                    reply_cell,
                ]
            } else {
                vec![
                    Cell::from((i + 1).to_string()),
                    Cell::from(msg.raw.offset.to_string()),
                    Cell::from(ts),
                    Cell::from(msg.preview.clone()),
                    reply_cell,
                ]
            };
            Row::new(cells).style(style)
        })
        .collect();

    let constraints: Vec<Constraint> = if show_part {
        vec![
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(20),
            Constraint::Min(10),
            Constraint::Length(REPLY_COL_WIDTH),
        ]
    } else {
        vec![
            Constraint::Length(4),
            Constraint::Length(10),
            Constraint::Length(20),
            Constraint::Min(10),
            Constraint::Length(REPLY_COL_WIDTH),
        ]
    };

    let table = Table::new(rows, constraints)
        .header(
            Row::new(header_cells.iter().map(|h| Cell::from(*h)).collect::<Vec<_>>())
                .style(theme::HEADER)
                .bottom_margin(1),
        );

    frame.render_widget(table, table_area);

    let visible_count = browser
        .messages
        .len()
        .saturating_sub(scroll)
        .min(visible_rows.max(1));

    let reply_x = table_area
        .x
        .saturating_add(table_area.width.saturating_sub(REPLY_COL_WIDTH));
    let reply_buttons: Vec<Rect> = (0..visible_count)
        .map(|i| Rect {
            x: reply_x,
            y: table_body.y.saturating_add(i as u16),
            width: REPLY_COL_WIDTH,
            height: 1,
        })
        .collect();

    ListHitArea {
        body: table_body,
        row_count: visible_count,
        scroll_offset: scroll,
        reply_buttons,
    }
}

fn table_scroll(selected: usize, visible_rows: usize, total: usize) -> usize {
    if visible_rows == 0 || total <= visible_rows {
        return 0;
    }
    if selected >= total.saturating_sub(visible_rows) {
        total.saturating_sub(visible_rows)
    } else if selected >= visible_rows {
        selected + 1 - visible_rows
    } else {
        0
    }
}

fn inner_rect(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}
