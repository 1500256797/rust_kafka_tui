use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Cell, Clear, Row, Table};

use crate::app::App;
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let popup = centered_rect(70, 70, area);
    frame.render_widget(Clear, popup);

    let Some(browser) = app.message_browser.as_ref() else {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" 分区信息 ")
            .border_style(theme::MODAL_BORDER);
        frame.render_widget(block, popup);
        return;
    };

    let mut total: i64 = 0;
    let mut rows: Vec<Row> = browser
        .partitions
        .iter()
        .map(|p| {
            let count = (p.high_watermark - p.log_start_offset).max(0);
            total += count;
            Row::new(vec![
                Cell::from(p.id.to_string()),
                Cell::from(p.log_start_offset.to_string()),
                Cell::from(p.high_watermark.to_string()),
                Cell::from(count.to_string()),
            ])
            .style(theme::FOOTER)
        })
        .collect();

    rows.push(
        Row::new(vec![
            Cell::from("合计"),
            Cell::from("-"),
            Cell::from("-"),
            Cell::from(total.to_string()),
        ])
        .style(theme::TITLE),
    );

    let table = Table::new(
        rows,
        [
            ratatui::layout::Constraint::Length(8),
            ratatui::layout::Constraint::Length(14),
            ratatui::layout::Constraint::Length(14),
            ratatui::layout::Constraint::Min(8),
        ],
    )
    .header(
        Row::new(vec!["分区", "起始Offset", "末尾Offset", "保留条数"])
            .style(theme::HEADER)
            .bottom_margin(1),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" 分区信息 - {}  (Esc 关闭) ", browser.topic))
            .border_style(theme::MODAL_BORDER),
    );

    frame.render_widget(table, popup);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let w = r.width * percent_x / 100;
    let h = r.height * percent_y / 100;
    Rect {
        x: r.x + (r.width.saturating_sub(w)) / 2,
        y: r.y + (r.height.saturating_sub(h)) / 2,
        width: w,
        height: h,
    }
}
