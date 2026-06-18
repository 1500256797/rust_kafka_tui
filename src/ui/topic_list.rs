use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::app::{App, ListHitArea};
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let filtered = app.filtered_topics();

    if filtered.is_empty() {
        let msg = if app.topics_loading {
            "正在加载 Topic 列表..."
        } else if !app.topic_filter.is_empty() {
            "没有匹配的 Topic（Backspace 清除搜索）"
        } else {
            "暂无 Topic（r 刷新 / i 切换内部 Topic）"
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Topic 列表 ")
            .border_style(theme::HEADER);
        frame.render_widget(
            Paragraph::new(msg)
                .block(block)
                .alignment(Alignment::Center)
                .style(theme::FOOTER),
            area,
        );
        app.hit_areas.list = Some(ListHitArea {
            body: inner_rect(area),
            row_count: 0,
            scroll_offset: 0,
            reply_buttons: vec![],
        });
        return;
    }

    let total = filtered.len();
    let loading_mark = if app.topics_loading { " ⟳" } else { "" };
    let title = if app.filter_mode || !app.topic_filter.is_empty() {
        format!(
            " 搜索: {} ({}/{}){} ",
            app.topic_filter,
            app.topic_selected + 1,
            total,
            loading_mark
        )
    } else {
        format!(
            " Topic 列表 ({}/{}){} ",
            app.topic_selected + 1,
            total,
            loading_mark
        )
    };

    let inner = inner_rect(area);
    // header 占用 2 行（标题行 + bottom_margin）
    let visible_rows = inner.height.saturating_sub(2) as usize;
    let scroll = table_scroll(app.topic_selected, visible_rows, total);

    let rows: Vec<Row> = filtered
        .iter()
        .enumerate()
        .skip(scroll)
        .take(visible_rows.max(1))
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
            .title(title)
            .border_style(theme::HEADER),
    );

    frame.render_widget(table, area);

    let body = Rect {
        x: inner.x,
        y: inner.y.saturating_add(2),
        width: inner.width,
        height: inner.height.saturating_sub(2),
    };
    let visible_count = total.saturating_sub(scroll).min(visible_rows.max(1));
    app.hit_areas.list = Some(ListHitArea {
        body,
        row_count: visible_count,
        scroll_offset: scroll,
        reply_buttons: vec![],
    });
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
