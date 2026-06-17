use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::app::{App, ConnectionStatus, ToolbarHitAreas};
use crate::ui::theme;

const BTN_BACK: &str = " ◀ ";
const BTN_FORWARD: &str = " ▶ ";
const BTN_BACK_OFF: &str = " ◁ ";
const BTN_FORWARD_OFF: &str = " ▷ ";

pub fn render(frame: &mut Frame, area: Rect, app: &mut App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::HEADER);

    let inner = inner_rect(area);
    frame.render_widget(block, area);

    if inner.width < 16 || inner.height == 0 {
        return;
    }

    let (nav_w, cluster_w, status_w) = fit_columns(inner.width, app);

    let [nav_area, cluster_area, breadcrumb_area, status_area] = Layout::horizontal([
        Constraint::Length(nav_w),
        Constraint::Length(cluster_w),
        Constraint::Min(0),
        Constraint::Length(status_w),
    ])
    .areas(inner);

    let can_back = app.can_go_back();
    let can_forward = app.can_go_forward();

    let back_label = if can_back { BTN_BACK } else { BTN_BACK_OFF };
    let forward_label = if can_forward { BTN_FORWARD } else { BTN_FORWARD_OFF };
    let back_style = if can_back {
        theme::TITLE
    } else {
        theme::FOOTER
    };
    let forward_style = if can_forward {
        theme::TITLE
    } else {
        theme::FOOTER
    };

    let nav_line = Line::from(vec![
        Span::styled(back_label, back_style),
        Span::raw(" "),
        Span::styled(forward_label, forward_style),
    ]);
    frame.render_widget(Paragraph::new(nav_line), nav_area);

    let back_rect = Rect {
        x: nav_area.x,
        y: nav_area.y,
        width: 3.min(nav_area.width),
        height: nav_area.height,
    };
    let forward_rect = Rect {
        x: nav_area.x.saturating_add(4),
        y: nav_area.y,
        width: 3.min(nav_area.width.saturating_sub(4)),
        height: nav_area.height,
    };

    let cluster_label = truncate_chars(&app.toolbar_cluster_label(), cluster_w as usize);
    let cluster_line = Line::from(vec![
        Span::styled(" Cl ", theme::FOOTER),
        Span::styled("[", theme::HEADER),
        Span::styled(
            cluster_label,
            Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled("▼]", theme::HEADER),
    ]);
    frame.render_widget(Paragraph::new(cluster_line), cluster_area);

    let breadcrumb = truncate_chars(&app.breadcrumb(), breadcrumb_area.width as usize);
    let breadcrumb_line = Line::from(vec![
        Span::styled("│", theme::FOOTER),
        Span::styled(breadcrumb, theme::TITLE),
    ]);
    frame.render_widget(Paragraph::new(breadcrumb_line), breadcrumb_area);

    let status_line = status_line(app);
    frame.render_widget(Paragraph::new(status_line), status_area);

    app.hit_areas.toolbar = Some(ToolbarHitAreas {
        back: if can_back { Some(back_rect) } else { None },
        forward: if can_forward { Some(forward_rect) } else { None },
        cluster_dropdown: cluster_area,
    });
}

fn fit_columns(inner_width: u16, app: &App) -> (u16, u16, u16) {
    let nav_w = 8.min(inner_width);
    let mut remain = inner_width.saturating_sub(nav_w);

    let status_pref = status_width(app);
    let status_w = status_pref.min(remain.saturating_sub(6)).max(8.min(remain));
    remain = remain.saturating_sub(status_w);

    let cluster_pref = cluster_dropdown_width(app);
    let cluster_w = cluster_pref.min(remain).max(remain.min(10));

    (nav_w, cluster_w, status_w)
}

fn status_line(app: &App) -> Line<'_> {
    let status = match &app.connection_status {
        ConnectionStatus::Connected => Span::styled("●已连", theme::STATUS_OK),
        ConnectionStatus::Connecting => Span::styled("◌连接", theme::FOOTER),
        ConnectionStatus::Failed(_) => Span::styled("●失败", theme::STATUS_ERR),
        ConnectionStatus::Disconnected => Span::styled("○未连", theme::FOOTER),
    };

    let mut spans = vec![Span::styled("│", theme::FOOTER), status];
    if app.is_readonly() {
        spans.push(Span::styled("只读", theme::STATUS_ERR));
    }
    Line::from(spans)
}

fn cluster_dropdown_width(app: &App) -> u16 {
    let label_len = app.toolbar_cluster_label().chars().count() as u16;
    (label_len + 10).clamp(14, 32)
}

fn status_width(app: &App) -> u16 {
    let base = if app.is_readonly() { 14 } else { 10 };
    match &app.connection_status {
        ConnectionStatus::Failed(_) => base + 2,
        _ => base,
    }
}

fn truncate_chars(text: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }
    if text.chars().count() <= max_cols {
        text.to_string()
    } else if max_cols == 1 {
        "…".to_string()
    } else {
        format!(
            "{}…",
            text.chars().take(max_cols - 1).collect::<String>()
        )
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
