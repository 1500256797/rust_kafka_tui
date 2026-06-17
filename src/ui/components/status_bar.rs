use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::{App, ConnectionStatus};
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let cluster = app
        .current_cluster
        .as_deref()
        .unwrap_or("未选择");

    let status_span = match &app.connection_status {
        ConnectionStatus::Connected => Span::styled("● 已连接", theme::STATUS_OK),
        ConnectionStatus::Connecting => Span::styled("◌ 连接中...", theme::FOOTER),
        ConnectionStatus::Failed(msg) => Span::styled(format!("● 失败: {}", msg), theme::STATUS_ERR),
        ConnectionStatus::Disconnected => Span::styled("○ 未连接", theme::FOOTER),
    };

    let readonly = if app.is_readonly() {
        Span::styled(" [只读]", theme::STATUS_ERR)
    } else {
        Span::raw("")
    };

    let notification = app
        .notification
        .as_ref()
        .map(|n| {
            if n.is_error {
                Span::styled(format!(" | {}", n.message), theme::STATUS_ERR)
            } else {
                Span::styled(format!(" | {}", n.message), theme::STATUS_OK)
            }
        })
        .unwrap_or(Span::raw(""));

    let line = Line::from(vec![
        Span::styled(" kafka-tui ", theme::HEADER),
        Span::raw(format!("Cluster: {} ", cluster)),
        Span::raw(format!("[{}] ", app.bootstrap_servers())),
        status_span,
        readonly,
        notification,
    ]);

    let widget = Paragraph::new(line);
    frame.render_widget(widget, area);
}
