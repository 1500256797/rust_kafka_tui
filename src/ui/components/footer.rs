use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::{App, Screen};
use crate::service::BrowseMode;
use crate::ui::text::{format_page_index, sanitize_display};
use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    let hints = match app.screen {
        Screen::ClusterSelect => " j/k:选择 Enter:连接 c:Cluster下拉 q:退出 ?:帮助",
        Screen::TopicList => " j/k:移动 Enter:进入 /:搜索 r:刷新 i:内部Topic Alt+←/→:前进后退 c:Cluster q:退出",
        Screen::MessageBrowser => {
            " j/k:移动 点击[50▼]:每页条数 ◀▶:翻页 n/p:翻页 f:格式 y:复制 Reply:重发 q:退出"
        }
    };

    let extra = if app.is_readonly() {
        " [只读模式]"
    } else {
        ""
    };

    let page_info = if let Some(browser) = &app.message_browser {
        let topic = sanitize_display(&browser.topic);
        let page = format_page_index(browser.page_index);
        let mode = match browser.mode {
            BrowseMode::Merged => "Merged",
            BrowseMode::SinglePartition { partition } => {
                return render_with_page(
                    frame,
                    area,
                    &format!(
                        "{} | {} | Part-{} | P{} | ~{} msgs{}",
                        hints,
                        topic,
                        partition,
                        page,
                        browser.total_messages_estimate,
                        extra
                    ),
                    browser.loading,
                    app.tick_count,
                );
            }
        };
        format!(
            "{} | {} | {} | P{} | ~{} msgs{}",
            hints,
            topic,
            mode,
            page,
            browser.total_messages_estimate,
            extra
        )
    } else if app.topics_loading {
        format!("{} | 加载中...{}", hints, extra)
    } else {
        format!("{}{}", hints, extra)
    };

    render_with_page(frame, area, &page_info, false, app.tick_count);
}

fn render_with_page(
    frame: &mut Frame,
    area: Rect,
    text: &str,
    loading: bool,
    tick: u64,
) {
    let spinner = if loading {
        let frames = ['|', '/', '-', '\\'];
        format!(" {}", frames[(tick as usize / 2) % 4])
    } else {
        String::new()
    };

    let line = Line::from(vec![Span::styled(
        truncate_to_width(&format!("{}{}", text, spinner), area.width as usize),
        theme::FOOTER,
    )]);
    frame.render_widget(Paragraph::new(line), area);
}

fn truncate_to_width(text: &str, max_cols: usize) -> String {
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
