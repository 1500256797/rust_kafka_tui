use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::{App, PagerHitAreas};
use crate::service::MessageBrowserState;
use crate::ui::theme;

const BTN_W: u16 = 4;
const PAGE_W: u16 = 10;
const SIZE_W: u16 = 10;

pub fn render(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    browser: &MessageBrowserState,
    mode: &str,
    format: &str,
) -> PagerHitAreas {
    if area.width < SIZE_W + BTN_W * 2 + PAGE_W + 8 || area.height == 0 {
        return PagerHitAreas {
            page_size: area,
            page_prev: None,
            page_next: None,
        };
    }

    let loading = browser.loading;
    let prev_ok = browser.has_prev && !loading;
    let next_ok = browser.has_next && !loading;

    let [info_area, size_area, prev_area, page_area, next_area] = Layout::horizontal([
        Constraint::Min(8),
        Constraint::Length(SIZE_W),
        Constraint::Length(BTN_W),
        Constraint::Length(PAGE_W),
        Constraint::Length(BTN_W),
    ])
    .areas(area);

    let size_label = format!("{}▼", app.page_size);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("[", theme::HEADER),
            Span::styled(
                size_label,
                Style::new().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::styled("]", theme::HEADER),
        ])),
        size_area,
    );

    let spinner = if loading { " ⟳" } else { "" };
    let info = Line::from(vec![
        Span::styled(format!(" Mode:{} ", mode), theme::HEADER),
        Span::styled("|", theme::FOOTER),
        Span::styled(format!(" Fmt:{} ", format), theme::HEADER),
        Span::styled(spinner, theme::FOOTER),
    ]);
    frame.render_widget(Paragraph::new(info), info_area);

    let prev_style = if prev_ok { theme::ACTION } else { theme::DISABLED };
    let next_style = if next_ok { theme::ACTION } else { theme::DISABLED };
    frame.render_widget(
        Paragraph::new(Span::styled(if prev_ok { " ◀ " } else { " ◁ " }, prev_style)),
        prev_area,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!(" {}/{} ", browser.current_page, browser.total_pages),
            theme::TITLE,
        )),
        page_area,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(if next_ok { " ▶ " } else { " ▷ " }, next_style)),
        next_area,
    );

    PagerHitAreas {
        page_size: size_area,
        page_prev: if prev_ok { Some(prev_area) } else { None },
        page_next: if next_ok { Some(next_area) } else { None },
    }
}
