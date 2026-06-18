pub mod text;

pub mod theme {
    use ratatui::style::{Color, Modifier, Style};

    pub const SELECTED: Style = Style::new().bg(Color::DarkGray);
    pub const HEADER: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);
    pub const STATUS_OK: Style = Style::new().fg(Color::Green);
    pub const STATUS_ERR: Style = Style::new().fg(Color::Red);
    pub const FOOTER: Style = Style::new().fg(Color::DarkGray);
    pub const MODAL_BORDER: Style = Style::new().fg(Color::Yellow);
    pub const TITLE: Style = Style::new().fg(Color::White).add_modifier(Modifier::BOLD);
    pub const ACTION: Style = Style::new().fg(Color::Green).add_modifier(Modifier::BOLD);
    pub const DISABLED: Style = Style::new().fg(Color::DarkGray);
}

pub mod cluster;
pub mod help;
pub mod message_browser;
pub mod partition_info;
pub mod message_detail;
pub mod produce_modal;
pub mod reply_preview_modal;
pub mod topic_list;

pub mod components;

use ratatui::Frame;

use crate::app::App;
use crate::app::HitAreas;
use crate::app::Modal;

pub fn render(frame: &mut Frame, app: &mut App) {
    use ratatui::layout::{Constraint, Layout};

    app.hit_areas = HitAreas::default();

    let area = frame.area();
    let [toolbar_area, content_area, footer_area] = Layout::vertical([
        Constraint::Max(3),
        Constraint::Min(0),
        Constraint::Max(1),
    ])
    .areas(area);

    app.hit_areas.content = content_area;

    components::toolbar::render(frame, toolbar_area, app);
    components::footer::render(frame, footer_area, app);

    if let Some(modal) = app.modal.clone() {
        match modal {
            Modal::ClusterPicker { selected } => {
                render_screen(frame, content_area, app);
                if let Some(toolbar) = &app.hit_areas.toolbar {
                    components::cluster_picker::render(
                        frame,
                        toolbar.cluster_dropdown,
                        app,
                        selected,
                    );
                }
                components::toast::render(frame, content_area, app);
                return;
            }
            Modal::PageSizePicker { selected, anchor } => {
                render_screen(frame, content_area, app);
                components::page_size_picker::render(frame, anchor, app, selected);
                components::toast::render(frame, content_area, app);
                return;
            }
            _ => {
                render_modal(frame, content_area, app, &modal);
                components::toast::render(frame, content_area, app);
                return;
            }
        }
    }

    render_screen(frame, content_area, app);
    components::toast::render(frame, content_area, app);
}

fn render_screen(frame: &mut Frame, content_area: ratatui::layout::Rect, app: &mut App) {
    match app.screen {
        crate::app::Screen::ClusterSelect => cluster::render(frame, content_area, app),
        crate::app::Screen::TopicList => topic_list::render(frame, content_area, app),
        crate::app::Screen::MessageBrowser => message_browser::render(frame, content_area, app),
    }
}

fn render_modal(frame: &mut Frame, area: ratatui::layout::Rect, app: &App, modal: &Modal) {
    match modal {
        Modal::Help => help::render(frame, area),
        Modal::PartitionInfo => partition_info::render(frame, area, app),
        Modal::GoToOffsetInput { input } => {
            components::modal::render_input_modal(frame, area, "Go to Offset", input);
        }
        Modal::GoToTimestampInput { input } => {
            components::modal::render_input_modal(frame, area, "Go to Timestamp (epoch ms)", input);
        }
        Modal::Produce(draft) => produce_modal::render(frame, area, draft, app.is_readonly()),
        Modal::ConfirmProduce { message, choice, .. } => {
            components::modal::render_confirm_modal(frame, area, "确认发送", message, *choice);
        }
        Modal::ReplyPreview { preview, choice, .. } => {
            reply_preview_modal::render(frame, area, preview, *choice, app.is_readonly());
        }
        Modal::ClusterPicker { .. } => {}
        Modal::PageSizePicker { .. } => {}
    }
}
