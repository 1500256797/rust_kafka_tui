use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::app::state::{App, ConnectionStatus, DialogChoice, Modal, Notification, ProduceDraft, Screen, PAGE_SIZE_OPTIONS};
use crate::clipboard::{ClipboardService, CopyError};
use crate::kafka::{ProduceRequest, ProduceResult, TopicInfo};
use crate::service::{BrowseMode, MessageBrowserState, PageDirection, ProduceService};
use crate::ui::text::sanitize_display;

pub type CommandSender = mpsc::UnboundedSender<KafkaCommand>;
pub type EventSender = mpsc::UnboundedSender<KafkaEvent>;

#[derive(Debug)]
pub enum KafkaCommand {
    Connect { cluster: String },
    Disconnect,
    FetchTopics,
    FetchMessagePage {
        topic: String,
        mode: BrowseMode,
        direction: PageDirection,
        page_size: usize,
    },
    Produce(ProduceRequest),
    ReloadConfig,
}

#[derive(Debug)]
pub enum KafkaEvent {
    Connected { cluster: String, allow_produce: bool },
    ConnectionFailed { error: String },
    TopicsLoaded { topics: Vec<TopicInfo> },
    TopicsFailed { error: String },
    MessagePageLoaded { state: MessageBrowserState },
    MessagePageFailed { error: String },
    MessageProduced { result: ProduceResult },
    ProduceFailed { error: String },
    ConfigReloaded,
    ConfigReloadFailed { error: String },
}

pub fn handle_key(
    app: &mut App,
    key: KeyEvent,
    cmd_tx: &CommandSender,
    clipboard: &ClipboardService,
) {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
        let _ = cmd_tx.send(KafkaCommand::ReloadConfig);
        return;
    }

    if app.modal.is_some() {
        handle_modal_key(app, key, cmd_tx, clipboard);
        return;
    }

    if key.modifiers.contains(KeyModifiers::ALT) {
        match key.code {
            KeyCode::Left => {
                go_back(app);
                return;
            }
            KeyCode::Right => {
                go_forward(app);
                return;
            }
            _ => {}
        }
    }

    match app.screen {
        Screen::ClusterSelect => handle_cluster_key(app, key, cmd_tx),
        Screen::TopicList => handle_topic_list_key(app, key, cmd_tx),
        Screen::MessageBrowser => handle_message_browser_key(app, key, cmd_tx, clipboard),
    }
}

const DOUBLE_CLICK_THRESHOLD: Duration = Duration::from_millis(400);

pub fn handle_mouse(
    app: &mut App,
    mouse: MouseEvent,
    cmd_tx: &CommandSender,
    _clipboard: &ClipboardService,
) {
    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
        if app.screen == Screen::MessageBrowser {
            if let Some(pager) = app.hit_areas.pager.clone() {
                if pager.hit_page_size(mouse.column, mouse.row) {
                    if matches!(app.modal, Some(Modal::PageSizePicker { .. })) {
                        app.close_modal();
                    } else if let Some(anchor) = app.hit_areas.pager.as_ref().map(|p| p.page_size) {
                        app.open_page_size_picker(anchor);
                    }
                    return;
                }
                if pager.hit_page_prev(mouse.column, mouse.row) {
                    let topic = app
                        .message_browser
                        .as_ref()
                        .filter(|b| b.has_prev && !b.loading)
                        .map(|b| b.topic.clone());
                    if let Some(topic) = topic {
                        fetch_page(app, cmd_tx, &topic, PageDirection::Prev);
                    }
                    return;
                }
                if pager.hit_page_next(mouse.column, mouse.row) {
                    let topic = app
                        .message_browser
                        .as_ref()
                        .filter(|b| b.has_next && !b.loading)
                        .map(|b| b.topic.clone());
                    if let Some(topic) = topic {
                        fetch_page(app, cmd_tx, &topic, PageDirection::Next);
                    }
                    return;
                }
            }
        }
        if let Some(toolbar) = app.hit_areas.toolbar.clone() {
            if toolbar.hit_cluster(mouse.column, mouse.row) {
                if matches!(app.modal, Some(Modal::ClusterPicker { .. })) {
                    app.close_modal();
                } else {
                    app.open_cluster_picker();
                }
                return;
            }
            if toolbar.hit_back(mouse.column, mouse.row) {
                go_back(app);
                return;
            }
            if toolbar.hit_forward(mouse.column, mouse.row) {
                go_forward(app);
                return;
            }
        }
    }

    if matches!(app.modal, Some(Modal::ClusterPicker { .. })) {
        handle_cluster_picker_mouse(app, mouse, cmd_tx);
        return;
    }

    if matches!(app.modal, Some(Modal::PageSizePicker { .. })) {
        handle_page_size_picker_mouse(app, mouse, cmd_tx);
        return;
    }

    if app.modal.is_some() {
        if matches!(app.modal, Some(Modal::Help) | Some(Modal::PartitionInfo))
            && matches!(mouse.kind, MouseEventKind::Down(_))
        {
            app.close_modal();
        }
        return;
    }

    match mouse.kind {
        MouseEventKind::ScrollUp => match app.screen {
            Screen::ClusterSelect => move_cluster_selection(app, -1),
            Screen::TopicList => move_topic_selection(app, -1),
            Screen::MessageBrowser => move_message_selection(app, -1),
        },
        MouseEventKind::ScrollDown => match app.screen {
            Screen::ClusterSelect => move_cluster_selection(app, 1),
            Screen::TopicList => move_topic_selection(app, 1),
            Screen::MessageBrowser => move_message_selection(app, 1),
        },
        MouseEventKind::Down(MouseButton::Left) => {
            handle_left_click(app, mouse.column, mouse.row, cmd_tx);
        }
        _ => {}
    }
}

fn handle_cluster_picker_mouse(
    app: &mut App,
    mouse: MouseEvent,
    cmd_tx: &CommandSender,
) {
    match mouse.kind {
        MouseEventKind::ScrollUp => {
            if let Some(Modal::ClusterPicker { selected }) = &mut app.modal {
                if *selected > 0 {
                    *selected -= 1;
                }
            }
        }
        MouseEventKind::ScrollDown => {
            if let Some(Modal::ClusterPicker { selected }) = &mut app.modal {
                if *selected + 1 < app.cluster_names.len() {
                    *selected += 1;
                }
            }
        }
        MouseEventKind::Down(MouseButton::Left) => {
            let Some(list) = app.hit_areas.list.clone() else {
                app.close_modal();
                return;
            };
            if let Some(index) = list.row_at(mouse.column, mouse.row) {
                connect_cluster_from_picker(app, cmd_tx, index);
            } else {
                app.close_modal();
            }
        }
        _ => {}
    }
}

fn handle_page_size_picker_mouse(
    app: &mut App,
    mouse: MouseEvent,
    cmd_tx: &CommandSender,
) {
    match mouse.kind {
        MouseEventKind::ScrollUp => {
            if let Some(Modal::PageSizePicker { selected, .. }) = &mut app.modal {
                if *selected > 0 {
                    *selected -= 1;
                }
            }
        }
        MouseEventKind::ScrollDown => {
            if let Some(Modal::PageSizePicker { selected, .. }) = &mut app.modal {
                if *selected + 1 < PAGE_SIZE_OPTIONS.len() {
                    *selected += 1;
                }
            }
        }
        MouseEventKind::Down(MouseButton::Left) => {
            let Some(list) = app.hit_areas.list.clone() else {
                app.close_modal();
                return;
            };
            if let Some(index) = list.row_at(mouse.column, mouse.row) {
                if let Some(size) = PAGE_SIZE_OPTIONS.get(index) {
                    apply_page_size(app, cmd_tx, *size);
                }
            } else {
                app.close_modal();
            }
        }
        _ => {}
    }
}

fn handle_left_click(app: &mut App, col: u16, row: u16, cmd_tx: &CommandSender) {
    let Some(list) = app.hit_areas.list.clone() else {
        return;
    };

    if app.screen == Screen::MessageBrowser {
        if let Some(index) = list.reply_at(col, row) {
            open_reply_preview(app, index);
            return;
        }
        if let Some(index) = list.row_at(col, row) {
            if let Some(browser) = &mut app.message_browser {
                if index < browser.messages.len() {
                    browser.selected = index;
                }
            }
        }
        return;
    }

    let Some(index) = list.row_at(col, row) else {
        return;
    };

    let now = Instant::now();
    let is_double = app
        .last_mouse_click
        .map(|(t, r)| r == index && now.duration_since(t) < DOUBLE_CLICK_THRESHOLD)
        .unwrap_or(false);
    app.last_mouse_click = Some((now, index));

    match app.screen {
        Screen::ClusterSelect => {
            if index < app.cluster_names.len() {
                app.cluster_selected = index;
                if is_double {
                    connect_cluster_at(app, cmd_tx, index);
                }
            }
        }
        Screen::TopicList => {
            let filtered_len = app.filtered_topics().len();
            if index < filtered_len {
                app.topic_selected = index;
                if is_double {
                    open_topic_at(app, cmd_tx, index);
                }
            }
        }
        Screen::MessageBrowser => {}
    }
}

fn move_cluster_selection(app: &mut App, delta: i32) {
    if delta < 0 {
        if app.cluster_selected > 0 {
            app.cluster_selected -= 1;
        }
    } else if app.cluster_selected + 1 < app.cluster_names.len() {
        app.cluster_selected += 1;
    }
}

fn move_topic_selection(app: &mut App, delta: i32) {
    let filtered_len = app.filtered_topics().len();
    if delta < 0 {
        if app.topic_selected > 0 {
            app.topic_selected -= 1;
        }
    } else if filtered_len > 0 && app.topic_selected + 1 < filtered_len {
        app.topic_selected += 1;
    }
}

fn move_message_selection(app: &mut App, delta: i32) {
    if let Some(browser) = &mut app.message_browser {
        if delta < 0 {
            if browser.selected > 0 {
                browser.selected -= 1;
            }
        } else if browser.selected + 1 < browser.messages.len() {
            browser.selected += 1;
        }
    }
}

fn connect_cluster_at(app: &mut App, cmd_tx: &CommandSender, index: usize) {
    connect_cluster_from_picker(app, cmd_tx, index);
}

fn connect_cluster_from_picker(app: &mut App, cmd_tx: &CommandSender, index: usize) {
    let Some(cluster) = app.cluster_names.get(index).cloned() else {
        return;
    };

    let same_cluster = app.current_cluster.as_deref() == Some(cluster.as_str());
    let connected = matches!(app.connection_status, ConnectionStatus::Connected);

    app.close_modal();
    app.cluster_selected = index;

    if same_cluster && connected {
        return;
    }

    app.clear_nav();
    app.connection_status = ConnectionStatus::Connecting;
    app.current_cluster = Some(cluster.clone());
    app.message_browser = None;
    app.topics.clear();
    app.topics_loading = true;
    app.screen = Screen::TopicList;
    app.show_notification(Notification::info(
        format!("正在连接 {}...", cluster),
        Duration::from_secs(3),
    ));
    let _ = cmd_tx.send(KafkaCommand::Connect { cluster });
}

fn go_back(app: &mut App) {
    if let Some(snap) = app.nav_back.pop() {
        app.nav_forward.push(app.nav_snapshot());
        app.nav_restore(snap);
    }
}

fn go_forward(app: &mut App) {
    if let Some(snap) = app.nav_forward.pop() {
        app.nav_back.push(app.nav_snapshot());
        app.nav_restore(snap);
    }
}

fn open_topic_at(app: &mut App, cmd_tx: &CommandSender, index: usize) {
    app.push_nav();
    app.topic_selected = index;
    if let Some(topic) = app.filtered_topics().get(index).cloned() {
        app.screen = Screen::MessageBrowser;
        app.message_browser = Some(MessageBrowserState {
            topic: topic.name.clone(),
            mode: app.browse_mode,
            messages: vec![],
            cursors: vec![],
            start_offset: 0,
            page_index: 0,
            current_page: 1,
            total_pages: 1,
            total_messages_estimate: 0,
            partitions: vec![],
            loading: true,
            selected: 0,
            detail_expanded: true,
            has_prev: false,
            has_next: false,
        });
        let _ = cmd_tx.send(KafkaCommand::FetchMessagePage {
            topic: topic.name,
            mode: app.browse_mode,
            direction: PageDirection::First,
            page_size: app.page_size,
        });
    } else {
        app.nav_back.pop();
    }
}

fn handle_cluster_key(app: &mut App, key: KeyEvent, cmd_tx: &CommandSender) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Char('j') | KeyCode::Down => {
            if app.cluster_selected + 1 < app.cluster_names.len() {
                app.cluster_selected += 1;
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if app.cluster_selected > 0 {
                app.cluster_selected -= 1;
            }
        }
        KeyCode::Enter => connect_cluster_at(app, cmd_tx, app.cluster_selected),
        KeyCode::Char('?') => app.open_modal(Modal::Help),
        KeyCode::Char('c') | KeyCode::Char('C') => app.open_cluster_picker(),
        _ => {}
    }
}

fn handle_topic_list_key(app: &mut App, key: KeyEvent, cmd_tx: &CommandSender) {
    let filtered_len = app.filtered_topics().len();

    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Esc => {
            if app.can_go_back() {
                go_back(app);
            }
        }
        KeyCode::Char('c') | KeyCode::Char('C') => app.open_cluster_picker(),
        KeyCode::Char('/') => {
            app.filter_mode = true;
            app.topic_filter.clear();
        }
        KeyCode::Char('r') => {
            app.topics_loading = true;
            app.show_notification(Notification::info(
                "正在刷新 Topic 列表...",
                Duration::from_secs(2),
            ));
            let _ = cmd_tx.send(KafkaCommand::FetchTopics);
        }
        KeyCode::Char('i') => {
            app.show_internal_topics = !app.show_internal_topics;
            app.topic_selected = 0;
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if filtered_len > 0 && app.topic_selected + 1 < filtered_len {
                app.topic_selected += 1;
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if app.topic_selected > 0 {
                app.topic_selected -= 1;
            }
        }
        KeyCode::Enter => open_topic_at(app, cmd_tx, app.topic_selected),
        KeyCode::Char('?') => app.open_modal(Modal::Help),
        KeyCode::Backspace if app.filter_mode || !app.topic_filter.is_empty() => {
            app.topic_filter.pop();
            app.topic_selected = 0;
        }
        KeyCode::Char(c) if app.filter_mode || !c.is_control() => {
            app.filter_mode = true;
            app.topic_filter.push(c);
            app.topic_selected = 0;
        }
        _ => {}
    }
}

fn handle_message_browser_key(
    app: &mut App,
    key: KeyEvent,
    cmd_tx: &CommandSender,
    clipboard: &ClipboardService,
) {
    let topic = app
        .message_browser
        .as_ref()
        .map(|b| b.topic.clone())
        .unwrap_or_default();

    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Esc => {
            if app.can_go_back() {
                go_back(app);
            } else {
                app.screen = Screen::TopicList;
                app.message_browser = None;
            }
        }
        KeyCode::Char('j') | KeyCode::Down => {
            if let Some(browser) = &mut app.message_browser {
                if browser.selected + 1 < browser.messages.len() {
                    browser.selected += 1;
                }
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if let Some(browser) = &mut app.message_browser {
                if browser.selected > 0 {
                    browser.selected -= 1;
                }
            }
        }
        KeyCode::Char('n') | KeyCode::Char(']') => {
            fetch_page(app, cmd_tx, &topic, PageDirection::Next);
        }
        KeyCode::Char('p') | KeyCode::Char('[') => {
            fetch_page(app, cmd_tx, &topic, PageDirection::Prev);
        }
        KeyCode::Char('g') => app.open_modal(Modal::GoToOffsetInput {
            input: String::new(),
        }),
        KeyCode::Char('t') => app.open_modal(Modal::GoToTimestampInput {
            input: String::new(),
        }),
        KeyCode::Char('b') => {
            fetch_page(app, cmd_tx, &topic, PageDirection::First);
        }
        KeyCode::Char('l') => {
            fetch_page(app, cmd_tx, &topic, PageDirection::Last);
        }
        KeyCode::Char('m') => {
            app.browse_mode = match app.browse_mode {
                BrowseMode::Merged => BrowseMode::SinglePartition {
                    partition: app.single_partition,
                },
                BrowseMode::SinglePartition { .. } => BrowseMode::Merged,
            };
            fetch_page(app, cmd_tx, &topic, PageDirection::First);
        }
        KeyCode::Char('f') => {
            app.format_override = app.format_override.next();
        }
        KeyCode::Char('d') => {
            if let Some(browser) = &mut app.message_browser {
                browser.detail_expanded = !browser.detail_expanded;
            }
        }
        KeyCode::Char('y') => copy_selected(app, clipboard, CopyKind::FormattedValue),
        KeyCode::Char('Y') => copy_selected(app, clipboard, CopyKind::RawValue),
        KeyCode::Char('K') => copy_selected(app, clipboard, CopyKind::Key),
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {}
        KeyCode::Char('C') => copy_selected(app, clipboard, CopyKind::FullJson),
        KeyCode::Char('R') => {
            if app.is_readonly() {
                app.show_notification(Notification::error(
                    "只读模式，写入已禁用",
                    std::time::Duration::from_secs(3),
                ));
            } else if let Some(msg) = selected_message(app) {
                let headers = msg
                    .raw
                    .headers
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}: {}",
                            k,
                            String::from_utf8_lossy(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let key = msg
                    .raw
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_default();
                app.open_modal(Modal::Produce(ProduceDraft::from_replay(
                    &topic,
                    &key,
                    &headers,
                    &msg.formatted_value,
                )));
            }
        }
        KeyCode::Char('P') => {
            if app.is_readonly() {
                app.show_notification(Notification::error(
                    "只读模式，写入已禁用",
                    std::time::Duration::from_secs(3),
                ));
            } else {
                let mut draft = ProduceDraft::blank();
                draft.target_topic = topic.clone();
                app.open_modal(Modal::Produce(draft));
            }
        }
        KeyCode::Char('i') => app.open_modal(Modal::PartitionInfo),
        KeyCode::Char('?') => app.open_modal(Modal::Help),
        _ => {}
    }
}

enum CopyKind {
    FormattedValue,
    RawValue,
    Key,
    FullJson,
}

fn copy_selected(app: &mut App, clipboard: &ClipboardService, kind: CopyKind) {
    let Some(msg) = selected_message(app) else {
        return;
    };
    let text = match kind {
        CopyKind::FormattedValue => msg.formatted_value.clone(),
        CopyKind::RawValue => msg
            .raw
            .value
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_else(|| "(null)".to_string()),
        CopyKind::Key => msg
            .raw
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .unwrap_or_default(),
        CopyKind::FullJson => serde_json::json!({
            "topic": msg.raw.topic,
            "partition": msg.raw.partition,
            "offset": msg.raw.offset,
            "timestamp": msg.raw.timestamp.millis,
            "key": msg.raw.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
            "value": msg.raw.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
            "headers": msg.raw.headers.iter().map(|(k,v)| (k, String::from_utf8_lossy(v).to_string())).collect::<Vec<_>>(),
        })
        .to_string(),
    };

    match clipboard.copy(&text) {
        Ok(_) => app.show_notification(Notification::info(
            "已复制到剪贴板",
            std::time::Duration::from_secs(2),
        )),
        Err(CopyError::Unavailable) => app.show_notification(Notification::error(
            "剪贴板不可用",
            std::time::Duration::from_secs(3),
        )),
        Err(CopyError::Failed(e)) => app.show_notification(Notification::error(
            e,
            std::time::Duration::from_secs(3),
        )),
    }
}

fn selected_message(app: &App) -> Option<&crate::service::DisplayMessage> {
    app.message_browser
        .as_ref()
        .and_then(|b| b.messages.get(b.selected))
}

fn message_at(app: &App, index: usize) -> Option<(&str, &crate::service::DisplayMessage)> {
    app.message_browser.as_ref().and_then(|b| {
        b.messages
            .get(index)
            .map(|msg| (b.topic.as_str(), msg))
    })
}

fn build_reply_preview(topic: &str, msg: &crate::service::DisplayMessage) -> (ProduceRequest, String) {
    let request = ProduceService::from_message(&msg.raw, topic, None);

    let key = msg
        .raw
        .key
        .as_ref()
        .map(|k| String::from_utf8_lossy(k).to_string())
        .unwrap_or_else(|| "(null)".to_string());

    let headers = msg
        .raw
        .headers
        .iter()
        .map(|(k, v)| format!("  {}: {}", k, String::from_utf8_lossy(v)))
        .collect::<Vec<_>>()
        .join("\n");

    let value_preview = if msg.formatted_value.chars().count() > 500 {
        format!("{}...", msg.formatted_value.chars().take(500).collect::<String>())
    } else {
        msg.formatted_value.clone()
    };

    let preview = sanitize_display(&format!(
        "即将重发以下消息:\n\nTopic: {}\nPartition: auto\nKey: {}\nHeaders:\n{}\n\nValue:\n{}",
        topic,
        key,
        if headers.is_empty() {
            "  (none)".to_string()
        } else {
            headers
        },
        value_preview,
    ));

    (request, preview)
}

fn open_reply_preview(app: &mut App, index: usize) {
    if app.is_readonly() {
        app.show_notification(Notification::error(
            "只读模式，写入已禁用",
            std::time::Duration::from_secs(3),
        ));
        return;
    }
    let Some((topic, msg)) = message_at(app, index) else {
        return;
    };
    let (request, preview) = build_reply_preview(topic, msg);
    app.open_modal(Modal::ReplyPreview {
        request,
        preview,
        choice: DialogChoice::default(),
    });
}

fn submit_reply(app: &mut App, cmd_tx: &CommandSender, request: ProduceRequest) {
    if app.is_readonly() {
        app.show_notification(Notification::error(
            "只读模式，写入已禁用",
            std::time::Duration::from_secs(3),
        ));
        return;
    }
    let topic = request.topic.clone();
    if ProduceService::needs_confirmation(&topic) {
        let draft = request_to_draft(&request);
        app.open_modal(Modal::ConfirmProduce {
            draft,
            message: format!("即将重发消息到: {}", topic),
            choice: DialogChoice::default(),
            raw_request: Some(request),
        });
    } else {
        dispatch_produce(app, cmd_tx, request);
    }
}

fn request_to_draft(request: &ProduceRequest) -> ProduceDraft {
    ProduceDraft {
        target_topic: request.topic.clone(),
        partition: "auto".to_string(),
        key: request
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .unwrap_or_default(),
        headers: request
            .headers
            .iter()
            .map(|(k, v)| format!("{}: {}", k, String::from_utf8_lossy(v)))
            .collect::<Vec<_>>()
            .join("\n"),
        value: String::from_utf8_lossy(&request.value).to_string(),
        error: None,
        is_replay: true,
        focused_field: 4,
    }
}

fn fetch_page(app: &mut App, cmd_tx: &CommandSender, topic: &str, direction: PageDirection) {
    if let Some(browser) = &mut app.message_browser {
        browser.loading = true;
    }
    let _ = cmd_tx.send(KafkaCommand::FetchMessagePage {
        topic: topic.to_string(),
        mode: app.browse_mode,
        direction,
        page_size: app.page_size,
    });
}

fn apply_page_size(app: &mut App, cmd_tx: &CommandSender, size: usize) {
    if app.page_size == size {
        app.close_modal();
        return;
    }
    app.page_size = size;
    app.close_modal();
    if let Some(topic) = app.message_browser.as_ref().map(|b| b.topic.clone()) {
        fetch_page(app, cmd_tx, &topic, PageDirection::First);
    }
}

fn handle_modal_key(
    app: &mut App,
    key: KeyEvent,
    cmd_tx: &CommandSender,
    _clipboard: &ClipboardService,
) {
    match app.modal.clone() {
        Some(Modal::Help) => {
            if matches!(key.code, KeyCode::Esc | KeyCode::Char('?')) {
                app.close_modal();
            }
        }
        Some(Modal::PartitionInfo) => {
            if matches!(
                key.code,
                KeyCode::Esc | KeyCode::Char('i') | KeyCode::Char('q')
            ) {
                app.close_modal();
            }
        }
        Some(Modal::ClusterPicker { mut selected }) => match key.code {
            KeyCode::Esc => app.close_modal(),
            KeyCode::Char('j') | KeyCode::Down => {
                if selected + 1 < app.cluster_names.len() {
                    selected += 1;
                    app.modal = Some(Modal::ClusterPicker { selected });
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if selected > 0 {
                    selected -= 1;
                    app.modal = Some(Modal::ClusterPicker { selected });
                }
            }
            KeyCode::Enter => connect_cluster_from_picker(app, cmd_tx, selected),
            _ => {}
        },
        Some(Modal::PageSizePicker { mut selected, .. }) => match key.code {
            KeyCode::Esc => app.close_modal(),
            KeyCode::Char('j') | KeyCode::Down => {
                if selected + 1 < PAGE_SIZE_OPTIONS.len() {
                    selected += 1;
                    app.modal = Some(Modal::PageSizePicker {
                        selected,
                        anchor: app
                            .hit_areas
                            .pager
                            .as_ref()
                            .map(|p| p.page_size)
                            .unwrap_or_default(),
                    });
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if selected > 0 {
                    selected -= 1;
                    app.modal = Some(Modal::PageSizePicker {
                        selected,
                        anchor: app
                            .hit_areas
                            .pager
                            .as_ref()
                            .map(|p| p.page_size)
                            .unwrap_or_default(),
                    });
                }
            }
            KeyCode::Enter => {
                if let Some(size) = PAGE_SIZE_OPTIONS.get(selected) {
                    apply_page_size(app, cmd_tx, *size);
                }
            }
            _ => {}
        },
        Some(Modal::GoToOffsetInput { mut input }) => match key.code {
            KeyCode::Esc => app.close_modal(),
            KeyCode::Enter => {
                if let Ok(offset) = input.parse::<i64>() {
                    app.close_modal();
                    if let Some(topic) = app.message_browser.as_ref().map(|b| b.topic.clone()) {
                        app.browse_mode = BrowseMode::SinglePartition {
                            partition: app.single_partition,
                        };
                        fetch_page(app, cmd_tx, &topic, PageDirection::GoToOffset(offset));
                    }
                }
            }
            KeyCode::Backspace => {
                input.pop();
                app.modal = Some(Modal::GoToOffsetInput { input });
            }
            KeyCode::Char(c) => {
                input.push(c);
                app.modal = Some(Modal::GoToOffsetInput { input });
            }
            _ => {}
        },
        Some(Modal::GoToTimestampInput { mut input }) => match key.code {
            KeyCode::Esc => app.close_modal(),
            KeyCode::Enter => {
                if let Ok(ts) = input.parse::<i64>() {
                    app.close_modal();
                    if let Some(topic) = app.message_browser.as_ref().map(|b| b.topic.clone()) {
                        fetch_page(app, cmd_tx, &topic, PageDirection::GoToTimestamp(ts));
                    }
                }
            }
            KeyCode::Backspace => {
                input.pop();
                app.modal = Some(Modal::GoToTimestampInput { input });
            }
            KeyCode::Char(c) => {
                input.push(c);
                app.modal = Some(Modal::GoToTimestampInput { input });
            }
            _ => {}
        },
        Some(Modal::Produce(mut draft)) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
                submit_produce(app, cmd_tx, draft);
                return;
            }
            match key.code {
                KeyCode::Esc => app.close_modal(),
                KeyCode::Tab => {
                    draft.focused_field = (draft.focused_field + 1) % 5;
                    app.modal = Some(Modal::Produce(draft));
                }
                KeyCode::BackTab => {
                    draft.focused_field = if draft.focused_field == 0 {
                        4
                    } else {
                        draft.focused_field - 1
                    };
                    app.modal = Some(Modal::Produce(draft));
                }
                KeyCode::Backspace => {
                    edit_draft_field(&mut draft, |s| {
                        s.pop();
                    });
                    app.modal = Some(Modal::Produce(draft));
                }
                KeyCode::Enter if draft.focused_field < 4 => {
                    draft.focused_field += 1;
                    app.modal = Some(Modal::Produce(draft));
                }
                KeyCode::Char(c) => {
                    edit_draft_field(&mut draft, |s| s.push(c));
                    app.modal = Some(Modal::Produce(draft));
                }
                _ => {}
            }
        }
        Some(Modal::ConfirmProduce {
            draft,
            message,
            choice,
            raw_request,
        }) => {
            match dialog_key_action(key, choice) {
                DialogKeyAction::Update(new_choice) => {
                    app.modal = Some(Modal::ConfirmProduce {
                        draft,
                        message,
                        choice: new_choice,
                        raw_request,
                    });
                }
                DialogKeyAction::Confirm => {
                    if choice == DialogChoice::Confirm {
                        if let Some(req) = raw_request {
                            dispatch_produce(app, cmd_tx, req);
                        } else {
                            let draft_clone = draft.clone();
                            dispatch_produce_from_draft(app, cmd_tx, draft_clone);
                        }
                    } else {
                        app.close_modal();
                    }
                }
                DialogKeyAction::Cancel => app.close_modal(),
                DialogKeyAction::None => {}
            }
        }
        Some(Modal::ReplyPreview {
            request,
            preview,
            choice,
        }) => {
            if app.is_readonly() {
                if matches!(key.code, KeyCode::Esc) {
                    app.close_modal();
                }
                return;
            }
            match dialog_key_action(key, choice) {
                DialogKeyAction::Update(new_choice) => {
                    app.modal = Some(Modal::ReplyPreview {
                        request,
                        preview,
                        choice: new_choice,
                    });
                }
                DialogKeyAction::Confirm => {
                    if choice == DialogChoice::Confirm {
                        submit_reply(app, cmd_tx, request);
                    } else {
                        app.close_modal();
                    }
                }
                DialogKeyAction::Cancel => app.close_modal(),
                DialogKeyAction::None => {}
            }
        }
        None => {}
    }
}

fn edit_draft_field(draft: &mut ProduceDraft, f: impl FnOnce(&mut String)) {
    let field = match draft.focused_field {
        0 => &mut draft.target_topic,
        1 => &mut draft.partition,
        2 => &mut draft.key,
        3 => &mut draft.headers,
        _ => &mut draft.value,
    };
    f(field);
}

fn submit_produce(app: &mut App, cmd_tx: &CommandSender, draft: ProduceDraft) {
    if app.is_readonly() {
        app.show_notification(Notification::error(
            "只读模式，写入已禁用",
            std::time::Duration::from_secs(3),
        ));
        return;
    }
    let target = draft.target_topic.clone();
    if ProduceService::needs_confirmation(&target) {
        app.open_modal(Modal::ConfirmProduce {
            draft,
            message: format!("即将发送消息到: {}", target),
            choice: DialogChoice::default(),
            raw_request: None,
        });
    } else {
        dispatch_produce_from_draft(app, cmd_tx, draft);
    }
}

fn dispatch_produce(app: &mut App, cmd_tx: &CommandSender, req: ProduceRequest) {
    app.close_modal();
    app.show_notification(Notification::info(
        format!("正在发送到 {}...", req.topic),
        std::time::Duration::from_secs(30),
    ));
    let _ = cmd_tx.send(KafkaCommand::Produce(req));
}

fn dispatch_produce_from_draft(app: &mut App, cmd_tx: &CommandSender, draft: ProduceDraft) {
    let partition = if draft.partition == "auto" || draft.partition.is_empty() {
        None
    } else {
        draft.partition.parse().ok()
    };

    let headers: Vec<(String, Vec<u8>)> = draft
        .headers
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, ':');
            Some((
                parts.next()?.trim().to_string(),
                parts.next().unwrap_or("").trim().as_bytes().to_vec(),
            ))
        })
        .collect();

    let req = ProduceRequest {
        topic: draft.target_topic,
        partition,
        key: if draft.key.is_empty() {
            None
        } else {
            Some(draft.key.into_bytes())
        },
        value: draft.value.into_bytes(),
        headers,
    };

    dispatch_produce(app, cmd_tx, req);
}

enum DialogKeyAction {
    None,
    Update(DialogChoice),
    Confirm,
    Cancel,
}

fn dialog_key_action(key: KeyEvent, choice: DialogChoice) -> DialogKeyAction {
    match key.code {
        KeyCode::Left | KeyCode::Char('h') => DialogKeyAction::Update(choice.toggle()),
        KeyCode::Right | KeyCode::Char('l') => DialogKeyAction::Update(choice.toggle()),
        KeyCode::Enter => match choice {
            DialogChoice::Confirm => DialogKeyAction::Confirm,
            DialogChoice::Cancel => DialogKeyAction::Cancel,
        },
        KeyCode::Esc => DialogKeyAction::Cancel,
        KeyCode::Char('y') | KeyCode::Char('Y') => DialogKeyAction::Confirm,
        KeyCode::Char('n') | KeyCode::Char('N') => DialogKeyAction::Cancel,
        _ => DialogKeyAction::None,
    }
}

fn refresh_messages_after_produce(app: &mut App, cmd_tx: &CommandSender) {
    if app.screen != Screen::MessageBrowser {
        return;
    }
    let Some(browser) = app.message_browser.as_ref() else {
        return;
    };
    let topic = browser.topic.clone();
    let direction = if browser.has_next {
        PageDirection::Refresh
    } else {
        app.refresh_after_produce = true;
        PageDirection::Last
    };
    fetch_page(app, cmd_tx, &topic, direction);
}

pub fn handle_kafka_event(app: &mut App, event: KafkaEvent, cmd_tx: &CommandSender) {
    match event {
        KafkaEvent::Connected {
            cluster,
            allow_produce,
        } => {
            app.connection_status = ConnectionStatus::Connected;
            app.current_cluster = Some(cluster);
            app.allow_produce = allow_produce;
            app.screen = Screen::TopicList;
            app.topics_loading = true;
            app.show_notification(Notification::info(
                "已连接",
                std::time::Duration::from_secs(2),
            ));
        }
        KafkaEvent::ConnectionFailed { error } => {
            app.connection_status = ConnectionStatus::Failed(error.clone());
            app.show_notification(Notification::error(
                format!("连接失败: {}", error),
                std::time::Duration::from_secs(5),
            ));
        }
        KafkaEvent::TopicsLoaded { topics } => {
            app.topics = topics;
            app.topics_loading = false;
            app.topic_selected = 0;
        }
        KafkaEvent::TopicsFailed { error } => {
            app.topics_loading = false;
            app.show_notification(Notification::error(
                format!("Topic 加载失败: {}", error),
                std::time::Duration::from_secs(5),
            ));
        }
        KafkaEvent::MessagePageLoaded { mut state } => {
            state.loading = false;
            if let Some(prev) = &app.message_browser {
                state.detail_expanded = prev.detail_expanded;
                if app.refresh_after_produce {
                    state.selected = state.messages.len().saturating_sub(1);
                    app.refresh_after_produce = false;
                } else {
                    state.selected = prev
                        .selected
                        .min(state.messages.len().saturating_sub(1));
                }
            }
            app.message_browser = Some(state);
        }
        KafkaEvent::MessagePageFailed { error } => {
            if let Some(browser) = &mut app.message_browser {
                browser.loading = false;
            }
            app.show_notification(Notification::error(
                format!("消息加载失败: {}", error),
                std::time::Duration::from_secs(5),
            ));
        }
        KafkaEvent::MessageProduced { result } => {
            app.show_notification(Notification::info(
                format!(
                    "已发送 partition={} offset={}",
                    result.partition, result.offset
                ),
                std::time::Duration::from_secs(6),
            ));
            refresh_messages_after_produce(app, cmd_tx);
        }
        KafkaEvent::ProduceFailed { error } => {
            app.show_notification(Notification::error(
                format!("发送失败: {}", error),
                std::time::Duration::from_secs(8),
            ));
        }
        KafkaEvent::ConfigReloaded => {
            app.show_notification(Notification::info(
                "配置已重载",
                std::time::Duration::from_secs(3),
            ));
        }
        KafkaEvent::ConfigReloadFailed { error } => {
            app.show_notification(Notification::error(
                format!("配置重载失败: {}", error),
                std::time::Duration::from_secs(5),
            ));
        }
    }
}
