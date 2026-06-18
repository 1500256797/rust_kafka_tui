use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::layout::Rect;

use crate::config::{AppConfig, Cli, MessageFormat};
use crate::kafka::{ProduceRequest, ProduceResult, TopicInfo};
use crate::service::{BrowseMode, MessageBrowserState, PageDirection};

pub type TaskId = u64;

pub const PAGE_SIZE_OPTIONS: [usize; 3] = [20, 50, 100];

pub fn resolve_page_size(size: usize) -> usize {
    if PAGE_SIZE_OPTIONS.contains(&size) {
        size
    } else {
        50
    }
}

#[derive(Debug, Clone, Default)]
pub struct HitAreas {
    pub content: Rect,
    pub list: Option<ListHitArea>,
    pub toolbar: Option<ToolbarHitAreas>,
    pub pager: Option<PagerHitAreas>,
}

#[derive(Debug, Clone)]
pub struct PagerHitAreas {
    pub page_size: Rect,
    pub page_prev: Option<Rect>,
    pub page_next: Option<Rect>,
}

impl PagerHitAreas {
    pub fn hit_page_size(&self, col: u16, row: u16) -> bool {
        rect_contains(self.page_size, col, row)
    }

    pub fn hit_page_prev(&self, col: u16, row: u16) -> bool {
        self.page_prev
            .is_some_and(|r| rect_contains(r, col, row))
    }

    pub fn hit_page_next(&self, col: u16, row: u16) -> bool {
        self.page_next
            .is_some_and(|r| rect_contains(r, col, row))
    }
}

#[derive(Debug, Clone)]
pub struct ToolbarHitAreas {
    pub back: Option<Rect>,
    pub forward: Option<Rect>,
    pub cluster_dropdown: Rect,
}

impl ToolbarHitAreas {
    pub fn hit_back(&self, col: u16, row: u16) -> bool {
        self.back
            .is_some_and(|r| rect_contains(r, col, row))
    }

    pub fn hit_forward(&self, col: u16, row: u16) -> bool {
        self.forward
            .is_some_and(|r| rect_contains(r, col, row))
    }

    pub fn hit_cluster(&self, col: u16, row: u16) -> bool {
        rect_contains(self.cluster_dropdown, col, row)
    }
}

fn rect_contains(rect: Rect, col: u16, row: u16) -> bool {
    col >= rect.x
        && col < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

#[derive(Debug, Clone)]
pub struct ListHitArea {
    pub body: Rect,
    pub row_count: usize,
    pub scroll_offset: usize,
    pub reply_buttons: Vec<Rect>,
}

impl ListHitArea {
    pub fn row_at(&self, col: u16, row: u16) -> Option<usize> {
        if col < self.body.x
            || col >= self.body.x.saturating_add(self.body.width)
            || row < self.body.y
            || row >= self.body.y.saturating_add(self.body.height)
        {
            return None;
        }
        let idx = (row - self.body.y) as usize;
        if idx < self.row_count {
            Some(self.scroll_offset + idx)
        } else {
            None
        }
    }

    pub fn reply_at(&self, col: u16, row: u16) -> Option<usize> {
        self.reply_buttons
            .iter()
            .enumerate()
            .find(|(_, rect)| rect_contains(**rect, col, row))
            .map(|(idx, _)| self.scroll_offset + idx)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    ClusterSelect,
    TopicList,
    MessageBrowser,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub message: String,
    pub expires_at: Instant,
    pub is_error: bool,
}

impl Notification {
    pub fn info(message: impl Into<String>, duration: Duration) -> Self {
        Self {
            message: message.into(),
            expires_at: Instant::now() + duration,
            is_error: false,
        }
    }

    pub fn error(message: impl Into<String>, duration: Duration) -> Self {
        Self {
            message: message.into(),
            expires_at: Instant::now() + duration,
            is_error: true,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

#[derive(Debug, Clone)]
pub struct ProduceDraft {
    pub target_topic: String,
    pub partition: String,
    pub key: String,
    pub headers: String,
    pub value: String,
    pub error: Option<String>,
    pub is_replay: bool,
    pub focused_field: usize,
}

impl ProduceDraft {
    pub fn blank() -> Self {
        Self {
            target_topic: String::new(),
            partition: "auto".to_string(),
            key: String::new(),
            headers: String::new(),
            value: String::new(),
            error: None,
            is_replay: false,
            focused_field: 0,
        }
    }

    pub fn from_replay(topic: &str, key: &str, headers: &str, value: &str) -> Self {
        Self {
            target_topic: topic.to_string(),
            partition: "auto".to_string(),
            key: key.to_string(),
            headers: headers.to_string(),
            value: value.to_string(),
            error: None,
            is_replay: true,
            focused_field: 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DialogChoice {
    #[default]
    Cancel,
    Confirm,
}

impl DialogChoice {
    pub fn toggle(self) -> Self {
        match self {
            Self::Cancel => Self::Confirm,
            Self::Confirm => Self::Cancel,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Modal {
    Help,
    PartitionInfo,
    ClusterPicker { selected: usize },
    PageSizePicker { selected: usize, anchor: Rect },
    GoToOffsetInput { input: String },
    GoToTimestampInput { input: String },
    Produce(ProduceDraft),
    ConfirmProduce {
        draft: ProduceDraft,
        message: String,
        choice: DialogChoice,
        raw_request: Option<crate::kafka::ProduceRequest>,
    },
    ReplyPreview {
        request: ProduceRequest,
        preview: String,
        choice: DialogChoice,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFormatOverride {
    FollowConfig,
    Auto,
    Json,
    Raw,
    Avro,
}

impl MessageFormatOverride {
    pub fn to_format(&self, default: MessageFormat) -> MessageFormat {
        match self {
            Self::FollowConfig => default,
            Self::Auto => MessageFormat::Auto,
            Self::Json => MessageFormat::Json,
            Self::Raw => MessageFormat::Raw,
            Self::Avro => MessageFormat::Avro,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::FollowConfig => "config",
            Self::Auto => "auto",
            Self::Json => "json",
            Self::Raw => "raw",
            Self::Avro => "avro",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::FollowConfig => Self::Auto,
            Self::Auto => Self::Json,
            Self::Json => Self::Raw,
            Self::Raw => Self::Avro,
            Self::Avro => Self::FollowConfig,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NavSnapshot {
    pub screen: Screen,
    pub topic_selected: usize,
    pub topic_filter: String,
    pub filter_mode: bool,
    pub message_browser: Option<MessageBrowserState>,
}

pub struct App {
    pub config: AppConfig,
    pub config_path: PathBuf,
    pub cli_cluster: Option<String>,

    pub current_cluster: Option<String>,
    pub connection_status: ConnectionStatus,
    pub allow_produce: bool,

    pub screen: Screen,
    pub previous_screen: Option<Screen>,

    pub cluster_names: Vec<String>,
    pub cluster_selected: usize,

    pub topics: Vec<TopicInfo>,
    pub topic_filter: String,
    pub show_internal_topics: bool,
    pub topic_selected: usize,
    pub topics_loading: bool,

    pub message_browser: Option<MessageBrowserState>,
    pub browse_mode: BrowseMode,
    pub format_override: MessageFormatOverride,
    pub single_partition: i32,
    pub page_size: usize,

    pub modal: Option<Modal>,
    pub notification: Option<Notification>,

    pub pending_tasks: HashSet<TaskId>,
    pub should_quit: bool,
    pub tick_count: u64,
    pub filter_mode: bool,

    pub hit_areas: HitAreas,
    pub last_mouse_click: Option<(Instant, usize)>,

    pub nav_back: Vec<NavSnapshot>,
    pub nav_forward: Vec<NavSnapshot>,

    pub refresh_after_produce: bool,
}

impl App {
    pub fn new(config: AppConfig, config_path: PathBuf, cli: &Cli) -> Self {
        let cluster_names = config.cluster_names();
        let cluster_selected = cli
            .cluster
            .as_ref()
            .and_then(|name| cluster_names.iter().position(|n| n == name))
            .unwrap_or(0);
        let page_size = resolve_page_size(config.topic_data.page_size);

        Self {
            config,
            config_path,
            cli_cluster: cli.cluster.clone(),
            current_cluster: None,
            connection_status: ConnectionStatus::Disconnected,
            allow_produce: true,
            screen: Screen::ClusterSelect,
            previous_screen: None,
            cluster_names,
            cluster_selected,
            topics: Vec::new(),
            topic_filter: String::new(),
            show_internal_topics: false,
            topic_selected: 0,
            topics_loading: false,
            message_browser: None,
            browse_mode: BrowseMode::Merged,
            format_override: MessageFormatOverride::FollowConfig,
            single_partition: 0,
            page_size,
            modal: None,
            notification: None,
            pending_tasks: HashSet::new(),
            should_quit: false,
            tick_count: 0,
            filter_mode: false,
            hit_areas: HitAreas::default(),
            last_mouse_click: None,
            nav_back: Vec::new(),
            nav_forward: Vec::new(),
            refresh_after_produce: false,
        }
    }

    pub fn filtered_topics(&self) -> Vec<TopicInfo> {
        let topics = if self.show_internal_topics {
            self.topics.clone()
        } else {
            self.topics
                .iter()
                .filter(|t| !t.is_internal)
                .cloned()
                .collect()
        };
        crate::service::TopicService::filter(&topics, &self.topic_filter)
    }

    pub fn selected_topic(&self) -> Option<TopicInfo> {
        self.filtered_topics().get(self.topic_selected).cloned()
    }

    pub fn current_cluster_config(&self) -> Option<&crate::config::ClusterConfig> {
        self.current_cluster
            .as_ref()
            .and_then(|name| self.config.connections.get(name))
    }

    pub fn is_readonly(&self) -> bool {
        !self.allow_produce
    }

    pub fn show_notification(&mut self, notification: Notification) {
        self.notification = Some(Notification {
            message: crate::ui::text::sanitize_display(&notification.message),
            expires_at: notification.expires_at,
            is_error: notification.is_error,
        });
    }

    pub fn on_tick(&mut self) {
        self.tick_count += 1;
        if let Some(n) = &self.notification {
            if n.is_expired() {
                self.notification = None;
            }
        }
    }

    pub fn open_modal(&mut self, modal: Modal) {
        self.modal = Some(modal);
    }

    pub fn close_modal(&mut self) {
        self.modal = None;
    }

    pub fn bootstrap_servers(&self) -> String {
        self.current_cluster
            .as_ref()
            .and_then(|name| self.config.connections.get(name))
            .and_then(|c| c.properties.get("bootstrap.servers"))
            .cloned()
            .unwrap_or_default()
    }

    pub fn nav_snapshot(&self) -> NavSnapshot {
        NavSnapshot {
            screen: self.screen.clone(),
            topic_selected: self.topic_selected,
            topic_filter: self.topic_filter.clone(),
            filter_mode: self.filter_mode,
            message_browser: self.message_browser.clone(),
        }
    }

    pub fn nav_restore(&mut self, snap: NavSnapshot) {
        self.screen = snap.screen;
        self.topic_selected = snap.topic_selected;
        self.topic_filter = snap.topic_filter;
        self.filter_mode = snap.filter_mode;
        self.message_browser = snap.message_browser;
    }

    pub fn can_go_back(&self) -> bool {
        !self.nav_back.is_empty()
    }

    pub fn can_go_forward(&self) -> bool {
        !self.nav_forward.is_empty()
    }

    pub fn push_nav(&mut self) {
        self.nav_back.push(self.nav_snapshot());
        self.nav_forward.clear();
    }

    pub fn clear_nav(&mut self) {
        self.nav_back.clear();
        self.nav_forward.clear();
    }

    pub fn breadcrumb(&self) -> String {
        match self.screen {
            Screen::ClusterSelect => "选择 Cluster".to_string(),
            Screen::TopicList => "Topics".to_string(),
            Screen::MessageBrowser => {
                let topic = self
                    .message_browser
                    .as_ref()
                    .map(|b| b.topic.as_str())
                    .unwrap_or("?");
                format!(
                    "Topics › {}",
                    crate::ui::text::sanitize_display(topic)
                )
            }
        }
    }

    pub fn toolbar_cluster_label(&self) -> String {
        self.current_cluster
            .clone()
            .or_else(|| {
                self.cluster_names
                    .get(self.cluster_selected)
                    .cloned()
            })
            .unwrap_or_else(|| "未选择".to_string())
    }

    pub fn open_cluster_picker(&mut self) {
        let selected = self
            .current_cluster
            .as_ref()
            .and_then(|name| self.cluster_names.iter().position(|n| n == name))
            .unwrap_or(self.cluster_selected);
        self.open_modal(Modal::ClusterPicker { selected });
    }

    pub fn open_page_size_picker(&mut self, anchor: Rect) {
        let selected = PAGE_SIZE_OPTIONS
            .iter()
            .position(|s| *s == self.page_size)
            .unwrap_or(1);
        self.open_modal(Modal::PageSizePicker { selected, anchor });
    }
}
