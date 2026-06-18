use kafka_tui::app::{
    handle_kafka_event, handle_key, handle_mouse, App, CommandSender, KafkaCommand, KafkaEvent,
    Notification,
};
use kafka_tui::clipboard::ClipboardService;
use kafka_tui::config::{load_config, Cli};
use kafka_tui::kafka::KafkaClient;
use kafka_tui::service::{MessageService, TopicService};

use std::io::{self, stdout};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use crossterm::event::{self, Event, KeyEventKind, MouseEventKind};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    kafka_tui::terminal::install_panic_hook();

    let cli = Cli::parse();
    init_logging(&cli)?;

    let (config, config_path) = load_config(&cli).map_err(|e| {
        eprintln!("配置错误: {}", e);
        e
    })?;

    let mut terminal = init_terminal()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let config_clone = config.clone();
    let config_path_clone = config_path.clone();
    runtime.spawn(kafka_worker(cmd_rx, event_tx, config_clone, config_path_clone));

    let mut app = App::new(config, config_path, &cli);
    let clipboard = ClipboardService::new();

    if let Some(cluster) = cli.cluster.clone() {
        app.connection_status = kafka_tui::app::ConnectionStatus::Connecting;
        app.current_cluster = Some(cluster.clone());
        app.show_notification(Notification::info(
            format!("正在连接 {}...", cluster),
            Duration::from_secs(3),
        ));
        let _ = cmd_tx.send(KafkaCommand::Connect { cluster });
    }

    let mut event_rx = event_rx;
    let result = run_app(&mut terminal, &mut app, &mut event_rx, &cmd_tx, &clipboard);

    restore_terminal()?;
    result
}

fn init_logging(cli: &Cli) -> Result<()> {
    let log_dir = dirs::home_dir()
        .map(|h| h.join(".config/kafka-tui/logs"))
        .unwrap_or_else(|| PathBuf::from("./logs"));

    std::fs::create_dir_all(&log_dir)?;

    let filter = if cli.verbose {
        "kafka_tui=debug,rdkafka=warn"
    } else {
        "kafka_tui=info,rdkafka=warn"
    };

    let file_appender = tracing_appender::rolling::daily(&log_dir, "kafka-tui.log");
    tracing_subscriber::fmt()
        .with_writer(file_appender)
        .with_env_filter(EnvFilter::new(filter))
        .init();

    Ok(())
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    let mut out = stdout();
    kafka_tui::terminal::enter_terminal(&mut out)?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend)?;
    let _ = terminal.clear();
    Ok(terminal)
}

fn restore_terminal() -> Result<()> {
    kafka_tui::terminal::leave_terminal(&mut stdout()).map_err(Into::into)
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    event_rx: &mut mpsc::UnboundedReceiver<KafkaEvent>,
    cmd_tx: &CommandSender,
    clipboard: &ClipboardService,
) -> Result<()> {
    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| kafka_tui::ui::render(f, app))?;

        if app.should_quit {
            return Ok(());
        }

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)? {
            loop {
                match event::read()? {
                    Event::Key(key) if key.kind == KeyEventKind::Press => {
                        handle_key(app, key, cmd_tx, clipboard);
                    }
                    Event::Mouse(mouse)
                        if !matches!(mouse.kind, MouseEventKind::Moved) =>
                    {
                        handle_mouse(app, mouse, cmd_tx, clipboard);
                    }
                    _ => {}
                }
                if !event::poll(Duration::ZERO)? {
                    break;
                }
            }
        }

        while let Ok(kafka_event) = event_rx.try_recv() {
            handle_kafka_event(app, kafka_event, cmd_tx);
        }

        if last_tick.elapsed() >= tick_rate {
            app.on_tick();
            last_tick = Instant::now();
        }
    }
}

async fn kafka_worker(
    mut cmd_rx: mpsc::UnboundedReceiver<KafkaCommand>,
    event_tx: mpsc::UnboundedSender<KafkaEvent>,
    mut config: kafka_tui::config::AppConfig,
    config_path: PathBuf,
) {
    let mut client: Option<Arc<KafkaClient>> = None;
    let mut message_service: Option<Arc<Mutex<MessageService>>> = None;
    let mut current_cluster: Option<String> = None;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            KafkaCommand::Connect { cluster } => {
                match connect_cluster(&config, &cluster) {
                    Ok(c) => {
                        let allow_produce = c.allow_produce;
                        let schema_cfg = config
                            .connections
                            .get(&cluster)
                            .and_then(|c| c.schema_registry.as_ref());
                        message_service = Some(Arc::new(Mutex::new(MessageService::new(
                            &config.topic_data,
                            schema_cfg,
                        ))));
                        current_cluster = Some(cluster.clone());
                        client = Some(Arc::new(c));
                        let _ = event_tx.send(KafkaEvent::Connected {
                            cluster,
                            allow_produce,
                        });
                        if let Some(c) = &client {
                            match TopicService::list(c, false) {
                                Ok(topics) => {
                                    let _ = event_tx.send(KafkaEvent::TopicsLoaded { topics });
                                }
                                Err(e) => {
                                    let _ = event_tx.send(KafkaEvent::TopicsFailed {
                                        error: e.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = event_tx.send(KafkaEvent::ConnectionFailed {
                            error: e.to_string(),
                        });
                    }
                }
            }
            KafkaCommand::Disconnect => {
                client = None;
                message_service = None;
                current_cluster = None;
            }
            KafkaCommand::FetchTopics => {
                if let Some(c) = &client {
                    let c = Arc::clone(c);
                    let result =
                        tokio::task::spawn_blocking(move || TopicService::list(&c, false)).await;

                    match result {
                        Ok(Ok(topics)) => {
                            let _ = event_tx.send(KafkaEvent::TopicsLoaded { topics });
                        }
                        Ok(Err(e)) => {
                            let _ = event_tx.send(KafkaEvent::TopicsFailed {
                                error: e.to_string(),
                            });
                        }
                        Err(e) => {
                            let _ = event_tx.send(KafkaEvent::TopicsFailed {
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }
            KafkaCommand::FetchMessagePage {
                topic,
                mode,
                direction,
                page_size,
            } => {
                if let (Some(c), Some(ms)) = (&client, &message_service) {
                    let c = Arc::clone(c);
                    let ms = Arc::clone(ms);
                    let result = tokio::task::spawn_blocking(move || {
                        let mut service = ms
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        service.fetch_page(&c, &topic, mode, direction, page_size)
                    })
                    .await;

                    match result {
                        Ok(Ok(state)) => {
                            let _ = event_tx.send(KafkaEvent::MessagePageLoaded { state });
                        }
                        Ok(Err(e)) => {
                            let _ = event_tx.send(KafkaEvent::MessagePageFailed {
                                error: e.to_string(),
                            });
                        }
                        Err(e) => {
                            let _ = event_tx.send(KafkaEvent::MessagePageFailed {
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }
            KafkaCommand::Produce(req) => {
                if let Some(c) = &client {
                    let c = Arc::clone(c);
                    match kafka_tui::service::ProduceService::send(&c, req).await {
                        Ok(r) => {
                            let _ = event_tx.send(KafkaEvent::MessageProduced { result: r });
                        }
                        Err(e) => {
                            let _ = event_tx.send(KafkaEvent::ProduceFailed {
                                error: e.to_string(),
                            });
                        }
                    }
                } else {
                    let _ = event_tx.send(KafkaEvent::ProduceFailed {
                        error: "未连接或只读模式，无法发送".to_string(),
                    });
                }
            }
            KafkaCommand::ReloadConfig => {
                match kafka_tui::config::load_config_from_path(&config_path) {
                    Ok(new_config) => {
                        config = new_config;
                        if let Some(cluster) = &current_cluster {
                            if let Some(cluster_cfg) = config.connections.get(cluster) {
                                let schema_cfg = cluster_cfg.schema_registry.as_ref();
                                if let Some(ms) = &message_service {
                                    ms.lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .update_schema(schema_cfg);
                                }
                                match connect_cluster(&config, cluster) {
                                    Ok(c) => {
                                        let allow_produce = c.allow_produce;
                                        client = Some(Arc::new(c));
                                        let _ = event_tx.send(KafkaEvent::Connected {
                                            cluster: cluster.clone(),
                                            allow_produce,
                                        });
                                    }
                                    Err(e) => {
                                        let _ = event_tx.send(KafkaEvent::ConfigReloadFailed {
                                            error: e.to_string(),
                                        });
                                        continue;
                                    }
                                }
                            }
                        }
                        let _ = event_tx.send(KafkaEvent::ConfigReloaded);
                    }
                    Err(e) => {
                        let _ = event_tx.send(KafkaEvent::ConfigReloadFailed {
                            error: e.to_string(),
                        });
                    }
                }
            }
        }
    }
}

fn connect_cluster(
    config: &kafka_tui::config::AppConfig,
    cluster: &str,
) -> Result<KafkaClient, kafka_tui::error::KafkaError> {
    let cluster_cfg = config
        .get_cluster(cluster)
        .map_err(|e| kafka_tui::error::KafkaError::Client(e.to_string()))?;
    KafkaClient::connect(cluster, cluster_cfg, &config.topic_data)
}
