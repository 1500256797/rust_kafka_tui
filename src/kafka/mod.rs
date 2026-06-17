use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{ClusterConfig, TopicDataConfig};
use crate::error::KafkaError;

pub mod admin;
pub mod consumer;
pub mod producer;
pub mod types;

pub use admin::KafkaAdmin;
pub use consumer::KafkaConsumer;
pub use producer::{KafkaProducer, ProduceRequest, ProduceResult};
pub use types::*;

pub struct KafkaClient {
    pub admin: KafkaAdmin,
    pub consumer: KafkaConsumer,
    pub producer: Option<KafkaProducer>,
    pub cluster_name: String,
    pub allow_produce: bool,
    pub bootstrap_servers: String,
}

impl KafkaClient {
    pub fn connect(
        name: &str,
        config: &ClusterConfig,
        topic_data: &TopicDataConfig,
    ) -> Result<Self, KafkaError> {
        let producer = if config.allow_produce {
            Some(
                KafkaProducer::new(&config.properties)
                    .map_err(|e| KafkaError::Client(e.to_string()))?,
            )
        } else {
            None
        };

        let bootstrap_servers = config
            .properties
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_default();

        Ok(Self {
            admin: KafkaAdmin::new(&config.properties)?,
            consumer: KafkaConsumer::new(&config.properties, topic_data.poll_timeout_ms)?,
            producer,
            cluster_name: name.to_string(),
            allow_produce: config.allow_produce,
            bootstrap_servers,
        })
    }
}

pub fn build_client_config(properties: &HashMap<String, String>) -> rdkafka::ClientConfig {
    let mut config = rdkafka::ClientConfig::new();
    for (k, v) in properties {
        config.set(k, v);
    }
    // Avoid librdkafka writing log lines to stderr while the TUI owns the terminal.
    config.set("log_level", "0");
    config.set("log.connection.close", "false");
    config
}

pub type SharedKafkaClient = Arc<KafkaClient>;
