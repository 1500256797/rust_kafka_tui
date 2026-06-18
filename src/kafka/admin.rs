use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};

use crate::error::KafkaError;
use crate::kafka::build_client_config;
use crate::kafka::types::{PartitionInfo, TopicInfo};

pub struct KafkaAdmin {
    client: AdminClient<DefaultClientContext>,
    consumer: StreamConsumer,
}

impl KafkaAdmin {
    pub fn new(properties: &HashMap<String, String>) -> Result<Self, KafkaError> {
        let mut config = build_client_config(properties);
        config.set("group.id", "kafka-tui-admin");
        config.set("enable.auto.commit", "false");

        let client: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        Ok(Self { client, consumer })
    }

    pub fn list_topics(&self) -> Result<Vec<TopicInfo>, KafkaError> {
        let metadata = self
            .consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        let mut topics = Vec::new();
        for topic in metadata.topics() {
            if topic.error().is_some() {
                continue;
            }
            let name = topic.name().to_string();
            let is_internal = name.starts_with("__") || name.starts_with('_');
            // 列表阶段只需要分区数量等元信息，不在这里拉每个分区的 watermark
            // （否则会对每个分区发起一次同步网络往返，topic 多时极慢）。
            // 真正的 watermark 在进入某个 topic 时由 get_watermarks 单独拉取。
            let partitions = topic
                .partitions()
                .iter()
                .map(|p| PartitionInfo {
                    id: p.id(),
                    leader: p.leader(),
                    log_start_offset: 0,
                    high_watermark: 0,
                })
                .collect();

            topics.push(TopicInfo {
                name,
                partitions,
                is_internal,
            });
        }

        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    pub fn get_watermarks(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| KafkaError::TopicNotFound(topic.to_string()))?;

        let mut partitions = Vec::new();
        for p in topic_meta.partitions() {
            let (low, high) = self
                .consumer
                .fetch_watermarks(topic, p.id(), Duration::from_secs(10))
                .map_err(|e| KafkaError::Client(e.to_string()))?;

            partitions.push(PartitionInfo {
                id: p.id(),
                leader: p.leader(),
                log_start_offset: low,
                high_watermark: high,
            });
        }

        Ok(partitions)
    }

    pub fn ping(&self) -> Result<(), KafkaError> {
        self.consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        Ok(())
    }

    pub fn consumer(&self) -> &StreamConsumer {
        &self.consumer
    }
}
