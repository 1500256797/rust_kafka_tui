use std::collections::HashMap;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use rdkafka::error::KafkaError as RdKafkaError;

use crate::error::KafkaError;
use crate::kafka::build_client_config;
use crate::kafka::types::KafkaMessage;

pub struct KafkaConsumer {
    consumer: BaseConsumer,
    poll_timeout: Duration,
}

impl KafkaConsumer {
    pub fn new(properties: &HashMap<String, String>, poll_timeout_ms: u64) -> Result<Self, KafkaError> {
        let mut config = build_client_config(properties);
        config.set("group.id", "kafka-tui-consumer");
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "earliest");
        // 读到分区末尾时立即返回 PartitionEOF，避免拉不满一页时空轮询傻等。
        config.set("enable.partition.eof", "true");

        let consumer: BaseConsumer = config
            .create()
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        Ok(Self {
            consumer,
            poll_timeout: Duration::from_millis(poll_timeout_ms),
        })
    }

    pub fn fetch_from_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        count: usize,
    ) -> Result<Vec<KafkaMessage>, KafkaError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(start_offset))
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        self.consumer
            .assign(&tpl)
            .map_err(|e| KafkaError::Client(e.to_string()))?;
        self.consumer
            .seek(
                topic,
                partition,
                Offset::Offset(start_offset),
                self.poll_timeout,
            )
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        self.poll_messages(count)
    }

    pub fn fetch_from_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
        count: usize,
    ) -> Result<Vec<KafkaMessage>, KafkaError> {
        let offsets = self.offsets_for_timestamp(topic, &[partition], timestamp_ms)?;
        let start = offsets
            .first()
            .map(|(_, o)| *o)
            .unwrap_or(0);
        self.fetch_from_offset(topic, partition, start, count)
    }

    pub fn offsets_for_timestamp(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp_ms: i64,
    ) -> Result<Vec<(i32, i64)>, KafkaError> {
        let mut tpl = TopicPartitionList::new();
        for &p in partitions {
            tpl.add_partition_offset(topic, p, Offset::Offset(timestamp_ms))
                .map_err(|e| KafkaError::Client(e.to_string()))?;
        }

        let result = self
            .consumer
            .offsets_for_times(tpl, Timeout::After(self.poll_timeout))
            .map_err(|e| KafkaError::Client(e.to_string()))?;

        let mut offsets = Vec::new();
        for &p in partitions {
            if let Some(part) = result.find_partition(topic, p) {
                let raw = part.offset().to_raw().unwrap_or(0);
                offsets.push((p, raw.max(0)));
            }
        }
        Ok(offsets)
    }

    pub fn fetch_multi_partition(
        &self,
        topic: &str,
        assignments: &[(i32, i64)],
        count_per_partition: usize,
    ) -> Result<Vec<Vec<KafkaMessage>>, KafkaError> {
        let mut all = Vec::with_capacity(assignments.len());
        for &(partition, start_offset) in assignments {
            let msgs = self.fetch_from_offset(topic, partition, start_offset, count_per_partition)?;
            all.push(msgs);
        }
        Ok(all)
    }

    fn poll_messages(&self, count: usize) -> Result<Vec<KafkaMessage>, KafkaError> {
        let mut messages = Vec::with_capacity(count);
        // poll_timeout 作为整次拉取的总预算（兜底），而不是每次 poll 的等待时长。
        // 正常情况下读到分区末尾会收到 PartitionEOF 立即返回，不会等满。
        let deadline = Instant::now() + self.poll_timeout;
        let unit = Duration::from_millis(50);

        while messages.len() < count {
            match self.consumer.poll(unit) {
                Some(Ok(msg)) => {
                    messages.push(KafkaMessage::from_borrowed(&msg));
                }
                Some(Err(RdKafkaError::PartitionEOF(_))) => break,
                Some(Err(e)) => return Err(KafkaError::Client(e.to_string())),
                None => {
                    if Instant::now() >= deadline {
                        break;
                    }
                }
            }
        }
        Ok(messages)
    }
}
