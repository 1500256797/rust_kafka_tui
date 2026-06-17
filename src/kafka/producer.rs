use std::collections::HashMap;
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::message::OwnedHeaders;

use crate::error::ProduceError;
use crate::kafka::build_client_config;
use crate::kafka::types::KafkaMessage;

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct ProduceResult {
    pub partition: i32,
    pub offset: i64,
}

pub struct KafkaProducer {
    producer: FutureProducer,
    timeout: Duration,
}

impl KafkaProducer {
    pub fn new(properties: &HashMap<String, String>) -> Result<Self, ProduceError> {
        let config = build_client_config(properties);

        let producer: FutureProducer = config
            .create()
            .map_err(|e| ProduceError::Send(e.to_string()))?;

        Ok(Self {
            producer,
            timeout: Duration::from_secs(30),
        })
    }

    pub async fn send(&self, req: ProduceRequest) -> Result<ProduceResult, ProduceError> {
        let mut owned_headers = OwnedHeaders::new();
        for (key, value) in &req.headers {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key,
                value: Some(value),
            });
        }

        let mut record = FutureRecord::to(&req.topic)
            .payload(&req.value)
            .headers(owned_headers);

        if let Some(ref key) = req.key {
            record = record.key(key);
        }
        if let Some(p) = req.partition {
            record = record.partition(p);
        }

        let delivery = self
            .producer
            .send(record, Timeout::After(self.timeout))
            .await
            .map_err(|(e, _)| ProduceError::Send(e.to_string()))?;

        Ok(ProduceResult {
            partition: delivery.0,
            offset: delivery.1,
        })
    }

    pub fn from_message(
        msg: &KafkaMessage,
        target_topic: &str,
        target_partition: Option<i32>,
    ) -> ProduceRequest {
        ProduceRequest {
            topic: target_topic.to_string(),
            partition: target_partition,
            key: msg.key.clone(),
            value: msg.value.clone().unwrap_or_default(),
            headers: msg.headers.clone(),
        }
    }
}
