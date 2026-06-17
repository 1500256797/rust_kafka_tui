use crate::error::ProduceError;
use crate::kafka::{KafkaClient, KafkaMessage, ProduceRequest, ProduceResult};

pub struct ProduceService;

impl ProduceService {
    pub fn validate(client: &KafkaClient, _req: &ProduceRequest) -> Result<(), ProduceError> {
        if client.producer.is_none() {
            return Err(ProduceError::ReadOnlyMode);
        }
        Ok(())
    }

    pub async fn send(client: &KafkaClient, req: ProduceRequest) -> Result<ProduceResult, ProduceError> {
        Self::validate(client, &req)?;
        let producer = client
            .producer
            .as_ref()
            .ok_or(ProduceError::ReadOnlyMode)?;
        producer.send(req).await
    }

    pub fn from_message(
        msg: &KafkaMessage,
        target_topic: &str,
        target_partition: Option<i32>,
    ) -> ProduceRequest {
        crate::kafka::producer::KafkaProducer::from_message(msg, target_topic, target_partition)
    }

    pub fn needs_confirmation(target_topic: &str) -> bool {
        let lower = target_topic.to_lowercase();
        !lower.ends_with("-test")
            && !lower.ends_with("_test")
            && !lower.ends_with("-dev")
            && !lower.ends_with("_dev")
            && !lower.contains("test-")
            && !lower.contains("dev-")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needs_confirmation() {
        assert!(!ProduceService::needs_confirmation("orders-test"));
        assert!(!ProduceService::needs_confirmation("orders_dev"));
        assert!(ProduceService::needs_confirmation("orders-prod"));
    }
}
