use crate::kafka::{KafkaClient, TopicInfo};

pub struct TopicService;

impl TopicService {
    pub fn list(client: &KafkaClient, show_internal: bool) -> Result<Vec<TopicInfo>, crate::error::KafkaError> {
        let topics = client.admin.list_topics()?;
        if show_internal {
            Ok(topics)
        } else {
            Ok(topics.into_iter().filter(|t| !t.is_internal).collect())
        }
    }

    pub fn filter(topics: &[TopicInfo], query: &str) -> Vec<TopicInfo> {
        if query.is_empty() {
            return topics.to_vec();
        }
        let q = query.to_lowercase();
        topics
            .iter()
            .filter(|t| t.name.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }
}
