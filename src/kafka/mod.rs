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
        if k == "sasl.jaas.config" {
            // librdkafka 不支持 Java 风格的 sasl.jaas.config，
            // 自动解析出 username/password 转换为它支持的属性。
            if !properties.contains_key("sasl.username") {
                if let Some(user) = extract_jaas_field(v, "username") {
                    config.set("sasl.username", user);
                }
            }
            if !properties.contains_key("sasl.password") {
                if let Some(pass) = extract_jaas_field(v, "password") {
                    config.set("sasl.password", pass);
                }
            }
            // 若未显式指定机制，从 LoginModule 类型推断（仅能确定 PLAIN）。
            if !properties.contains_key("sasl.mechanism")
                && !properties.contains_key("sasl.mechanisms")
                && v.contains("PlainLoginModule")
            {
                config.set("sasl.mechanism", "PLAIN");
            }
            continue;
        }
        config.set(k, v);
    }
    // Avoid librdkafka writing log lines to stderr while the TUI owns the terminal.
    config.set("log_level", "0");
    config.set("log.connection.close", "false");
    config
}

/// 从 Java JAAS 配置字符串中提取字段值，例如：
/// `org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";`
fn extract_jaas_field(jaas: &str, field: &str) -> Option<String> {
    let needle = format!("{}=", field);
    let start = jaas.find(&needle)? + needle.len();
    let rest = jaas[start..].trim_start();

    if let Some(stripped) = rest.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else if let Some(stripped) = rest.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        Some(stripped[..end].to_string())
    } else {
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(rest.len());
        Some(rest[..end].to_string())
    }
}

pub type SharedKafkaClient = Arc<KafkaClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_jaas_quoted_fields() {
        let jaas = r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="myuser" password="mypass";"#;
        assert_eq!(extract_jaas_field(jaas, "username").as_deref(), Some("myuser"));
        assert_eq!(extract_jaas_field(jaas, "password").as_deref(), Some("mypass"));
    }

    #[test]
    fn extract_jaas_password_with_special_chars() {
        let jaas = r#"org.apache.kafka.common.security.scram.ScramLoginModule required username="u" password="p@ss w0rd!";"#;
        assert_eq!(
            extract_jaas_field(jaas, "password").as_deref(),
            Some("p@ss w0rd!")
        );
    }

    #[test]
    fn jaas_config_converted_to_sasl_fields() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";"#
                .to_string(),
        );

        let config = build_client_config(&props);
        assert_eq!(config.get("sasl.username"), Some("u"));
        assert_eq!(config.get("sasl.password"), Some("p"));
        assert_eq!(config.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(config.get("sasl.jaas.config"), None);
    }
}
