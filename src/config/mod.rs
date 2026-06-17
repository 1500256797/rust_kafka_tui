use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::Parser;
use serde::Deserialize;

use crate::error::ConfigError;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub connections: HashMap<String, ClusterConfig>,
    #[serde(default)]
    pub topic: TopicDefaults,
    #[serde(default, rename = "topic-data")]
    pub topic_data: TopicDataConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub properties: HashMap<String, String>,
    #[serde(default, rename = "schema-registry")]
    pub schema_registry: Option<SchemaRegistryConfig>,
    #[serde(default = "default_allow_produce", rename = "allow-produce")]
    pub allow_produce: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaRegistryConfig {
    pub url: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TopicDefaults {
    #[serde(default = "default_replication")]
    pub replication: i32,
    #[serde(default = "default_partition")]
    pub partition: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicDataConfig {
    #[serde(default = "default_page_size", rename = "page-size")]
    pub page_size: usize,
    #[serde(default = "default_poll_timeout", rename = "poll-timeout-ms")]
    pub poll_timeout_ms: u64,
    #[serde(default = "default_max_msg_len", rename = "max-message-length")]
    pub max_message_length: usize,
    #[serde(default = "default_format", rename = "default-format")]
    pub default_format: MessageFormat,
    #[serde(default = "default_cache_pages", rename = "cache-pages")]
    pub cache_pages: usize,
}

impl Default for TopicDataConfig {
    fn default() -> Self {
        Self {
            page_size: default_page_size(),
            poll_timeout_ms: default_poll_timeout(),
            max_message_length: default_max_msg_len(),
            default_format: default_format(),
            cache_pages: default_cache_pages(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MessageFormat {
    #[default]
    Auto,
    Json,
    Raw,
    Avro,
}

fn default_allow_produce() -> bool {
    true
}

fn default_replication() -> i32 {
    3
}

fn default_partition() -> i32 {
    3
}

fn default_page_size() -> usize {
    50
}

fn default_poll_timeout() -> u64 {
    1000
}

fn default_max_msg_len() -> usize {
    1_000_000
}

fn default_format() -> MessageFormat {
    MessageFormat::Auto
}

fn default_cache_pages() -> usize {
    10
}

#[derive(Parser, Debug)]
#[command(name = "kafka-tui", about = "Terminal Kafka browser and producer")]
pub struct Cli {
    #[arg(short, long, help = "配置文件路径")]
    pub config: Option<PathBuf>,

    #[arg(long, help = "启动时直接连接指定 Cluster")]
    pub cluster: Option<String>,

    #[arg(short, long, help = "启用 debug 日志")]
    pub verbose: bool,
}

pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".config/kafka-tui/config.yaml"))
        .unwrap_or_else(|| PathBuf::from("config.yaml"))
}

pub fn resolve_config_path(cli: &Cli) -> PathBuf {
    if let Some(path) = &cli.config {
        return path.clone();
    }
    let default = default_config_path();
    if default.exists() {
        return default;
    }
    let local = PathBuf::from("config.yaml");
    if local.exists() {
        return local;
    }
    default
}

pub fn load_config(cli: &Cli) -> Result<(AppConfig, PathBuf), ConfigError> {
    let path = resolve_config_path(cli);
    let config = load_config_from_path(&path)?;
    Ok((config, path))
}

pub fn load_config_from_path(path: &Path) -> Result<AppConfig, ConfigError> {
    if !path.exists() {
        return Err(ConfigError::NotFound(path.display().to_string()));
    }
    let content = std::fs::read_to_string(path)?;
    let expanded = expand_env_vars_in_str(&content)?;
    let mut config: AppConfig = serde_yaml::from_str(&expanded)?;
    validate_config(&mut config)?;
    Ok(config)
}

fn validate_config(config: &mut AppConfig) -> Result<(), ConfigError> {
    if config.connections.is_empty() {
        return Err(ConfigError::EmptyConnections);
    }
    if config.topic_data.page_size == 0 {
        return Err(ConfigError::InvalidPageSize);
    }
    for (name, cluster) in &mut config.connections {
        expand_env_vars_in_map(&mut cluster.properties)?;
        if let Some(sr) = &mut cluster.schema_registry {
            expand_env_vars_in_map(&mut sr.properties)?;
        }
        if !cluster
            .properties
            .contains_key("bootstrap.servers")
        {
            return Err(ConfigError::MissingBootstrapServers(name.clone()));
        }
    }
    Ok(())
}

fn expand_env_vars_in_map(map: &mut HashMap<String, String>) -> Result<(), ConfigError> {
    let keys: Vec<String> = map.keys().cloned().collect();
    for key in keys {
        if let Some(value) = map.get(&key) {
            let expanded = expand_env_vars(value)?;
            map.insert(key, expanded);
        }
    }
    Ok(())
}

fn expand_env_vars_in_str(input: &str) -> Result<String, ConfigError> {
    expand_env_vars(input)
}

/// 支持 "${KAFKA_PASSWORD}" 和 "$KAFKA_PASSWORD" 两种写法
pub fn expand_env_vars(input: &str) -> Result<String, ConfigError> {
    let mut result = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' {
            if i + 1 < chars.len() && chars[i + 1] == '{' {
                // ${VAR} format
                if let Some(end) = find_closing_brace(&chars, i + 2) {
                    let var_name: String = chars[i + 2..end].iter().collect();
                    let value = std::env::var(&var_name)
                        .map_err(|_| ConfigError::MissingEnvVar(var_name))?;
                    result.push_str(&value);
                    i = end + 1;
                    continue;
                }
            } else {
                // $VAR format
                let start = i + 1;
                let mut end = start;
                while end < chars.len()
                    && (chars[end].is_ascii_uppercase()
                        || chars[end].is_ascii_digit()
                        || chars[end] == '_')
                {
                    end += 1;
                }
                if end > start {
                    let var_name: String = chars[start..end].iter().collect();
                    let value = std::env::var(&var_name)
                        .map_err(|_| ConfigError::MissingEnvVar(var_name))?;
                    result.push_str(&value);
                    i = end;
                    continue;
                }
            }
        }
        result.push(chars[i]);
        i += 1;
    }
    Ok(result)
}

fn find_closing_brace(chars: &[char], start: usize) -> Option<usize> {
    for (idx, &c) in chars.iter().enumerate().skip(start) {
        if c == '}' {
            return Some(idx);
        }
    }
    None
}

impl AppConfig {
    pub fn cluster_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.connections.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn get_cluster(&self, name: &str) -> Result<&ClusterConfig, ConfigError> {
        self.connections
            .get(name)
            .ok_or_else(|| ConfigError::ClusterNotFound(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars_brace_format() {
        std::env::set_var("TEST_KAFKA_PASS", "secret123");
        let result = expand_env_vars("password: ${TEST_KAFKA_PASS}").unwrap();
        assert_eq!(result, "password: secret123");
    }

    #[test]
    fn test_expand_env_vars_dollar_format() {
        std::env::set_var("TEST_KAFKA_USER", "admin");
        let result = expand_env_vars("user: $TEST_KAFKA_USER").unwrap();
        assert_eq!(result, "user: admin");
    }

    #[test]
    fn test_missing_env_var() {
        let result = expand_env_vars("${NONEXISTENT_VAR_XYZ123}");
        assert!(result.is_err());
    }
}
