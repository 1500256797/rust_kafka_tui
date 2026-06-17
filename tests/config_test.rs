use std::io::Write;
use tempfile::NamedTempFile;

use kafka_tui::config::{expand_env_vars, load_config_from_path};

// Re-export config for integration tests from binary crate
// Tests are in tests/ directory and use the binary's modules via a lib if needed.

#[test]
fn test_expand_env_vars() {
    std::env::set_var("KAFKA_TUI_TEST_VAR", "hello");
    assert_eq!(
        expand_env_vars("${KAFKA_TUI_TEST_VAR}").unwrap(),
        "hello"
    );
    assert_eq!(
        expand_env_vars("$KAFKA_TUI_TEST_VAR").unwrap(),
        "hello"
    );
}

#[test]
fn test_config_parse() {
    let yaml = r#"
connections:
  test:
    properties:
      bootstrap.servers: "localhost:9092"
    allow-produce: true
topic-data:
  page-size: 25
"#;
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    let config = load_config_from_path(file.path()).unwrap();
    assert_eq!(config.connections.len(), 1);
    assert_eq!(config.topic_data.page_size, 25);
}

#[test]
fn test_empty_connections_fails() {
    let yaml = "connections: {}";
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    assert!(load_config_from_path(file.path()).is_err());
}
