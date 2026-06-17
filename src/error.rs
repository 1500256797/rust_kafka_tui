use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("配置文件不存在: {0}")]
    NotFound(String),

    #[error("YAML 解析失败: {0}")]
    Parse(#[from] serde_yaml::Error),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("connections 不能为空")]
    EmptyConnections,

    #[error("cluster '{0}' 缺少 bootstrap.servers")]
    MissingBootstrapServers(String),

    #[error("cluster '{0}' 不存在")]
    ClusterNotFound(String),

    #[error("环境变量未设置: {0}")]
    MissingEnvVar(String),

    #[error("page_size 必须大于 0")]
    InvalidPageSize,
}

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Kafka 客户端错误: {0}")]
    Client(String),

    #[error("未连接")]
    NotConnected,

    #[error("Topic 不存在: {0}")]
    TopicNotFound(String),
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("非 Avro 格式")]
    NotAvro,

    #[error("Avro 解码失败: {0}")]
    Decode(String),

    #[error("Schema Registry 错误: {0}")]
    SchemaRegistry(String),
}

#[derive(Error, Debug)]
pub enum ProduceError {
    #[error("只读模式，禁止写入")]
    ReadOnlyMode,

    #[error("发送失败: {0}")]
    Send(String),
}

#[derive(Error, Debug)]
pub enum CopyError {
    #[error("剪贴板不可用")]
    Unavailable,

    #[error("复制失败: {0}")]
    Failed(String),
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("配置错误: {0}")]
    Config(#[from] ConfigError),

    #[error("Kafka 错误: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Schema 解码错误: {0}")]
    Schema(#[from] DecodeError),

    #[error("只读模式，禁止写入")]
    ReadOnly,

    #[error("剪贴板不可用")]
    Clipboard(#[from] CopyError),

    #[error("终端错误: {0}")]
    Terminal(String),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
