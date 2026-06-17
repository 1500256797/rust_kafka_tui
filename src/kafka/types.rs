use rdkafka::message::{Headers, Message, Timestamp};
use rdkafka::Message as _;

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub is_internal: bool,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub log_start_offset: i64,
    pub high_watermark: i64,
}

#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: MessageTimestamp,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageTimestamp {
    pub millis: i64,
    pub kind: TimestampKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimestampKind {
    CreateTime,
    LogAppendTime,
    NotAvailable,
}

impl KafkaMessage {
    pub fn from_borrowed<M: Message>(msg: &M) -> Self {
        let timestamp = match msg.timestamp() {
            Timestamp::CreateTime(ms) => MessageTimestamp {
                millis: ms,
                kind: TimestampKind::CreateTime,
            },
            Timestamp::LogAppendTime(ms) => MessageTimestamp {
                millis: ms,
                kind: TimestampKind::LogAppendTime,
            },
            Timestamp::NotAvailable => MessageTimestamp {
                millis: 0,
                kind: TimestampKind::NotAvailable,
            },
        };

        let mut headers = Vec::new();
        if let Some(hdrs) = msg.headers() {
            for i in 0..hdrs.count() {
                let header = hdrs.get(i);
                let key = header.key.to_string();
                let value = header.value.unwrap_or(&[]).to_vec();
                headers.push((key, value));
            }
        }

        Self {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp,
            key: msg.key().map(|k| k.to_vec()),
            value: msg.payload().map(|v| v.to_vec()),
            headers,
        }
    }
}
