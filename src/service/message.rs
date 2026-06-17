use crate::config::{MessageFormat, SchemaRegistryConfig, TopicDataConfig};
use crate::error::{DecodeError, KafkaError};
use crate::kafka::{KafkaClient, KafkaMessage};
use crate::schema::{DecodedMessage, PayloadFormat, SchemaService};
use crate::service::pagination::{
    k_way_merge, k_way_merge_reverse, BrowseMode, MessagePage, PageCache, PageDirection, PageKey,
    PartitionCursor, total_messages_estimate,
};

#[derive(Debug, Clone)]
pub struct DisplayMessage {
    pub raw: KafkaMessage,
    pub formatted_value: String,
    pub preview: String,
    pub format: PayloadFormat,
}

#[derive(Debug, Clone)]
pub struct MessageBrowserState {
    pub topic: String,
    pub mode: BrowseMode,
    pub messages: Vec<DisplayMessage>,
    pub cursors: Vec<PartitionCursor>,
    pub start_offset: i64,
    pub page_index: u64,
    pub current_page: u64,
    pub total_pages: u64,
    pub total_messages_estimate: u64,
    pub loading: bool,
    pub selected: usize,
    pub detail_expanded: bool,
    pub has_prev: bool,
    pub has_next: bool,
}

pub struct MessageService {
    page_cache: PageCache,
    schema: Option<SchemaService>,
    topic_data: TopicDataConfig,
    // persisted pagination state
    current_cursors: Vec<PartitionCursor>,
    current_page_index: u64,
    current_start_offset: i64,
    last_page_timestamps: Vec<i64>,
}

impl MessageService {
    pub fn new(topic_data: &TopicDataConfig, schema_config: Option<&SchemaRegistryConfig>) -> Self {
        Self {
            page_cache: PageCache::new(topic_data.cache_pages),
            schema: schema_config.map(SchemaService::new),
            topic_data: topic_data.clone(),
            current_cursors: Vec::new(),
            current_page_index: 0,
            current_start_offset: 0,
            last_page_timestamps: Vec::new(),
        }
    }

    pub fn update_schema(&mut self, schema_config: Option<&SchemaRegistryConfig>) {
        self.schema = schema_config.map(SchemaService::new);
    }

    pub fn fetch_page(
        &mut self,
        client: &KafkaClient,
        topic: &str,
        mode: BrowseMode,
        direction: PageDirection,
        page_size: usize,
    ) -> Result<MessageBrowserState, KafkaError> {
        if matches!(direction, PageDirection::First) {
            self.reset_pagination();
        }
        let partitions = client.admin.get_watermarks(topic)?;
        let total_est = total_messages_estimate(&partitions);

        let page = match mode {
            BrowseMode::Merged => {
                self.fetch_merged_page(client, topic, &partitions, direction, page_size)?
            }
            BrowseMode::SinglePartition { partition } => {
                self.fetch_single_page(client, topic, partition, &partitions, direction, page_size)?
            }
        };

        let messages: Vec<DisplayMessage> = page
            .messages
            .iter()
            .map(|m| self.format_message_sync(m, self.topic_data.default_format))
            .collect();

        let has_next = match mode {
            BrowseMode::Merged => page.cursors.iter().any(|c| {
                partitions
                    .iter()
                    .find(|p| p.id == c.partition)
                    .map(|p| c.next_offset < p.high_watermark)
                    .unwrap_or(false)
            }),
            BrowseMode::SinglePartition { partition } => {
                let fetched = page.messages.len();
                if fetched >= page_size {
                    partitions
                        .iter()
                        .find(|p| p.id == partition)
                        .map(|p| page.start_offset + (fetched as i64) < p.high_watermark)
                        .unwrap_or(false)
                } else {
                    false
                }
            }
        };

        let has_prev = page.page_index > 0 || page.start_offset > 0;

        let mut total_pages = if total_est == 0 {
            1
        } else {
            total_est.div_ceil(page_size as u64).max(1)
        };
        let current_page = if page.page_index == u64::MAX {
            total_pages
        } else {
            page.page_index + 1
        };
        if has_next && page.page_index != u64::MAX {
            total_pages = total_pages.max(current_page + 1);
        }

        Ok(MessageBrowserState {
            topic: topic.to_string(),
            mode,
            messages,
            cursors: page.cursors,
            start_offset: page.start_offset,
            page_index: page.page_index,
            current_page,
            total_pages,
            total_messages_estimate: total_est,
            loading: false,
            selected: 0,
            detail_expanded: true,
            has_prev,
            has_next,
        })
    }

    fn fetch_merged_page(
        &mut self,
        client: &KafkaClient,
        topic: &str,
        partitions: &[crate::kafka::PartitionInfo],
        direction: PageDirection,
        page_size: usize,
    ) -> Result<MessagePage, KafkaError> {
        let partition_ids: Vec<i32> = partitions.iter().map(|p| p.id).collect();

        match direction {
            PageDirection::First => {
                self.current_page_index = 0;
                self.current_cursors = partitions
                    .iter()
                    .map(|p| PartitionCursor {
                        partition: p.id,
                        next_offset: p.log_start_offset,
                    })
                    .collect();
            }
            PageDirection::Next => {
                self.current_page_index += 1;
            }
            PageDirection::Prev => {
                if self.current_page_index > 0 {
                    self.current_page_index -= 1;
                }
            }
            PageDirection::Last => {
                // fetch from end: start near high watermark
                self.current_cursors = partitions
                    .iter()
                    .map(|p| PartitionCursor {
                        partition: p.id,
                        next_offset: (p.high_watermark - page_size as i64).max(p.log_start_offset),
                    })
                    .collect();
                self.current_page_index = u64::MAX;
            }
            PageDirection::GoToTimestamp(ts) => {
                let offsets = client
                    .consumer
                    .offsets_for_timestamp(topic, &partition_ids, ts)?;
                self.current_cursors = offsets
                    .into_iter()
                    .map(|(p, o)| PartitionCursor {
                        partition: p,
                        next_offset: o,
                    })
                    .collect();
                self.current_page_index = 0;
            }
            PageDirection::GoToOffset(_) => {}
            PageDirection::Refresh => {}
        }

        let cache_key = PageKey::new(
            topic,
            BrowseMode::Merged,
            self.current_page_index,
            0,
            page_size,
        );
        if !matches!(direction, PageDirection::Refresh) {
            if let Some(cached) = self.page_cache.get(&cache_key) {
                return Ok(cached);
            }
        }

        let assignments: Vec<(i32, i64)> = self
            .current_cursors
            .iter()
            .map(|c| (c.partition, c.next_offset))
            .collect();

        let fetched = client
            .consumer
            .fetch_multi_partition(topic, &assignments, page_size)?;

        let (messages, new_cursors) = if matches!(direction, PageDirection::Prev | PageDirection::Last) {
            k_way_merge_reverse(&fetched, &partition_ids, page_size)
        } else {
            k_way_merge(&fetched, &partition_ids, page_size)
        };

        self.current_cursors = new_cursors.clone();
        self.last_page_timestamps = messages
            .iter()
            .map(|m| m.timestamp.millis)
            .collect();

        let page = MessagePage {
            messages,
            cursors: new_cursors,
            start_offset: 0,
            page_index: self.current_page_index,
            last_page_timestamps: self.last_page_timestamps.clone(),
        };

        self.page_cache.put(cache_key, page.clone());
        Ok(page)
    }

    fn fetch_single_page(
        &mut self,
        client: &KafkaClient,
        topic: &str,
        partition: i32,
        partitions: &[crate::kafka::PartitionInfo],
        direction: PageDirection,
        page_size: usize,
    ) -> Result<MessagePage, KafkaError> {
        let part_info = partitions
            .iter()
            .find(|p| p.id == partition)
            .ok_or_else(|| KafkaError::Client(format!("partition {} not found", partition)))?;

        match direction {
            PageDirection::First => {
                self.current_start_offset = part_info.log_start_offset;
                self.current_page_index = 0;
            }
            PageDirection::Next => {
                self.current_start_offset += page_size as i64;
                self.current_page_index += 1;
            }
            PageDirection::Prev => {
                self.current_start_offset =
                    (self.current_start_offset - page_size as i64).max(part_info.log_start_offset);
                if self.current_page_index > 0 {
                    self.current_page_index -= 1;
                }
            }
            PageDirection::Last => {
                self.current_start_offset =
                    (part_info.high_watermark - page_size as i64).max(part_info.log_start_offset);
            }
            PageDirection::GoToOffset(offset) => {
                self.current_start_offset = offset.max(part_info.log_start_offset);
                self.current_page_index = 0;
            }
            PageDirection::GoToTimestamp(ts) => {
                let offsets = client
                    .consumer
                    .offsets_for_timestamp(topic, &[partition], ts)?;
                self.current_start_offset = offsets
                    .first()
                    .map(|(_, o)| *o)
                    .unwrap_or(part_info.log_start_offset);
                self.current_page_index = 0;
            }
            PageDirection::Refresh => {}
        }

        let cache_key = PageKey::new(
            topic,
            BrowseMode::SinglePartition { partition },
            self.current_page_index,
            self.current_start_offset,
            page_size,
        );
        if !matches!(direction, PageDirection::Refresh) {
            if let Some(cached) = self.page_cache.get(&cache_key) {
                return Ok(cached);
            }
        }

        let messages = client.consumer.fetch_from_offset(
            topic,
            partition,
            self.current_start_offset,
            page_size,
        )?;

        let page = MessagePage {
            messages,
            cursors: vec![PartitionCursor {
                partition,
                next_offset: self.current_start_offset + page_size as i64,
            }],
            start_offset: self.current_start_offset,
            page_index: self.current_page_index,
            last_page_timestamps: vec![],
        };

        self.page_cache.put(cache_key, page.clone());
        Ok(page)
    }

    pub fn format_message_sync(
        &self,
        msg: &KafkaMessage,
        format: MessageFormat,
    ) -> DisplayMessage {
        let (formatted_value, payload_format) = self.format_payload(msg, format);
        let preview = truncate_preview(&formatted_value, 80);
        DisplayMessage {
            raw: msg.clone(),
            formatted_value,
            preview,
            format: payload_format,
        }
    }

    pub async fn format_message(
        &mut self,
        msg: &KafkaMessage,
        format: MessageFormat,
    ) -> DisplayMessage {
        let (formatted_value, payload_format) = self.format_payload_async(msg, format).await;
        let preview = truncate_preview(&formatted_value, 80);
        DisplayMessage {
            raw: msg.clone(),
            formatted_value,
            preview,
            format: payload_format,
        }
    }

    fn format_payload(&self, msg: &KafkaMessage, format: MessageFormat) -> (String, PayloadFormat) {
        let payload = match &msg.value {
            Some(v) => v.as_slice(),
            None => return ("(null)".to_string(), PayloadFormat::Raw),
        };

        match format {
            MessageFormat::Avro => {
                if SchemaService::is_confluent_avro(payload) {
                    return (
                        format!("[Avro schema id {}]", SchemaService::schema_id(payload).unwrap_or(0)),
                        PayloadFormat::Avro,
                    );
                }
            }
            MessageFormat::Json => {
                if let Ok(s) = format_json(payload) {
                    return (s, PayloadFormat::Json);
                }
            }
            MessageFormat::Raw => {
                if let Ok(s) = String::from_utf8(payload.to_vec()) {
                    return (s, PayloadFormat::Raw);
                }
            }
            MessageFormat::Auto => {}
        }

        // auto mode priority
        if self.schema.is_some() && SchemaService::is_confluent_avro(payload) {
            return (
                format!("[Avro - async decode on select, schema id {}]", SchemaService::schema_id(payload).unwrap_or(0)),
                PayloadFormat::Avro,
            );
        }
        if let Ok(s) = format_json(payload) {
            return (s, PayloadFormat::Json);
        }
        if let Ok(s) = String::from_utf8(payload.to_vec()) {
            return (s, PayloadFormat::Raw);
        }
        (hex_dump(payload), PayloadFormat::Hex)
    }

    async fn format_payload_async(
        &mut self,
        msg: &KafkaMessage,
        format: MessageFormat,
    ) -> (String, PayloadFormat) {
        let payload = match &msg.value {
            Some(v) => v.as_slice(),
            None => return ("(null)".to_string(), PayloadFormat::Raw),
        };

        let try_avro = matches!(format, MessageFormat::Auto | MessageFormat::Avro)
            && self.schema.is_some()
            && SchemaService::is_confluent_avro(payload);

        if try_avro {
            if let Some(ref mut schema) = self.schema {
                match schema.decode_value(payload).await {
                    Ok(DecodedMessage { json, .. }) => return (json, PayloadFormat::Avro),
                    Err(DecodeError::NotAvro) => {}
                    Err(e) => {
                        return (
                            format!("[Avro 解码失败: {}]\n--- Raw (hex) ---\n{}", e, hex_dump(payload)),
                            PayloadFormat::Hex,
                        );
                    }
                }
            }
        }

        if matches!(format, MessageFormat::Auto | MessageFormat::Json) {
            if let Ok(s) = format_json(payload) {
                return (s, PayloadFormat::Json);
            }
        }
        if matches!(format, MessageFormat::Auto | MessageFormat::Raw) {
            if let Ok(s) = String::from_utf8(payload.to_vec()) {
                return (s, PayloadFormat::Raw);
            }
        }
        (hex_dump(payload), PayloadFormat::Hex)
    }

    pub fn reset_pagination(&mut self) {
        self.current_cursors.clear();
        self.current_page_index = 0;
        self.current_start_offset = 0;
        self.last_page_timestamps.clear();
        self.page_cache.clear();
    }
}

fn format_json(payload: &[u8]) -> Result<String, ()> {
    let s = std::str::from_utf8(payload).map_err(|_| ())?;
    let v: serde_json::Value = serde_json::from_str(s).map_err(|_| ())?;
    serde_json::to_string_pretty(&v).map_err(|_| ())
}

fn hex_dump(data: &[u8]) -> String {
    data.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_preview(s: &str, max: usize) -> String {
    use crate::ui::text::sanitize_display;
    let flat: String = sanitize_display(s).lines().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        let truncated: String = flat.chars().take(max).collect();
        format!("{}...", truncated)
    }
}
