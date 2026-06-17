use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use lru::LruCache;

use crate::kafka::types::KafkaMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BrowseMode {
    Merged,
    SinglePartition { partition: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageDirection {
    First,
    Last,
    Next,
    Prev,
    GoToOffset(i64),
    GoToTimestamp(i64),
    Refresh,
}

#[derive(Debug, Clone, Default)]
pub struct PartitionCursor {
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone)]
pub struct MessagePage {
    pub messages: Vec<KafkaMessage>,
    pub cursors: Vec<PartitionCursor>,
    pub start_offset: i64,
    pub page_index: u64,
    pub last_page_timestamps: Vec<i64>,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct PageKey {
    pub topic: String,
    pub mode_hash: u64,
    pub page_index: u64,
    pub start_offset: i64,
    pub page_size: usize,
}

impl PageKey {
    pub fn new(
        topic: &str,
        mode: BrowseMode,
        page_index: u64,
        start_offset: i64,
        page_size: usize,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        mode.hash(&mut hasher);
        Self {
            topic: topic.to_string(),
            mode_hash: hasher.finish(),
            page_index,
            start_offset,
            page_size,
        }
    }
}

pub struct PageCache {
    cache: LruCache<PageKey, MessagePage>,
}

impl PageCache {
    pub fn new(max_pages: usize) -> Self {
        let cap = std::num::NonZeroUsize::new(max_pages.max(1)).unwrap();
        Self {
            cache: LruCache::new(cap),
        }
    }

    pub fn get(&mut self, key: &PageKey) -> Option<MessagePage> {
        self.cache.get(key).cloned()
    }

    pub fn put(&mut self, key: PageKey, page: MessagePage) {
        self.cache.put(key, page);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// 多路归并，取全局 timestamp 最小的 count 条
pub fn k_way_merge(
    partitions: &[Vec<KafkaMessage>],
    partition_ids: &[i32],
    count: usize,
) -> (Vec<KafkaMessage>, Vec<PartitionCursor>) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut heap: BinaryHeap<Reverse<(i64, i64, usize, usize)>> = BinaryHeap::new();

    for (p_idx, msgs) in partitions.iter().enumerate() {
        if !msgs.is_empty() {
            let ts = msgs[0].timestamp.millis;
            let offset = msgs[0].offset;
            heap.push(Reverse((ts, offset, p_idx, 0)));
        }
    }

    let mut result = Vec::with_capacity(count);
    let mut cursors: Vec<PartitionCursor> = partition_ids
        .iter()
        .map(|&p| PartitionCursor {
            partition: p,
            next_offset: 0,
        })
        .collect();

    while result.len() < count && !heap.is_empty() {
        let Some(Reverse((_ts, _offset, p_idx, m_idx))) = heap.pop() else {
            break;
        };
        let msg = partitions[p_idx][m_idx].clone();
        cursors[p_idx].next_offset = msg.offset + 1;
        result.push(msg);

        if m_idx + 1 < partitions[p_idx].len() {
            let next = &partitions[p_idx][m_idx + 1];
            heap.push(Reverse((
                next.timestamp.millis,
                next.offset,
                p_idx,
                m_idx + 1,
            )));
        }
    }

    (result, cursors)
}

/// 反向归并，取全局 timestamp 最大的 count 条
pub fn k_way_merge_reverse(
    partitions: &[Vec<KafkaMessage>],
    partition_ids: &[i32],
    count: usize,
) -> (Vec<KafkaMessage>, Vec<PartitionCursor>) {
    use std::collections::BinaryHeap;

    let mut heap: BinaryHeap<(i64, i64, usize, usize)> = BinaryHeap::new();

    for (p_idx, msgs) in partitions.iter().enumerate() {
        if !msgs.is_empty() {
            let last_idx = msgs.len() - 1;
            let ts = msgs[last_idx].timestamp.millis;
            let offset = msgs[last_idx].offset;
            heap.push((ts, offset, p_idx, last_idx));
        }
    }

    let mut result = Vec::with_capacity(count);
    let mut cursors: Vec<PartitionCursor> = partition_ids
        .iter()
        .map(|&p| PartitionCursor {
            partition: p,
            next_offset: 0,
        })
        .collect();

    while result.len() < count && !heap.is_empty() {
        let Some((_ts, _offset, p_idx, m_idx)) = heap.pop() else {
            break;
        };
        let msg = partitions[p_idx][m_idx].clone();
        cursors[p_idx].next_offset = msg.offset;
        result.push(msg);

        if m_idx > 0 {
            let prev = &partitions[p_idx][m_idx - 1];
            heap.push((prev.timestamp.millis, prev.offset, p_idx, m_idx - 1));
        }
    }

    result.reverse();
    (result, cursors)
}

pub fn total_messages_estimate(partitions: &[crate::kafka::types::PartitionInfo]) -> u64 {
    partitions
        .iter()
        .map(|p| (p.high_watermark - p.log_start_offset).max(0) as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::types::{MessageTimestamp, TimestampKind};

    fn make_msg(partition: i32, offset: i64, millis: i64) -> KafkaMessage {
        KafkaMessage {
            topic: "test".into(),
            partition,
            offset,
            timestamp: MessageTimestamp {
                millis,
                kind: TimestampKind::CreateTime,
            },
            key: None,
            value: Some(format!("v{}", offset).into_bytes()),
            headers: vec![],
        }
    }

    #[test]
    fn test_k_way_merge_basic() {
        let p0 = vec![make_msg(0, 0, 100), make_msg(0, 1, 300)];
        let p1 = vec![make_msg(1, 0, 200), make_msg(1, 1, 400)];
        let ids = vec![0, 1];

        let (merged, cursors) = k_way_merge(&[p0, p1], &ids, 3);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].timestamp.millis, 100);
        assert_eq!(merged[1].timestamp.millis, 200);
        assert_eq!(merged[2].timestamp.millis, 300);
        assert_eq!(cursors[0].next_offset, 2);
    }

    #[test]
    fn test_k_way_merge_empty() {
        let (merged, _) = k_way_merge(&[], &[], 10);
        assert!(merged.is_empty());
    }
}
