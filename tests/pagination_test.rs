use kafka_tui::kafka::types::{KafkaMessage, MessageTimestamp, TimestampKind};
use kafka_tui::service::pagination::k_way_merge;

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
fn test_k_way_merge_three_partitions() {
    let p0 = vec![make_msg(0, 0, 100), make_msg(0, 1, 300)];
    let p1 = vec![make_msg(1, 0, 200)];
    let p2 = vec![make_msg(2, 0, 150), make_msg(2, 1, 250)];
    let ids = vec![0, 1, 2];

    let (merged, _) = k_way_merge(&[p0, p1, p2], &ids, 4);
    assert_eq!(merged.len(), 4);
    assert_eq!(
        merged.iter().map(|m| m.timestamp.millis).collect::<Vec<_>>(),
        vec![100, 150, 200, 250]
    );
}

#[test]
fn test_k_way_merge_insufficient_messages() {
    let p0 = vec![make_msg(0, 0, 100)];
    let ids = vec![0];
    let (merged, _) = k_way_merge(&[p0], &ids, 10);
    assert_eq!(merged.len(), 1);
}
