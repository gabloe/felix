use felix_wire::AckMode;

use super::make_publisher;
use crate::publish::PublishSharding;

#[tokio::test]
async fn publish_fails_when_pool_empty() {
    let publisher = make_publisher(PublishSharding::RoundRobin, 0);
    let err = publisher
        .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
        .await
        .expect_err("empty pool");
    assert!(err.to_string().contains("publish pool is empty"));
}

#[tokio::test]
async fn publish_and_finish_success() {
    let publisher = make_publisher(PublishSharding::RoundRobin, 1);
    publisher
        .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
        .await
        .expect("publish");
    publisher
        .publish("t", "ns", "s", b"payload".to_vec(), AckMode::PerMessage)
        .await
        .expect("publish ack");
    publisher.finish().await.expect("finish");
}

#[tokio::test]
async fn publish_batch_and_binary_paths() {
    let publisher = make_publisher(PublishSharding::HashStream, 2);
    publisher
        .publish_batch(
            "t",
            "ns",
            "s",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::PerBatch,
        )
        .await
        .expect("publish batch");
    publisher
        .publish_batch_binary("t", "ns", "s", &[b"a".to_vec(), b"b".to_vec()])
        .await
        .expect("publish batch binary");
    publisher.finish().await.expect("finish");
}

#[tokio::test]
#[cfg(feature = "telemetry")]
async fn publish_records_enqueue_timings_when_sampling() {
    crate::timings::enable_collection(1);
    let publisher = make_publisher(PublishSharding::RoundRobin, 1);
    publisher
        .publish("t", "ns", "s", b"payload".to_vec(), AckMode::None)
        .await
        .expect("publish");
    publisher
        .publish_batch(
            "t",
            "ns",
            "s",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::PerBatch,
        )
        .await
        .expect("publish batch");
    publisher
        .publish_batch_binary("t", "ns", "s", &[b"a".to_vec(), b"b".to_vec()])
        .await
        .expect("publish batch binary");
    publisher.finish().await.expect("finish");
}
