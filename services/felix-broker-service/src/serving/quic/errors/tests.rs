use super::*;

#[test]
fn ack_enqueue_reason_returns_correct_strings() {
    assert_eq!(ack_enqueue_reason(&AckEnqueueError::Full), "ack_queue_full");
    assert_eq!(
        ack_enqueue_reason(&AckEnqueueError::Closed),
        "ack_queue_closed"
    );
    assert_eq!(
        ack_enqueue_reason(&AckEnqueueError::Timeout),
        "ack_queue_timeout"
    );
}

#[test]
fn record_ack_enqueue_failure_returns_error() {
    let err = record_ack_enqueue_failure(AckEnqueueError::Full);
    assert!(err.to_string().contains("ack_queue_full"));

    let err = record_ack_enqueue_failure(AckEnqueueError::Closed);
    assert!(err.to_string().contains("ack_queue_closed"));

    let err = record_ack_enqueue_failure(AckEnqueueError::Timeout);
    assert!(err.to_string().contains("ack_queue_timeout"));
}

#[test]
fn record_ack_enqueue_failure_metrics_does_not_panic() {
    // Just ensure it doesn't panic
    record_ack_enqueue_failure_metrics("test_reason");
}
