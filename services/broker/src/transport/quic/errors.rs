// Error types and helpers for QUIC adapter enqueue failures.
use crate::transport::quic::telemetry::t_counter;

#[derive(Debug)]
pub(crate) enum AckEnqueueError {
    Full,
    Closed,
    Timeout,
}

pub(crate) fn record_ack_enqueue_failure_metrics(reason: &'static str) {
    t_counter!(
        "felix_broker_control_stream_closed_total",
        "reason" => reason
    )
    .increment(1);
    tracing::info!(reason = %reason, "closing control stream due to ack enqueue failure");
}

pub(crate) fn ack_enqueue_reason(err: &AckEnqueueError) -> &'static str {
    match err {
        AckEnqueueError::Full => "ack_queue_full",
        AckEnqueueError::Closed => "ack_queue_closed",
        AckEnqueueError::Timeout => "ack_queue_timeout",
    }
}

pub(crate) fn record_ack_enqueue_failure(err: AckEnqueueError) -> anyhow::Error {
    let reason = ack_enqueue_reason(&err);
    record_ack_enqueue_failure_metrics(reason);
    anyhow::anyhow!("closing control stream: {reason}")
}

#[cfg(test)]
mod tests {
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
}
