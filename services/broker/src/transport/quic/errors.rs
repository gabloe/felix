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
