use felix_wire::Message;
use futures::{StreamExt, stream::FuturesUnordered};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, watch};

use crate::transport::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, AckWaiterResult, Outgoing, handle_ack_enqueue_result,
    send_outgoing_critical,
};
use crate::transport::quic::telemetry::{t_consume_instant, t_counter, t_histogram};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_ack_waiter_loop(
    mut ack_waiter_rx: mpsc::Receiver<AckWaiterMessage>,
    out_ack_tx_waiter: mpsc::Sender<Outgoing>,
    out_ack_depth_waiter: Arc<std::sync::atomic::AtomicUsize>,
    ack_throttle_tx_waiter: watch::Sender<bool>,
    ack_timeout_state_waiter: Arc<Mutex<AckTimeoutState>>,
    cancel_tx_waiter: watch::Sender<bool>,
    mut cancel_rx_waiter: watch::Receiver<bool>,
    ack_wait_timeout: Duration,
) {
    // Ack waiter task (only meaningful when ack_on_commit=true):
    // We decouple "publish committed" from the control stream read loop.
    // - Each acked publish creates a oneshot receiver that completes when the publish worker
    //   finishes the broker call.
    // - FuturesUnordered allows acks to be emitted as commits complete (out-of-order allowed).
    // - A semaphore bounds the number of in-flight waiters to avoid unbounded memory growth.
    // Correctness note: every waiter must drop its semaphore permit on *all* paths.
    let mut pending = FuturesUnordered::new();
    let mut closed = false;
    loop {
        tokio::select! {
            changed = cancel_rx_waiter.changed() => {
                if changed.is_err() || *cancel_rx_waiter.borrow() {
                    break;
                }
            }
            message = ack_waiter_rx.recv() => {
                match message {
                    Some(message) => {
                        let mut cancel_rx = cancel_rx_waiter.clone();
                        let fut = async move {
                            match message {
                                AckWaiterMessage::Publish {
                                    request_id,
                                    payload_len,
                                    start,
                                    response_rx,
                                    permit,
                                } => {
                                    let response = tokio::select! {
                                        _ = cancel_rx.changed() => return None,
                                        response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                    };
                                    drop(permit);
                                    match response {
                                        Ok(response) => Some(AckWaiterResult::Publish {
                                            request_id,
                                            payload_len,
                                            start,
                                            response,
                                        }),
                                        Err(_) => Some(AckWaiterResult::PublishTimeout {
                                            request_id,
                                            start,
                                        }),
                                    }
                                }
                                AckWaiterMessage::PublishBatch {
                                    request_id,
                                    payload_bytes,
                                    response_rx,
                                    permit,
                                } => {
                                    let response = tokio::select! {
                                        _ = cancel_rx.changed() => return None,
                                        response = tokio::time::timeout(ack_wait_timeout, response_rx) => response,
                                    };
                                    drop(permit);
                                    match response {
                                        Ok(response) => Some(AckWaiterResult::PublishBatch {
                                            request_id,
                                            payload_bytes,
                                            response,
                                        }),
                                        Err(_) => Some(AckWaiterResult::PublishBatchTimeout {
                                            request_id,
                                            payload_bytes,
                                        }),
                                    }
                                }
                            }
                        };
                        pending.push(fut);
                    }
                    None => {
                        closed = true;
                    }
                }
            }
            result = pending.next(), if !pending.is_empty() => {
                if let Some(result) = result.flatten() {
                    match result {
                        AckWaiterResult::Publish {
                            request_id,
                            payload_len,
                            start,
                            response,
                        } => {
                            t_consume_instant(start);
                            match response {
                                Ok(Ok(_)) => {
                                    t_counter!("felix_publish_requests_total", "result" => "ok")
                                        .increment(1);
                                    t_counter!("felix_publish_bytes_total")
                                        .increment(payload_len);
                                    #[cfg(feature = "telemetry")]
                                    {
                                        t_histogram!(
                                            "felix_publish_latency_ms",
                                            "mode" => "commit"
                                        )
                                        .record(start.elapsed().as_secs_f64() * 1000.0);
                                    }
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishOk { request_id }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Ok(Err(err)) => {
                                    t_counter!("felix_publish_requests_total", "result" => "error")
                                        .increment(1);
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishError {
                                                request_id,
                                                message: err.to_string(),
                                            }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Err(_) => {
                                    t_counter!("felix_publish_requests_total", "result" => "error")
                                        .increment(1);
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishError {
                                                request_id,
                                                message: "publish worker dropped response".to_string(),
                                            }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                            }
                        }
                        AckWaiterResult::PublishTimeout { request_id, start } => {
                            t_consume_instant(start);
                            t_counter!(
                                "felix_publish_requests_total",
                                "result" => "error"
                            )
                            .increment(1);
                            t_counter!("felix_broker_ack_waiter_timeout_total")
                                .increment(1);
                            #[cfg(feature = "telemetry")]
                            {
                                t_histogram!(
                                    "felix_publish_latency_ms",
                                    "mode" => "commit"
                                )
                                .record(start.elapsed().as_secs_f64() * 1000.0);
                            }
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx_waiter,
                                    &out_ack_depth_waiter,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx_waiter,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "publish commit timeout".to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state_waiter,
                                &ack_throttle_tx_waiter,
                                &cancel_tx_waiter,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                        }
                        AckWaiterResult::PublishBatch {
                            request_id,
                            payload_bytes,
                            response,
                        } => {
                            match response {
                                Ok(Ok(_)) => {
                                    t_counter!(
                                        "felix_publish_requests_total",
                                        "result" => "ok"
                                    )
                                    .increment(1);
                                    for bytes in &payload_bytes {
                                        t_counter!("felix_publish_bytes_total")
                                            .increment(*bytes as u64);
                                    }
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishOk { request_id }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Ok(Err(err)) => {
                                    t_counter!("felix_publish_requests_total", "result" => "error")
                                        .increment(1);
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishError {
                                                request_id,
                                                message: err.to_string(),
                                            }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                                Err(_) => {
                                    t_counter!("felix_publish_requests_total", "result" => "error")
                                        .increment(1);
                                    if let Err(err) = handle_ack_enqueue_result(
                                        send_outgoing_critical(
                                            &out_ack_tx_waiter,
                                            &out_ack_depth_waiter,
                                            "felix_broker_out_ack_depth",
                                            &ack_throttle_tx_waiter,
                                            Outgoing::Message(Message::PublishError {
                                                request_id,
                                                message: "publish batch worker dropped response".to_string(),
                                            }),
                                        )
                                        .await,
                                        &ack_timeout_state_waiter,
                                        &ack_throttle_tx_waiter,
                                        &cancel_tx_waiter,
                                    )
                                    .await
                                    {
                                        tracing::info!(error = %err, "ack enqueue failed");
                                    }
                                }
                            }
                        }
                        AckWaiterResult::PublishBatchTimeout {
                            request_id,
                            payload_bytes,
                        } => {
                            t_counter!(
                                "felix_publish_requests_total",
                                "result" => "error"
                            )
                            .increment(1);
                            t_counter!("felix_broker_ack_waiter_timeout_total")
                                .increment(1);
                            for bytes in &payload_bytes {
                                t_counter!("felix_publish_bytes_total")
                                    .increment(*bytes as u64);
                            }
                            if let Err(err) = handle_ack_enqueue_result(
                                send_outgoing_critical(
                                    &out_ack_tx_waiter,
                                    &out_ack_depth_waiter,
                                    "felix_broker_out_ack_depth",
                                    &ack_throttle_tx_waiter,
                                    Outgoing::Message(Message::PublishError {
                                        request_id,
                                        message: "publish commit timeout".to_string(),
                                    }),
                                )
                                .await,
                                &ack_timeout_state_waiter,
                                &ack_throttle_tx_waiter,
                                &cancel_tx_waiter,
                            )
                            .await
                            {
                                tracing::info!(error = %err, "ack enqueue failed");
                            }
                        }
                    }
                }
            }
        }
        if closed && pending.is_empty() {
            break;
        }
    }
}
