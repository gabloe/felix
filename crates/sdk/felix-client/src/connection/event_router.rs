//! Event stream router for subscription uni streams.
//!
//! Accepts incoming uni streams from the server, decodes the subscription id
//! from the EventStreamHello frame, and hands the stream to the waiting
//! subscription task.
//!
//! # Design notes
//! The router maintains pending maps for streams and registrations to handle
//! out-of-order arrivals while enforcing an upper bound on queued state.//!
//! The protocol rule this rests on: the first frame on an event stream is
//! `EventStreamHello { subscription_id }`, and nothing after it names the
//! subscription again, so the stream itself is the binding.

use std::collections::HashMap;

use bytes::BytesMut;
use felix_transport::QuicConnection;
use felix_wire::Message;
use quinn::RecvStream;
use tokio::sync::{mpsc, oneshot};

use crate::config::EVENT_ROUTER_QUEUE_DEPTH;
use crate::frame_io::read_message_with_limit;

pub(crate) enum EventRouterCommand {
    Register {
        subscription_id: u64,
        response: oneshot::Sender<anyhow::Result<RecvStream>>,
    },
}

#[cfg(test)]
pub(crate) fn spawn_event_router(connection: QuicConnection) -> mpsc::Sender<EventRouterCommand> {
    spawn_event_router_with_config(
        connection,
        crate::config::DEFAULT_EVENT_ROUTER_MAX_PENDING,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
}

pub(crate) fn spawn_event_router_with_config(
    connection: QuicConnection,
    max_pending: usize,
    max_frame_bytes: usize,
) -> mpsc::Sender<EventRouterCommand> {
    let (tx, rx) = mpsc::channel(EVENT_ROUTER_QUEUE_DEPTH);
    tokio::spawn(run_event_router(
        connection,
        rx,
        max_pending,
        max_frame_bytes,
    ));
    tx
}

pub(crate) async fn run_event_router(
    connection: QuicConnection,
    mut rx: mpsc::Receiver<EventRouterCommand>,
    max_pending: usize,
    max_frame_bytes: usize,
) {
    let mut pending_waiters: HashMap<u64, oneshot::Sender<anyhow::Result<RecvStream>>> =
        HashMap::new();
    let mut pending_streams: HashMap<u64, RecvStream> = HashMap::new();
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);

    // Both maps are capped by `max_pending` (a registration the server never
    // answers, or a stream nobody registers for). Entries do not time out, so
    // under that cap a stale one stays until the connection closes.
    loop {
        tokio::select! {
            command = rx.recv() => {
                match command {
                    Some(EventRouterCommand::Register { subscription_id, response }) => {
                        if pending_waiters.len() + pending_streams.len() >= max_pending {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "event router pending limit reached ({max_pending}); refusing registration"
                            )));
                            continue;
                        }
                        if let Some(stream) = pending_streams.remove(&subscription_id) {
                            let _ = response.send(Ok(stream));
                            continue;
                        }
                        if pending_waiters.contains_key(&subscription_id) {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "duplicate subscription registration for {subscription_id}"
                            )));
                            continue;
                        }
                        pending_waiters.insert(subscription_id, response);
                    }
                    None => {
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!("event stream router closed")));
                        }
                        break;
                    }
                }
            }
            stream = connection.accept_uni() => {
                let mut recv = match stream {
                    Ok(recv) => recv,
                    Err(err) => {
                        let message = err.to_string();
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!(message.clone())));
                        }
                        break;
                    }
                };
                if pending_waiters.len() + pending_streams.len() >= max_pending {
                    // Best-effort: drop the stream to cap memory growth under overload.
                    continue;
                }
                let subscription_id = match read_message_with_limit(
                    &mut recv,
                    &mut frame_scratch,
                    max_frame_bytes,
                ).await {
                    Ok(Some(Message::EventStreamHello { subscription_id })) => subscription_id,
                    Ok(Some(_)) => continue,
                    Ok(None) => continue,
                    Err(_) => continue,
                };
                if let Some(waiter) = pending_waiters.remove(&subscription_id) {
                    let _ = waiter.send(Ok(recv));
                    continue;
                }
                if pending_streams.contains_key(&subscription_id) {
                    continue;
                }
                pending_streams.insert(subscription_id, recv);
            }
        }
    }
}

#[cfg(test)]
mod tests;
