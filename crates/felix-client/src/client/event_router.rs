// Event stream router for subscription uni streams.
use bytes::BytesMut;
use felix_transport::QuicConnection;
use felix_wire::Message;
use quinn::RecvStream;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::config::{EVENT_ROUTER_QUEUE_DEPTH, runtime_config};
use crate::wire::read_message;

pub(crate) enum EventRouterCommand {
    Register {
        subscription_id: u64,
        response: oneshot::Sender<anyhow::Result<RecvStream>>,
    },
}

pub(crate) fn spawn_event_router(connection: QuicConnection) -> mpsc::Sender<EventRouterCommand> {
    let (tx, rx) = mpsc::channel(EVENT_ROUTER_QUEUE_DEPTH);
    tokio::spawn(run_event_router(connection, rx));
    tx
}

pub(crate) async fn run_event_router(
    connection: QuicConnection,
    mut rx: mpsc::Receiver<EventRouterCommand>,
) {
    let mut pending_waiters: HashMap<u64, oneshot::Sender<anyhow::Result<RecvStream>>> =
        HashMap::new();
    let mut pending_streams: HashMap<u64, RecvStream> = HashMap::new();
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);

    let max_pending = runtime_config().event_router_max_pending;

    // POTENTIAL ISSUE:
    // `pending_waiters` and `pending_streams` can grow without bound if:
    // - the server sends EventStreamHello for subscription_ids the client never registers, or
    // - the client registers but the server never sends the stream.
    // We should probably:
    // - add a maximum map size + eviction (LRU), and/or
    // - add per-entry timeouts and periodically purge stale entries.
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
                let subscription_id = match read_message(&mut recv, &mut frame_scratch).await {
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
