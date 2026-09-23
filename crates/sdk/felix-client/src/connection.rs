//! Where a client's pooled connections go, and how streams on them open.
//!
//! Cache operations are latency-sensitive and a slow response head-of-line
//! blocks whatever is queued behind it on the same stream, so a client pools
//! *connections* and opens several streams on each, every stream with a
//! single writer. Subscriptions are round-robined across the event
//! connections, and each still gets a server-opened uni stream of its own.
//!
//! Every stream authenticates on open (`handshake`), and the event
//! connections each run a router that hands those uni streams to the
//! subscriptions waiting for them (`event_router`).

mod event_router;
mod handshake;

pub(crate) use event_router::{EventRouterCommand, spawn_event_router_with_config};
pub(crate) use handshake::{Credentials, Negotiated};

use std::net::SocketAddr;

use felix_transport::QuicConnection;

/// Where this client's pooled connections should go, given what the broker
/// said about its listeners.
///
/// `dialled` always comes first and is always present, even if the broker did
/// not name its port: it is the address that demonstrably works, and a pool
/// that abandoned it on the strength of an advertisement would be trusting a
/// claim it has not tested.
///
/// Only the *port* is taken from the advertisement. The host stays the one
/// already connected to, so an `AuthOk` cannot move a client to a different
/// machine -- that would be a redirect, which is a much larger claim than "I
/// also listen here" and belongs to `NotLeader`.
pub(crate) fn listener_targets(dialled: SocketAddr, ports: &[u16]) -> Vec<SocketAddr> {
    let mut targets = vec![dialled];
    for port in ports {
        let mut candidate = dialled;
        candidate.set_port(*port);
        if !targets.contains(&candidate) {
            targets.push(candidate);
        }
    }
    targets
}

/// The listener one pool connection at `index` should dial, noting it in
/// `listeners` the first time any pool lands on it.
///
/// Shared by the publish, cache and event pools: each spreads its
/// connections across `targets` the same way and needs the same bookkeeping
/// for `Client::listeners_in_use`.
pub(crate) fn pool_target(
    targets: &[SocketAddr],
    index: usize,
    listeners: &mut Vec<SocketAddr>,
) -> SocketAddr {
    let target = targets[index % targets.len()];
    if !listeners.contains(&target) {
        listeners.push(target);
    }
    target
}

/// Periodic path stats for client-side connections, mirroring the broker's
/// `FELIX_CONN_STATS_MS` logging. The client is the sender on the publish path,
/// so its cwnd/rtt is invisible from broker-side stats. Off unless set.
pub(crate) fn spawn_conn_stats_logger(connection: &QuicConnection, role: &'static str) {
    let Some(interval_ms) = std::env::var("FELIX_CONN_STATS_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    else {
        return;
    };
    let connection = connection.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            ticker.tick().await;
            if connection.close_reason().is_some() {
                break;
            }
            let stats = connection.stats();
            tracing::info!(
                role,
                conn = connection.info().id.0,
                mtu = stats.path.current_mtu,
                cwnd = stats.path.cwnd,
                rtt_us = stats.path.rtt.as_micros() as u64,
                congestion_events = stats.path.congestion_events,
                lost_packets = stats.path.lost_packets,
                udp_tx_bytes = stats.udp_tx.bytes,
                udp_tx_datagrams = stats.udp_tx.datagrams,
                tx_data_blocked = stats.frame_tx.data_blocked,
                tx_stream_data_blocked = stats.frame_tx.stream_data_blocked,
                "client quic connection path stats"
            );
        }
    });
}

#[cfg(test)]
mod tests;
