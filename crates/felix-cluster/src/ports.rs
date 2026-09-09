//! Picking addresses that are free right now.
//!
//! Every process the harness starts binds a port it was told to bind, so the
//! harness has to choose them. It asks the OS for an ephemeral port, notes it,
//! and closes the socket — which leaves a window where something else could take
//! it. That window is why callers get a fresh port per attempt rather than a
//! cached one, and why start-up retries.
use std::net::{SocketAddr, TcpListener, UdpSocket};

use anyhow::{Context, Result};

/// A free TCP port on loopback, for the HTTP listeners.
pub fn free_tcp() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral TCP port")?;
    listener.local_addr().context("read TCP port")
}

/// A free UDP port on loopback, for the QUIC listeners.
pub fn free_udp() -> Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0").context("bind ephemeral UDP port")?;
    socket.local_addr().context("read UDP port")
}
