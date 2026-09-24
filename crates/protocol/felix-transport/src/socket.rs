//! The UDP socket under an endpoint, and what the OS actually granted it.

use std::net::SocketAddr;

use anyhow::{Context, Result};

use crate::config::TransportConfig;

impl TransportConfig {
    pub(crate) fn bind_udp_socket(&self, addr: SocketAddr) -> Result<std::net::UdpSocket> {
        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
                .context("create UDP socket")?;
        // Large socket buffers absorb bursts at high message rates; drops here
        // surface as QUIC retransmits and latency spikes. Halve until the OS
        // accepts the size so hosts with low limits still work.
        let mut send_bytes = self.udp_send_buffer_bytes;
        while send_bytes >= 64 * 1024 && socket.set_send_buffer_size(send_bytes).is_err() {
            send_bytes /= 2;
        }
        let mut recv_bytes = self.udp_recv_buffer_bytes;
        while recv_bytes >= 64 * 1024 && socket.set_recv_buffer_size(recv_bytes).is_err() {
            recv_bytes /= 2;
        }
        socket.bind(&addr.into()).context("bind UDP socket")?;
        socket
            .set_nonblocking(true)
            .context("set UDP socket nonblocking")?;
        Ok(socket.into())
    }
}

/// The smaller of the socket's achieved send/receive buffers. Read back from
/// the socket rather than taken from config: Linux accepts an oversized
/// `SO_RCVBUF`/`SO_SNDBUF` and silently clamps it to `net.core.rmem_max` /
/// `wmem_max`, so the configured size says nothing about what was granted.
pub(crate) fn effective_udp_buffer_bytes(socket: &std::net::UdpSocket) -> usize {
    let socket = socket2::SockRef::from(socket);
    let send = socket.send_buffer_size().unwrap_or(0);
    let recv = socket.recv_buffer_size().unwrap_or(0);
    send.min(recv)
}

/// Say so when the OS granted far less socket buffer than was asked for.
///
/// Linux accepts an oversized `SO_RCVBUF`/`SO_SNDBUF` and silently clamps it to
/// `net.core.rmem_max` / `wmem_max`, which ship at around 208 KB. Against the
/// 8 MiB Felix asks for that is a fortieth, and the consequence is not an
/// error: bursts overflow the socket, the drops surface as QUIC retransmits,
/// and throughput is a fraction of what the host can do. Every perf session had
/// to raise these to 26 MiB before any other number meant anything.
///
/// Nothing here can fix it -- the limit belongs to the host -- so the only
/// useful thing is to stop it being silent. Once per endpoint, at `warn`,
/// naming the sysctls: a broker that is quietly at a fortieth of its capacity
/// should not look identical to one that is not.
pub(crate) fn warn_if_udp_buffers_were_clamped(socket: &std::net::UdpSocket, requested: usize) {
    let granted = effective_udp_buffer_bytes(socket);
    // Half is the threshold rather than any shortfall: the bind loop above
    // halves on rejection, so landing one step down is the mechanism working,
    // not the host being untuned.
    if granted == 0 || granted >= requested / 2 {
        return;
    }
    tracing::warn!(
        requested_bytes = requested,
        granted_bytes = granted,
        "the OS granted far less UDP socket buffer than requested; bursts will be \
         dropped at the socket and surface as QUIC retransmits. On Linux raise \
         net.core.rmem_max and net.core.wmem_max (perf sessions use 26 MiB)",
    );
}
