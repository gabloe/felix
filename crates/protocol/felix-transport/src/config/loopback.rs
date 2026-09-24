//! The loopback MTU pin: a larger, guaranteed datagram size for connections
//! whose peer is on the same host.

use super::{TransportConfig, env_u64};

// Largest UDP payload that fits a 16 KiB loopback interface MTU (macOS lo0 is
// 16384; Linux lo is 65536) after IPv6 headers (48 bytes; IPv4 needs only 28).
const LOOPBACK_UDP_PAYLOAD: u16 = 16336;

// Linux UDP GSO puts a whole `sendmsg` batch in one IP datagram, so
// `mtu * segments <= 65535`; quinn batches up to 10, making 6553 the true
// ceiling. Above it the kernel rejects every batch, with an error quinn does not
// treat as a GSO failure (it only falls back on EIO/EINVAL), so delivery stalls
// and stays stalled. 4096 holds margin to 15 segments and is also Linux's
// fastest. macOS has no GSO and no such limit.
const LOOPBACK_PINNED_MTU_CAP: u16 = if cfg!(target_os = "macos") {
    LOOPBACK_UDP_PAYLOAD
} else {
    4096
};

impl TransportConfig {
    /// The initial MTU to use for a connection whose peer is a loopback
    /// address, or `None` to keep the configured default.
    ///
    /// Running at the real loopback MTU matters beyond skipping the discovery
    /// ramp: quinn's black-hole detector can misread a congestive loss burst
    /// (all lost packets full-MTU, which is what overflowing the peer's UDP
    /// socket buffer looks like at high rate) as an MTU black hole and drop
    /// the path MTU to `min_mtu` (1200 by default). Recovery probes are only
    /// sent when the connection has nothing else to transmit, so a busy
    /// connection that collapses stays collapsed — measured on loopback as
    /// ~13x the datagrams per byte and a 5-6x throughput drop for the life of
    /// the load. Guaranteeing the loopback MTU (see
    /// [`Self::quinn_transport_config_for_loopback`]) leaves the detector
    /// nothing to collapse to, which removes the failure mode on the one path
    /// where it was both most likely and least recoverable. An explicit
    /// `FELIX_INITIAL_MTU` wins over this.
    ///
    /// `effective_buffer_bytes` is the smaller of the socket's *achieved*
    /// send/receive buffers. It gates the guarantee as a proxy for "this host
    /// was tuned": Linux clamps `SO_RCVBUF` to `net.core.rmem_max` (~208 KB
    /// stock), and an untuned host keeps the RFC-safe path instead.
    pub(crate) fn loopback_initial_mtu(&self, effective_buffer_bytes: usize) -> Option<u16> {
        if env_u64("FELIX_INITIAL_MTU").is_some() {
            return None;
        }
        // The gate asks one question -- was this host tuned? -- so it is
        // measured against the jumbo payload and nothing configurable. Reading
        // it from `target` instead made the answer move with the MTU knobs: when
        // `mtu_discovery_upper_bound` became 4096 by default on Linux, the
        // requirement fell from ~1 MiB to 256 KiB and a stock host (~416 KiB)
        // newly qualified. That is the precise thing the previous comment here
        // said must not happen, and the routed-path change that lowered the
        // bound had no business altering who gets a loopback pin.
        if effective_buffer_bytes
            < usize::from(LOOPBACK_UDP_PAYLOAD.min(self.max_udp_payload_size)).saturating_mul(64)
        {
            return None;
        }
        let target = LOOPBACK_UDP_PAYLOAD
            .min(self.mtu_discovery_upper_bound)
            .min(self.max_udp_payload_size);
        let target = target.min(LOOPBACK_PINNED_MTU_CAP);
        (target > self.initial_mtu).then_some(target)
    }
}

#[cfg(test)]
mod tests;
