use super::*;

#[test]
fn loopback_initial_mtu_respects_configured_bounds() {
    // Plenty of socket buffer: a typical macOS 8 MiB grant.
    const BIG_BUFFER: usize = 8 * 1024 * 1024;
    // A stock-Linux clamp (net.core.rmem_max ~208 KB, doubled by the
    // kernel), i.e. a host nobody has tuned.
    const CLAMPED_BUFFER: usize = 416 * 1024;

    // What the guarantee pins where it applies: the full loopback payload
    // on macOS, the platform cap elsewhere.
    const EXPECTED: u16 = LOOPBACK_PINNED_MTU_CAP;

    // Default config: loopback connections start above the RFC-safe 1200.
    let config = TransportConfig::default();
    assert_eq!(config.loopback_initial_mtu(BIG_BUFFER), Some(EXPECTED));

    // An untuned host keeps the stock path on every platform. The gate is
    // measured against the jumbo payload and nothing configurable, so this
    // answer does not move when the MTU knobs do -- which is what broke
    // when the discovery bound's default dropped to 4096 on Linux and a
    // stock host newly qualified.
    assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), None);

    // A lowered discovery bound caps the loopback start size with it.
    let config = TransportConfig {
        mtu_discovery_upper_bound: 4096,
        ..TransportConfig::default()
    };
    assert_eq!(config.loopback_initial_mtu(BIG_BUFFER), Some(4096));
    // But it does not buy the guarantee on a host that has not been tuned.
    // Asking for a smaller pin is not evidence of headroom, and the pin
    // exists to survive bursts an untuned socket buffer cannot absorb.
    // This case used to answer `Some(4096)`, back when the gate scaled with
    // the requested size; making the gate mean one thing costs it.
    assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), None);

    // No boost when the configured initial MTU is already at least as big.
    let config = TransportConfig {
        initial_mtu: 16354,
        ..TransportConfig::default()
    };
    assert_eq!(config.loopback_initial_mtu(8 * 1024 * 1024), None);
}

/// Arithmetic only — the Linux stall this guards needs a sustained batch
/// run on a tuned host, which nothing in this workspace performs.
#[test]
fn loopback_guarantee_is_capped_off_macos() {
    const BIG_BUFFER: usize = 8 * 1024 * 1024;
    let pinned = TransportConfig::default()
        .loopback_initial_mtu(BIG_BUFFER)
        .expect("tuned host takes the loopback path");

    if cfg!(target_os = "macos") {
        assert_eq!(pinned, LOOPBACK_UDP_PAYLOAD);
    } else {
        assert!(
            pinned <= 4096,
            "pinning min_mtu at {pinned} collapses throughput on Linux"
        );
    }
}

/// Both platforms, from either platform.
///
/// The regression this guards against was invisible on macOS: the default
/// discovery bound stays 16384 there, so the gate never moved and the test
/// that caught it passed locally while failing on Linux CI. Constructing
/// the config explicitly checks the behaviour that matters wherever this
/// runs.
fn config_with(bound: u16) -> TransportConfig {
    TransportConfig {
        mtu_discovery_upper_bound: bound,
        ..TransportConfig::default()
    }
}

/// A stock Linux host: `net.core.rmem_max` ~208 KB, doubled by the kernel.
const UNTUNED: usize = 416 * 1024;

const TUNED: usize = 8 * 1024 * 1024;

/// **Lowering the discovery bound must not hand the pin to an untuned
/// host.**
///
/// The gate is a proxy for "was this host tuned", and a proxy that moves
/// with an unrelated knob is not one. When 0.5.0 dropped the bound's
/// default to 4096 on Linux for a *routed*-path hazard, the requirement
/// fell from ~1 MiB of socket buffer to 256 KiB and every stock Linux host
/// silently started pinning the loopback MTU.
#[test]
fn an_untuned_host_is_refused_whatever_the_discovery_bound_says() {
    for bound in [16384, 4096, 2048] {
        assert_eq!(
            config_with(bound).loopback_initial_mtu(UNTUNED),
            None,
            "an untuned host qualified with the bound at {bound}: the gate \
             is tracking the MTU knob instead of the host",
        );
    }
}

/// And a tuned host still gets it, capped by whichever bound is lower.
#[test]
fn a_tuned_host_gets_the_pin_capped_by_the_bound() {
    assert_eq!(
        config_with(4096).loopback_initial_mtu(TUNED),
        Some(4096),
        "a tuned host lost the guarantee",
    );
    assert_eq!(
        config_with(2048).loopback_initial_mtu(TUNED),
        Some(2048),
        "an explicitly lowered bound should still cap the pin",
    );
}
