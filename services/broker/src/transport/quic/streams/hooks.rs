// Test hooks allow forcing specific branches without mutating production logic.
//
// This module provides wrappers around key encoding and writing functions used in the QUIC transport layer.
// The purpose of these wrappers is to enable test code to inject failures or specific behaviors without
// modifying the actual production code paths. This approach helps ensure that error handling and edge cases
// can be thoroughly tested.
//
// Additionally, the module provides a helper function related to ACK throttling logic, allowing tests to
// override the throttle reset decision.
//
// Mutable static atomic booleans are used as toggles to control these test hooks at runtime during tests,
// enabling or disabling forced errors or behaviors in a thread-safe manner.

use anyhow::{Context, Result};
use felix_wire::{Frame, Message};
use quinn::SendStream;

use crate::transport::quic::codec::{write_frame, write_message};
use crate::transport::quic::{ACK_HI_WATER, ACK_LO_WATER};

/// Wrapper around `write_message` that allows tests to inject a forced write failure.
///
/// In normal operation, this simply delegates to the underlying `write_message` function.
/// However, when testing, the `force_write_message_error` hook can be enabled to simulate
/// a failure in writing a message to the QUIC send stream. This helps test error handling
/// paths without changing production code.
pub(super) async fn write_message_with_hook(send: &mut SendStream, message: Message) -> Result<()> {
    #[cfg(test)]
    if test_hooks::force_write_message_error() {
        return Err(anyhow::anyhow!("forced write_message error"));
    }
    write_message(send, message).await
}

/// Wrapper around `write_frame` that allows tests to inject a forced write failure.
///
/// Similar to `write_message_with_hook`, this wrapper enables tests to simulate failures
/// when writing raw frames to the QUIC send stream. This is useful for testing error
/// recovery and robustness of the frame writing logic.
pub(super) async fn write_frame_with_hook(send: &mut SendStream, frame: &Frame) -> Result<()> {
    #[cfg(test)]
    if test_hooks::force_write_frame_error() {
        return Err(anyhow::anyhow!("forced write_frame error"));
    }
    write_frame(send, frame).await
}

/// Wrapper around the message encoding step that allows tests to inject a forced encode failure.
///
/// Normally, this function encodes a `Message` into a `Frame` and returns the result.
/// During testing, the `force_cache_encode_error` hook can be enabled to simulate an encoding failure,
/// allowing tests to verify error handling when message encoding fails.
pub(super) fn encode_cache_message_with_hook(message: Message) -> Result<Frame> {
    #[cfg(test)]
    if test_hooks::force_cache_encode_error() {
        return Err(anyhow::anyhow!("forced cache encode error"));
    }
    message.encode().context("encode message")
}

/// Determines whether the ACK throttle should reset based on queue depth transitions.
///
/// The ACK throttling logic uses two watermarks: a high watermark and a low watermark.
/// When the queue depth crosses above the high watermark, throttling may be enabled.
/// When the queue depth subsequently drops below the low watermark, throttling should reset.
///
/// This function takes an optional tuple `(prev, cur)` representing the previous and current
/// queue depths. It returns `true` if the throttle should reset (i.e., if the previous depth
/// was above or equal to the high watermark and the current depth is below the low watermark).
///
/// Tests can also force a throttle reset by enabling the corresponding test hook.
pub(super) fn should_reset_throttle(depth_update: Option<(usize, usize)>) -> bool {
    if let Some((prev, cur)) = depth_update
        && prev >= ACK_HI_WATER
        && cur < ACK_LO_WATER
    {
        return true;
    }
    #[cfg(test)]
    if test_hooks::force_throttle_reset() {
        return true;
    }
    false
}

/// Mutable test toggles for hard-to-reach error, timeout, and special-case branches.
///
/// These atomic boolean flags allow tests to dynamically enable or disable forced errors
/// or special behaviors in the transport layer without changing production code or requiring
/// complicated setup. They are intended to be used only in test builds (`#[cfg(test)]`).
///
/// Each toggle corresponds to a particular forced condition, such as forcing write failures,
/// encoding errors, throttle resets, or timeouts. Tests can set these flags to `true` to simulate
/// the corresponding condition and reset them back to `false` after the test.
///
/// The atomic operations ensure thread-safe access to these flags, which is important since
/// tests may run concurrently.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Atomic flag to force an error when writing a message.
    static FORCE_WRITE_MESSAGE_ERROR: AtomicBool = AtomicBool::new(false);
    // Atomic flag to force an error when writing a raw frame.
    static FORCE_WRITE_FRAME_ERROR: AtomicBool = AtomicBool::new(false);
    // Atomic flag to force an error during message encoding.
    static FORCE_CACHE_ENCODE_ERROR: AtomicBool = AtomicBool::new(false);
    // Atomic flag to force the ACK throttle to reset regardless of queue depth.
    static FORCE_THROTTLE_RESET: AtomicBool = AtomicBool::new(false);
    // Atomic flag to force a drain timeout condition (not used in this module but provided for completeness).
    static FORCE_DRAIN_TIMEOUT: AtomicBool = AtomicBool::new(false);

    /// Enable or disable forced write_message error.
    pub fn set_force_write_message_error(enabled: bool) {
        FORCE_WRITE_MESSAGE_ERROR.store(enabled, Ordering::Relaxed);
    }

    /// Enable or disable forced write_frame error.
    pub fn set_force_write_frame_error(enabled: bool) {
        FORCE_WRITE_FRAME_ERROR.store(enabled, Ordering::Relaxed);
    }

    /// Enable or disable forced cache encode error.
    pub fn set_force_cache_encode_error(enabled: bool) {
        FORCE_CACHE_ENCODE_ERROR.store(enabled, Ordering::Relaxed);
    }

    /// Enable or disable forced throttle reset.
    pub fn set_force_throttle_reset(enabled: bool) {
        FORCE_THROTTLE_RESET.store(enabled, Ordering::Relaxed);
    }

    /// Enable or disable forced drain timeout.
    pub fn set_force_drain_timeout(enabled: bool) {
        FORCE_DRAIN_TIMEOUT.store(enabled, Ordering::Relaxed);
    }

    /// Check if forced write_message error is enabled.
    pub fn force_write_message_error() -> bool {
        FORCE_WRITE_MESSAGE_ERROR.load(Ordering::Relaxed)
    }

    /// Check if forced write_frame error is enabled.
    pub fn force_write_frame_error() -> bool {
        FORCE_WRITE_FRAME_ERROR.load(Ordering::Relaxed)
    }

    /// Check if forced cache encode error is enabled.
    pub fn force_cache_encode_error() -> bool {
        FORCE_CACHE_ENCODE_ERROR.load(Ordering::Relaxed)
    }

    /// Check if forced throttle reset is enabled.
    pub fn force_throttle_reset() -> bool {
        FORCE_THROTTLE_RESET.load(Ordering::Relaxed)
    }

    /// Check if forced drain timeout is enabled.
    pub fn force_drain_timeout() -> bool {
        FORCE_DRAIN_TIMEOUT.load(Ordering::Relaxed)
    }

    /// Reset all test hooks to disabled.
    ///
    /// This is useful to ensure a clean state between tests.
    pub fn reset() {
        set_force_write_message_error(false);
        set_force_write_frame_error(false);
        set_force_cache_encode_error(false);
        set_force_throttle_reset(false);
        set_force_drain_timeout(false);
    }
}
