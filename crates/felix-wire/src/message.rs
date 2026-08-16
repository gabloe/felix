// V1 protocol message enum and its JSON codec.

use crate::error::{Error, Result};
use crate::frame::Frame;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// V1 wire messages encoded in framed payloads.
///
/// ```
/// use felix_wire::Message;
///
/// let message = Message::Publish {
///     tenant_id: "t1".to_string(),
///     namespace: "default".to_string(),
///     stream: "updates".to_string(),
///     payload: b"hello".to_vec(),
///     request_id: None,
///     ack: None,
/// };
/// let frame = message.encode().expect("encode");
/// let decoded = Message::decode(frame).expect("decode");
/// assert_eq!(message, decoded);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // Authenticate a control stream for a specific tenant.
    Auth {
        tenant_id: String,
        token: String,
        /// Frame-flag bits this client understands, offered so the broker can
        /// reply with its own set.
        ///
        /// Absent means the client predates capability negotiation. Because
        /// serde ignores unknown fields, a broker that predates it also simply
        /// ignores this field and answers with a plain `Ok` — which is exactly
        /// the signal the client needs to fall back. Both directions therefore
        /// degrade without a version check.
        #[serde(skip_serializing_if = "Option::is_none")]
        client_flags: Option<u16>,
    },
    // Successful auth, carrying the broker's supported frame-flag bits.
    //
    // Only ever sent in response to an `Auth` that offered `client_flags`, so a
    // client old enough not to understand this variant can never receive it.
    AuthOk {
        server_flags: u16,
    },
    // Publish a single payload to a stream.
    Publish {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::base64_serde::base64_bytes")]
        payload: Vec<u8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    // Publish a batch of payloads in a single request.
    PublishBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::base64_serde::base64_vec")]
        payloads: Vec<Vec<u8>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    // Subscribe to a stream; server responds with Subscribed.
    Subscribe {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        subscription_id: Option<u64>,
        /// Where to begin delivering from.
        ///
        /// Absent means the tail, which is what every client sent before this
        /// field existed — so an old client and a new broker still produce
        /// byte-for-byte the frames they always did, and a new client talking
        /// to an old broker has its position ignored rather than misread.
        #[serde(skip_serializing_if = "Option::is_none")]
        start: Option<StartPosition>,
    },
    // Subscription confirmation with server-assigned ID.
    Subscribed {
        subscription_id: u64,
    },
    // First message on the event stream for a subscription.
    EventStreamHello {
        subscription_id: u64,
    },
    // Single event delivered to a subscriber.
    Event {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::base64_serde::base64_bytes")]
        payload: Vec<u8>,
        /// Log offset of this event, for durable streams.
        ///
        /// Absent for in-memory streams, which have no durable position to
        /// checkpoint against, and absent from every frame a pre-offsets broker
        /// sends.
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<u64>,
    },
    // JSON event batch (binary batch uses FLAG_BINARY_EVENT_BATCH).
    EventBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::base64_serde::base64_vec")]
        payloads: Vec<Vec<u8>>,
        /// Offset of the first payload. The rest are contiguous from there, so
        /// payload `i` sits at `base_offset + i`.
        #[serde(skip_serializing_if = "Option::is_none")]
        base_offset: Option<u64>,
    },
    // Cache set operation; may include TTL and request id.
    CachePut {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "crate::base64_serde::base64_bytes_bytes")]
        value: Bytes,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        ttl_ms: Option<u64>,
    },
    // Cache get operation; request id is echoed in responses.
    CacheGet {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    // Cache read response (value is optional for misses).
    CacheValue {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "crate::base64_serde::base64_option_bytes")]
        value: Option<Bytes>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    // Cache write response with request id.
    CacheOk {
        request_id: u64,
    },
    // Publish ack with request id.
    PublishOk {
        request_id: u64,
    },
    // Publish error with request id.
    PublishError {
        request_id: u64,
        message: String,
    },
    // Generic success response.
    Ok,
    // Protocol-level error for invalid requests or unexpected message types.
    Error {
        message: String,
    },
    /// A subscribe could not start where it was asked to.
    ///
    /// Distinct from `Error` because the client must be able to *act* on it:
    /// `too_old` means pick a newer offset (or `earliest`), `in_future` means
    /// the log has not reached that offset yet. A generic string forces every
    /// client to parse prose to tell those apart, and a client that guesses
    /// wrong silently restarts at the tail -- the failure resume exists to
    /// remove.
    SubscribeCursorError {
        reason: CursorErrorReason,
        /// The offset that was asked for.
        requested: u64,
        /// For `too_old`, the oldest offset still retained. For `in_future`,
        /// the current tail. Either way: the nearest offset that would work.
        available: u64,
    },
}

/// Why a subscribe could not start at the requested position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorErrorReason {
    /// The offset has been discarded by retention, or has fallen out of an
    /// in-memory stream's replay ring.
    TooOld,
    /// The offset is past the end of the stream.
    InFuture,
}

/// Where a subscription should begin.
///
/// Untagged on the wire so `"latest"` and `{"offset": 42}` are both accepted,
/// and so the common cases stay short in a JSON control message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// The live tail: deliver only what is published from now on. Identical to
    /// omitting the field, and spelled out for clients that prefer to be
    /// explicit.
    Latest,
    /// The oldest record the broker still retains.
    ///
    /// Deliberately not "offset 0": for a stream whose head has been trimmed,
    /// offset 0 is gone and asking for it is an error, whereas `earliest` means
    /// "as far back as you can" and always succeeds.
    Earliest,
    /// Resume at an exact log offset — the first record the client has *not*
    /// seen, so a client checkpoints the offset it last handled plus one.
    Offset(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    None,
    PerMessage,
    PerBatch,
}

impl Message {
    pub fn encode(&self) -> Result<Frame> {
        // JSON-encode into a framed payload.
        let payload = serde_json::to_vec(self).map_err(Error::Serialize)?;
        Frame::new(0, Bytes::from(payload))
    }

    pub fn decode(frame: Frame) -> Result<Self> {
        serde_json::from_slice(&frame.payload).map_err(Error::Deserialize)
    }
}
