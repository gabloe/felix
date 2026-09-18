// Frame header layout and framing primitives for the v1 wire protocol.

use crate::error::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};

pub const MAGIC: u32 = 0x464C5831;
pub const VERSION: u16 = 1;
// Flags describe how to interpret the frame payload.
pub const FLAG_BINARY_PUBLISH_BATCH: u16 = 0x0001;
pub const FLAG_BINARY_EVENT_BATCH: u16 = 0x0002;
pub const FLAG_BINARY_EVENT_BATCH_SHARED: u16 = 0x0004;
/// Modifier on `FLAG_BINARY_PUBLISH_BATCH`: the payload is prefixed with a
/// `request_id` and an ack mode, and the broker owes the client an ack frame.
/// Never meaningful on its own — see [`crate::binary::decode_acked_publish_batch`].
pub const FLAG_BINARY_PUBLISH_ACKED: u16 = 0x0008;
/// Broker → client acknowledgement of an acked publish, replacing the JSON
/// `PublishOk`/`PublishError` messages on the binary path.
pub const FLAG_BINARY_PUBLISH_ACK: u16 = 0x0010;
/// Modifier on either event-batch flag: the payload carries a `base_offset`
/// before its payload count, giving the log offset of the batch's first event.
///
/// A batch's offsets are contiguous, so one `u64` per *batch* is enough — a
/// client derives each event's offset by adding its index. That is what keeps
/// this off the per-event cost model, and it is why offsets can ride the shared
/// encode-once batch at all: the offsets belong to the stream, not to the
/// subscriber, so one encoding still serves every subscriber that negotiated
/// the bit.
pub const FLAG_EVENT_BATCH_OFFSETS: u16 = 0x0020;

/// Every flag bit this version understands.
///
/// Frames carrying bits outside this mask are rejected rather than parsed with
/// the unknown bits ignored. That distinction matters: flag bits here change how
/// the *payload* is laid out, so silently ignoring one means confidently
/// misparsing the body. `FLAG_BINARY_PUBLISH_ACKED` is exactly that case — an
/// older broker that masked it off would read the new `request_id` prefix as a
/// `tenant_len` and produce garbage instead of an error. Rejecting unknown bits
/// cannot help those older brokers, but it means the next extension fails loudly
/// on this version instead of repeating the same trap.
pub const KNOWN_FLAGS: u16 = FLAG_BINARY_PUBLISH_BATCH
    | FLAG_BINARY_EVENT_BATCH
    | FLAG_BINARY_EVENT_BATCH_SHARED
    | FLAG_BINARY_PUBLISH_ACKED
    | FLAG_BINARY_PUBLISH_ACK
    | FLAG_EVENT_BATCH_OFFSETS;

/// The flag bits that existed before capability negotiation.
///
/// This is what a peer must be assumed to support when it does not advertise a
/// mask: brokers predating negotiation answer `Auth` with a plain `Ok`, and the
/// only safe reading of that silence is "the original three bits and nothing
/// more". Deliberately frozen — new bits must never be added here, or clients
/// will start assuming support that old brokers do not have.
pub const ORIGINAL_V1_FLAGS: u16 =
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_EVENT_BATCH | FLAG_BINARY_EVENT_BATCH_SHARED;

/// Optional requests a broker may implement, advertised in `AuthOk`.
///
/// A *feature* bit is not a frame flag. Frame flags say how a payload is laid
/// out and travel on every frame; these say only that a request exists, and
/// never appear on a frame at all. They are numbered separately for that
/// reason -- sharing the space would have a client offering to receive a frame
/// shape it has no encoder for.
///
/// A client must not send a featured request to a broker that did not advertise
/// the bit: an unrecognised message type is a fatal protocol error to the
/// broker's control loop, so probing costs the connection.
pub const FEATURE_TOPOLOGY: u32 = 0x0000_0001;

/// The peer understands `NotLeader`.
///
/// Offered by a *client*, and read by the broker, which is the direction that
/// matters here: `NotLeader` travels broker to client, and a client that cannot
/// decode it would lose the connection to a message meant to help it. A broker
/// talking to a client that did not offer this bit answers with an ordinary
/// error instead.
pub const FEATURE_REDIRECT: u32 = 0x0000_0002;

/// The broker understands `CacheDelete`.
///
/// Advertised by a *broker*, because this is a request rather than a response:
/// a client that sent it to a broker predating it would be sending an
/// unrecognised message type, which is fatal to the broker's control loop. A
/// client that does not see this bit reports that the broker cannot delete
/// rather than trying and losing the connection.
pub const FEATURE_CACHE_DELETE: u32 = 0x0000_0004;

/// The broker serves consumer groups: `group_poll`, `group_ack`, `group_nack`.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE` and for the same
/// reason: these are requests, and sending one to a broker that has no arm for
/// it ends that broker's control loop rather than returning an error.
///
/// A broker with no durable storage never advertises it. A group whose position
/// is lost on restart redelivers everything it had already finished, so there is
/// nothing useful to offer.
pub const FEATURE_CONSUMER_GROUP: u32 = 0x0000_0008;

/// The broker serves the dead-letter requests: `group_dead_letters`,
/// `group_discard`, `group_redrive`.
///
/// A bit of its own rather than folded into `FEATURE_CONSUMER_GROUP`. That bit
/// already means "serves poll, ack and nack" to every broker that advertises
/// it, and a broker built before these requests existed would have no arm for
/// them — which ends its control loop rather than returning an error. A feature
/// bit says one set of requests exists, and widening what an existing bit
/// promises is the one thing that cannot be done safely.
pub const FEATURE_GROUP_DEAD_LETTERS: u32 = 0x0000_0010;

/// The broker answers `stream_shards`: how many shards a stream was placed with.
///
/// A subscription reads one shard, so a client that wants a whole multi-shard
/// stream has to know how many there are. Nothing else on the wire tells it:
/// `topology` names brokers, not streams.
///
/// Advertised by a *broker*, and a separate bit rather than folded into
/// `FEATURE_TOPOLOGY` for the usual reason — that bit already means "names the
/// brokers" to every broker that advertises it, and a broker built before this
/// request existed has no arm for it.
pub const FEATURE_STREAM_SHARDS: u32 = 0x0000_0020;

/// The broker serves `cache_watch`: a subscription to changes for one cache
/// key or key prefix.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE` and for the same
/// reason: this is a request, and sending it to a broker that has no arm for
/// it ends that broker's control loop rather than returning an error.
///
/// Only a broker whose cache is log-backed advertises it. A watch's contract is
/// built on log offsets — resume, duplicate detection, and the lag signal all
/// name them — and a cache with no log has none to offer.
pub const FEATURE_CACHE_WATCH: u32 = 0x0000_0040;

/// The broker serves *retained* delivery on a `cache_watch`: each matching
/// key's current value first, then live changes.
///
/// A bit of its own rather than folded into `FEATURE_CACHE_WATCH`, for the
/// reason the dead-letter bit is not folded into the consumer-group bit: a bit
/// says which requests exist, and widening what an existing bit promises is
/// the one change that cannot be made safely. A broker built when
/// `FEATURE_CACHE_WATCH` meant live-and-resume only would ignore the unknown
/// `retained` field and serve a live-only watch — the client silently missing
/// exactly the state it joined for.
pub const FEATURE_CACHE_WATCH_RETAINED: u32 = 0x0000_0080;

/// The broker serves counters: `counter_add` and `counter_get`.
///
/// Advertised by a *broker*, like every request-shaped feature: sending either
/// to a broker with no arm for it ends that broker's control loop rather than
/// returning an error.
///
/// Only a broker with durable storage advertises it. A counter is a fold over
/// a log — the sum is rebuilt from the deltas on recovery — and a broker with
/// nowhere to write the log would be offering a sum that any restart resets,
/// which is worse than refusing to count at all.
pub const FEATURE_COUNTERS: u32 = 0x0000_0100;

/// The broker assigns producer ids (`producer_init`) and accepts
/// `publish_idempotent`: a batch carrying a producer id and a sequence number,
/// which it appends once however many times it arrives. A re-send of a batch
/// the broker already holds is acknowledged with the original's outcome rather
/// than appended again, and a refusal comes back as `publish_refused` with a
/// reason a client can act on.
///
/// Both messages exist only under this bit: a client offers the bit in its own
/// features to say it can decode `publish_refused`, and sends the requests
/// only to a broker that advertised it.
pub const FEATURE_IDEMPOTENT_PRODUCER: u32 = 0x0000_0200;

/// Every feature bit this version implements.
pub const KNOWN_FEATURES: u32 = FEATURE_TOPOLOGY
    | FEATURE_REDIRECT
    | FEATURE_CACHE_DELETE
    | FEATURE_CONSUMER_GROUP
    | FEATURE_GROUP_DEAD_LETTERS
    | FEATURE_STREAM_SHARDS
    | FEATURE_CACHE_WATCH
    | FEATURE_CACHE_WATCH_RETAINED
    | FEATURE_COUNTERS
    | FEATURE_IDEMPOTENT_PRODUCER;

/// True if `features` advertises `feature`.
pub fn supports_feature(features: u32, feature: u32) -> bool {
    features & feature == feature
}

/// True if `flags` contains any bit this version does not define.
pub fn has_unknown_flags(flags: u16) -> bool {
    flags & !KNOWN_FLAGS != 0
}

/// True if `peer_flags` advertises support for every bit in `required`.
pub fn supports(peer_flags: u16, required: u16) -> bool {
    peer_flags & required == required
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub length: u32,
}

impl FrameHeader {
    pub const LEN: usize = 12;

    // Create a header with the current protocol constants.
    pub fn new(flags: u16, length: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            flags,
            length,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        // Always encode in network byte order for portability.
        buf.extend_from_slice(&self.magic.to_be_bytes());
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf.extend_from_slice(&self.length.to_be_bytes());
    }

    pub fn encode_into(&self, buf: &mut [u8; Self::LEN]) {
        // Always encode in network byte order for portability.
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..6].copy_from_slice(&self.version.to_be_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_be_bytes());
        buf[8..12].copy_from_slice(&self.length.to_be_bytes());
    }

    pub fn decode(mut buf: Bytes) -> Result<Self> {
        // Validate header before we trust the length.
        if buf.remaining() < Self::LEN {
            return Err(Error::Incomplete);
        }
        let magic = buf.get_u32();
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = buf.get_u16();
        if version != VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let flags = buf.get_u16();
        let length = buf.get_u32();
        Ok(Self {
            magic,
            version,
            flags,
            length,
        })
    }
}

/// Frame containing a header and payload.
///
/// ```
/// use bytes::Bytes;
/// use felix_wire::Frame;
///
/// let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
/// let encoded = frame.encode();
/// let decoded = Frame::decode(encoded).expect("decode");
/// assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(flags: u16, payload: Bytes) -> Result<Self> {
        // Keep length within the on-wire u32 size.
        if payload.len() > u32::MAX as usize {
            return Err(Error::FrameTooLarge);
        }
        Ok(Self {
            header: FrameHeader::new(flags, payload.len() as u32),
            payload,
        })
    }

    pub fn encode(&self) -> Bytes {
        // Pre-allocate the exact size to avoid reallocation.
        let mut buf = BytesMut::with_capacity(FrameHeader::LEN + self.payload.len());
        self.header.encode(&mut buf);
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    pub fn decode(input: Bytes) -> Result<Self> {
        // Split header and payload based on the declared length.
        if input.len() < FrameHeader::LEN {
            return Err(Error::Incomplete);
        }
        let header = FrameHeader::decode(input.slice(0..FrameHeader::LEN))?;
        let length = header.length as usize;
        if input.len() < FrameHeader::LEN + length {
            return Err(Error::Incomplete);
        }
        let payload = input.slice(FrameHeader::LEN..FrameHeader::LEN + length);
        Ok(Self { header, payload })
    }
}
