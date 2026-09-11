//! The protocol brokers speak to each other.
//!
//! Deliberately separate from the client protocol, and not a superset of it. A
//! client able to send `ForwardPublish` could write to a shard on a broker that
//! never checked ownership; a broker accepting client frames on its internal
//! listener would treat a peer as an authenticated publisher. The distinct magic
//! below is what makes a misdirected connection fail loudly rather than parse
//! into something plausible — it is not the security boundary, which is the
//! separate listener and peer credential.
//!
//! Flows, correlation rules, and the error table live in
//! `docs/internal-protocol.md`.
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// `FLXI`. Distinct from the client protocol's `FLX1` so neither side can
/// decode the other's frames.
pub const INTERNAL_MAGIC: u32 = 0x464C_5849;

/// This protocol's version, independent of the client protocol's.
///
/// Additive change happens by adding a [`Kind`], which an older peer already
/// rejects as unknown. This exists for the change that cannot cover: the header,
/// or an existing body layout.
pub const INTERNAL_VERSION: u16 = 1;

/// Largest body this protocol will decode, before any allocation is sized from
/// a peer-provided number.
pub const MAX_BODY_BYTES: u32 = 64 * 1024 * 1024;

/// Longest identifier (tenant, namespace, stream, node id) accepted.
pub const MAX_IDENT_BYTES: usize = 512;

/// Most payloads one forwarded batch may carry.
pub const MAX_BATCH_PAYLOADS: usize = 65_536;

/// Every payload is a 4-byte length prefix plus its bytes, so a body can never
/// hold more payloads than it has 4-byte groups left.
const LEN_PREFIX: usize = 4;

/// Message discriminant.
///
/// A discriminant rather than flag bits: the client protocol uses bits because
/// they modify one payload layout, whereas here each kind *is* a layout, so an
/// enum makes "unknown kind" one unambiguous check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Kind {
    ForwardPublish = 1,
    ForwardPublishOk = 2,
    ForwardPublishError = 3,
    NotLeader = 4,
    Hello = 5,
    HelloOk = 6,
    ReplicateRecords = 7,
    ReplicateOk = 8,
    ReplicateError = 9,
    ReplicateBootstrap = 10,
}

impl Kind {
    /// An unknown kind is rejected, never skipped: the kind selects how to read
    /// the body, so ignoring one means confidently misparsing it.
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Kind::ForwardPublish),
            2 => Ok(Kind::ForwardPublishOk),
            3 => Ok(Kind::ForwardPublishError),
            4 => Ok(Kind::NotLeader),
            5 => Ok(Kind::Hello),
            6 => Ok(Kind::HelloOk),
            7 => Ok(Kind::ReplicateRecords),
            8 => Ok(Kind::ReplicateOk),
            9 => Ok(Kind::ReplicateError),
            10 => Ok(Kind::ReplicateBootstrap),
            other => Err(Error::UnsupportedInternalKind(other)),
        }
    }
}

/// Why a forwarded request could not be served.
///
/// Typed because each needs a different response from the requester — see the
/// table in `docs/internal-protocol.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    /// The responder is behind the generation the requester named. It must not
    /// accept the write: it may no longer hold the shard.
    StaleRoute = 1,
    /// The owner cannot serve yet, e.g. still opening the shard.
    Unavailable = 2,
    /// The peer is not permitted. Not retryable.
    Unauthorized = 3,
    /// The owner is shedding load.
    Overload = 4,
    /// The peer spoke a version this broker does not know. Not retryable.
    ProtocolVersion = 5,
    /// The body did not decode. Not retryable.
    Malformed = 6,
    /// The owner accepted the request and the local write failed.
    StorageFailed = 7,
    /// A replication batch starts past the follower's tail. Applying it would
    /// leave a hole, and a log with a hole cannot be read back. The follower
    /// reports the offset it does want; the leader resumes there.
    LogGap = 8,
    /// A replication batch disagrees with bytes the follower has already
    /// stored. Records are never rewritten, so there is no repair for this:
    /// the two logs have diverged and progress stops here.
    LogConflict = 9,
    /// The sender named an epoch older than the responder's. It has been
    /// superseded and is no longer the leader, so its records must not be
    /// stored: it may have written them after losing the shard. Not retryable —
    /// the fence does not lift.
    FencedEpoch = 10,
}

impl ErrorCode {
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(ErrorCode::StaleRoute),
            2 => Ok(ErrorCode::Unavailable),
            3 => Ok(ErrorCode::Unauthorized),
            4 => Ok(ErrorCode::Overload),
            5 => Ok(ErrorCode::ProtocolVersion),
            6 => Ok(ErrorCode::Malformed),
            7 => Ok(ErrorCode::StorageFailed),
            8 => Ok(ErrorCode::LogGap),
            9 => Ok(ErrorCode::LogConflict),
            10 => Ok(ErrorCode::FencedEpoch),
            other => Err(Error::UnknownInternalErrorCode(other)),
        }
    }

    /// Whether a requester should try the same peer again.
    ///
    /// `LogGap` is retryable but not by re-sending the same batch: the follower
    /// names the offset it wants, and the leader resumes from there. `LogConflict`
    /// is absent deliberately — divergent logs do not converge by retrying.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::StaleRoute
                | ErrorCode::Unavailable
                | ErrorCode::Overload
                | ErrorCode::LogGap
        )
    }
}

/// How the origin publisher asked for its write to be acknowledged.
///
/// Carried across the forward so the owner applies the guarantee the client
/// asked for, not the one the forwarding broker would have chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AckMode {
    None = 0,
    OnAccept = 1,
    OnCommit = 2,
}

impl AckMode {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(AckMode::None),
            1 => Ok(AckMode::OnAccept),
            2 => Ok(AckMode::OnCommit),
            other => Err(Error::UnknownInternalAckMode(other)),
        }
    }
}

/// Which shard a forwarded request is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRef {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    /// The assignment generation the requester resolved against.
    ///
    /// The owner compares this with its own. Equal proceeds; either mismatch is
    /// an explicit typed answer, and never a successful ownership claim.
    pub generation: u64,
}

/// A publish handed to the broker that owns the shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPublish {
    pub correlation_id: u64,
    pub shard: ShardRef,
    pub ack: AckMode,
    pub payloads: Vec<Bytes>,
}

/// The owner accepted and wrote the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardPublishOk {
    pub correlation_id: u64,
    pub first_offset: u64,
    /// Inclusive, so a single-record batch has `first_offset == last_offset`.
    pub last_offset: u64,
}

/// The owner refused, with a reason the requester can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPublishError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    /// Operator-facing detail. Never parsed for control flow — `code` is what a
    /// requester branches on.
    pub detail: String,
}

/// The shard is owned elsewhere, and here is where.
///
/// A distinct kind rather than an error code, because it carries a routing
/// answer rather than only a reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeader {
    pub correlation_id: u64,
    pub node_id: String,
    /// `host:port` of the owner's internal listener, as the catalog advertises
    /// it. A string rather than a parsed address: the responder repeats what it
    /// was told, and the requester decides whether it can be reached.
    pub advertise_addr: String,
    /// The generation the responder believes is current.
    pub generation: u64,
}

/// The first message on a peer connection, naming who is calling.
///
/// Sent before any request so a version or identity mismatch is found while the
/// connection is being established rather than on the first forwarded publish,
/// which would otherwise have to be failed and retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub correlation_id: u64,
    /// The caller's cluster identity, as the catalog knows it.
    pub node_id: String,
}

/// The responder accepted the handshake and names itself.
///
/// The caller checks this against the node id it dialled. An address the
/// catalog has since reassigned answers with a different id, which is a
/// connection to the wrong broker regardless of whether it would have served
/// the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloOk {
    pub correlation_id: u64,
    pub node_id: String,
}

/// Records the leader has committed, shipped to a follower.
///
/// Append-only, and identified by the offsets the *leader* assigned: a follower
/// stores a record at the leader's offset or not at all. That is what makes the
/// two logs comparable by offset, which every other part of replication relies
/// on.
///
/// `shard.generation` is the leader's epoch. A follower that knows of a newer
/// one refuses, because a leader at an older epoch may already have been
/// replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateRecords {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of `payloads[0]`. Payload `i` belongs at `first_offset + i`.
    pub first_offset: u64,
    /// Over the payload bytes, in order. Checked before anything is written, so
    /// a batch corrupted in transit is refused rather than stored.
    pub checksum: u64,
    pub payloads: Vec<Bytes>,
}

/// The follower stored the batch.
///
/// `durable_offset` is one past the last record the follower has on disk, so it
/// is both an acknowledgement and the offset the leader should send next. It
/// reports **durable** data, never buffered: a follower that acknowledged
/// before its own fsync would let the leader believe a record survived a
/// failure it would not have survived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateOk {
    pub correlation_id: u64,
    pub durable_offset: u64,
}

/// The leader has nothing older than `base_offset` left.
///
/// Sent when a follower's position is below everything the leader still holds,
/// so shipping cannot reach it: the records in between are gone from the leader
/// too. It says "the surviving log starts here" — which is the one fact a
/// follower cannot work out for itself, and the fact it needs before it may
/// place a log that begins anywhere other than zero.
///
/// A follower with records of its own refuses. Discarding them is an operator's
/// decision, not a leader's.
///
/// A separate kind rather than a field on [`ReplicateRecords`]: this protocol
/// freezes existing body layouts, and an older peer already rejects an unknown
/// kind rather than misreading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateBootstrap {
    pub correlation_id: u64,
    pub shard: ShardRef,
    /// Offset of the oldest record the leader still holds.
    pub base_offset: u64,
}

/// The follower refused, and where it stands.
///
/// `expected_offset` is what the follower wants next. For `LogGap` it is how
/// the leader repairs without a separate negotiation; for the rest it is
/// diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateError {
    pub correlation_id: u64,
    pub code: ErrorCode,
    pub expected_offset: u64,
    pub detail: String,
}

/// A decoded internal message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalMessage {
    ForwardPublish(ForwardPublish),
    ForwardPublishOk(ForwardPublishOk),
    ForwardPublishError(ForwardPublishError),
    NotLeader(NotLeader),
    Hello(Hello),
    HelloOk(HelloOk),
    ReplicateRecords(ReplicateRecords),
    ReplicateOk(ReplicateOk),
    ReplicateError(ReplicateError),
    ReplicateBootstrap(ReplicateBootstrap),
}

impl InternalMessage {
    pub fn kind(&self) -> Kind {
        match self {
            Self::ForwardPublish(_) => Kind::ForwardPublish,
            Self::ForwardPublishOk(_) => Kind::ForwardPublishOk,
            Self::ForwardPublishError(_) => Kind::ForwardPublishError,
            Self::NotLeader(_) => Kind::NotLeader,
            Self::Hello(_) => Kind::Hello,
            Self::HelloOk(_) => Kind::HelloOk,
            Self::ReplicateRecords(_) => Kind::ReplicateRecords,
            Self::ReplicateOk(_) => Kind::ReplicateOk,
            Self::ReplicateError(_) => Kind::ReplicateError,
            Self::ReplicateBootstrap(_) => Kind::ReplicateBootstrap,
        }
    }

    /// The id this message answers, or carries if it is a request.
    ///
    /// Every message has one, which is what makes the request lifecycle
    /// unambiguous: a response can always be matched or discarded, never left
    /// pending.
    pub fn correlation_id(&self) -> u64 {
        match self {
            Self::ForwardPublish(m) => m.correlation_id,
            Self::ForwardPublishOk(m) => m.correlation_id,
            Self::ForwardPublishError(m) => m.correlation_id,
            Self::NotLeader(m) => m.correlation_id,
            Self::Hello(m) => m.correlation_id,
            Self::HelloOk(m) => m.correlation_id,
            Self::ReplicateRecords(m) => m.correlation_id,
            Self::ReplicateOk(m) => m.correlation_id,
            Self::ReplicateError(m) => m.correlation_id,
            Self::ReplicateBootstrap(m) => m.correlation_id,
        }
    }

    /// Encode a complete frame: header then body.
    pub fn encode(&self) -> Result<Bytes> {
        let mut body = BytesMut::new();
        match self {
            Self::ForwardPublish(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u8(m.ack as u8);
                if m.payloads.len() > MAX_BATCH_PAYLOADS {
                    return Err(Error::FrameTooLarge);
                }
                body.put_u32(m.payloads.len() as u32);
                for payload in &m.payloads {
                    body.put_u32(u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?);
                    body.extend_from_slice(payload);
                }
            }
            Self::ForwardPublishOk(m) => {
                body.put_u64(m.correlation_id);
                body.put_u64(m.first_offset);
                body.put_u64(m.last_offset);
            }
            Self::ForwardPublishError(m) => {
                body.put_u64(m.correlation_id);
                body.put_u16(m.code as u16);
                put_str(&mut body, &m.detail)?;
            }
            Self::NotLeader(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
                put_str(&mut body, &m.advertise_addr)?;
                body.put_u64(m.generation);
            }
            Self::Hello(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
            }
            Self::HelloOk(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.node_id)?;
            }
            Self::ReplicateRecords(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u64(m.first_offset);
                body.put_u64(m.checksum);
                if m.payloads.len() > MAX_BATCH_PAYLOADS {
                    return Err(Error::FrameTooLarge);
                }
                body.put_u32(m.payloads.len() as u32);
                for payload in &m.payloads {
                    body.put_u32(u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge)?);
                    body.extend_from_slice(payload);
                }
            }
            Self::ReplicateOk(m) => {
                body.put_u64(m.correlation_id);
                body.put_u64(m.durable_offset);
            }
            Self::ReplicateError(m) => {
                body.put_u64(m.correlation_id);
                body.put_u16(m.code as u16);
                body.put_u64(m.expected_offset);
                put_str(&mut body, &m.detail)?;
            }
            Self::ReplicateBootstrap(m) => {
                body.put_u64(m.correlation_id);
                put_str(&mut body, &m.shard.tenant_id)?;
                put_str(&mut body, &m.shard.namespace)?;
                put_str(&mut body, &m.shard.stream)?;
                body.put_u32(m.shard.shard);
                body.put_u64(m.shard.generation);
                body.put_u64(m.base_offset);
            }
        }

        let length = u32::try_from(body.len()).map_err(|_| Error::FrameTooLarge)?;
        if length > MAX_BODY_BYTES {
            return Err(Error::FrameTooLarge);
        }

        let mut frame = BytesMut::with_capacity(InternalHeader::LEN + body.len());
        InternalHeader {
            kind: self.kind(),
            length,
        }
        .encode(&mut frame);
        frame.extend_from_slice(&body);
        Ok(frame.freeze())
    }

    /// Decode a complete frame.
    ///
    /// Every peer-provided length is checked against what remains before it is
    /// used to size anything. A broker is authenticated, not assumed correct.
    pub fn decode(buf: Bytes) -> Result<Self> {
        let header = InternalHeader::decode(&buf)?;
        let body = buf.slice(InternalHeader::LEN..);
        if body.len() != header.length as usize {
            return Err(Error::Incomplete);
        }
        let mut body = body;

        match header.kind {
            Kind::ForwardPublish => {
                let correlation_id = take_u64(&mut body)?;
                let tenant_id = take_str(&mut body)?;
                let namespace = take_str(&mut body)?;
                let stream = take_str(&mut body)?;
                let shard = take_u32(&mut body)?;
                let generation = take_u64(&mut body)?;
                let ack = AckMode::from_u8(take_u8(&mut body)?)?;

                let declared = take_u32(&mut body)? as usize;
                // Bounded against what the body could actually hold, before it
                // reaches `with_capacity`. Same trap the client binary path
                // documents: a tiny frame declaring u32::MAX payloads otherwise
                // reserves enough address space to abort the process.
                if declared > MAX_BATCH_PAYLOADS || declared > body.remaining() / LEN_PREFIX {
                    return Err(Error::Incomplete);
                }
                let mut payloads = Vec::with_capacity(declared);
                for _ in 0..declared {
                    let len = take_u32(&mut body)? as usize;
                    if len > body.remaining() {
                        return Err(Error::Incomplete);
                    }
                    payloads.push(body.split_to(len));
                }
                if body.has_remaining() {
                    // Trailing bytes mean the body did not describe itself, so
                    // something is wrong with the peer, not merely with this
                    // message.
                    return Err(Error::Incomplete);
                }

                Ok(Self::ForwardPublish(ForwardPublish {
                    correlation_id,
                    shard: ShardRef {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        generation,
                    },
                    ack,
                    payloads,
                }))
            }
            Kind::ForwardPublishOk => {
                let message = ForwardPublishOk {
                    correlation_id: take_u64(&mut body)?,
                    first_offset: take_u64(&mut body)?,
                    last_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ForwardPublishOk(message))
            }
            Kind::ForwardPublishError => {
                let message = ForwardPublishError {
                    correlation_id: take_u64(&mut body)?,
                    code: ErrorCode::from_u16(take_u16(&mut body)?)?,
                    detail: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ForwardPublishError(message))
            }
            Kind::NotLeader => {
                let message = NotLeader {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                    advertise_addr: take_str(&mut body)?,
                    generation: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::NotLeader(message))
            }
            Kind::Hello => {
                let message = Hello {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::Hello(message))
            }
            Kind::HelloOk => {
                let message = HelloOk {
                    correlation_id: take_u64(&mut body)?,
                    node_id: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::HelloOk(message))
            }
            Kind::ReplicateRecords => {
                let correlation_id = take_u64(&mut body)?;
                let tenant_id = take_str(&mut body)?;
                let namespace = take_str(&mut body)?;
                let stream = take_str(&mut body)?;
                let shard = take_u32(&mut body)?;
                let generation = take_u64(&mut body)?;
                let first_offset = take_u64(&mut body)?;
                let checksum = take_u64(&mut body)?;

                let declared = take_u32(&mut body)? as usize;
                // Bounded against what the body could hold before it reaches
                // `with_capacity`, exactly as `ForwardPublish` documents.
                if declared > MAX_BATCH_PAYLOADS || declared > body.remaining() / LEN_PREFIX {
                    return Err(Error::Incomplete);
                }
                let mut payloads = Vec::with_capacity(declared);
                for _ in 0..declared {
                    let len = take_u32(&mut body)? as usize;
                    if len > body.remaining() {
                        return Err(Error::Incomplete);
                    }
                    payloads.push(body.split_to(len));
                }
                expect_empty(&body)?;

                Ok(Self::ReplicateRecords(ReplicateRecords {
                    correlation_id,
                    shard: ShardRef {
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        generation,
                    },
                    first_offset,
                    checksum,
                    payloads,
                }))
            }
            Kind::ReplicateOk => {
                let message = ReplicateOk {
                    correlation_id: take_u64(&mut body)?,
                    durable_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateOk(message))
            }
            Kind::ReplicateError => {
                let message = ReplicateError {
                    correlation_id: take_u64(&mut body)?,
                    code: ErrorCode::from_u16(take_u16(&mut body)?)?,
                    expected_offset: take_u64(&mut body)?,
                    detail: take_str(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateError(message))
            }
            Kind::ReplicateBootstrap => {
                let message = ReplicateBootstrap {
                    correlation_id: take_u64(&mut body)?,
                    shard: ShardRef {
                        tenant_id: take_str(&mut body)?,
                        namespace: take_str(&mut body)?,
                        stream: take_str(&mut body)?,
                        shard: take_u32(&mut body)?,
                        generation: take_u64(&mut body)?,
                    },
                    base_offset: take_u64(&mut body)?,
                };
                expect_empty(&body)?;
                Ok(Self::ReplicateBootstrap(message))
            }
        }
    }
}

/// The checksum a [`ReplicateRecords`] batch carries.
///
/// Defined once, here, so the leader and the follower cannot compute it
/// differently — a checksum the two sides disagree about reports corruption on
/// every healthy batch, which is worse than not having one.
///
/// It covers each payload's length and its bytes, in order. Including the
/// length is what stops `["ab", "c"]` and `["a", "bc"]` hashing alike: they are
/// different records, and a follower storing one where the leader has the other
/// is exactly the divergence this is here to catch.
pub fn batch_checksum(payloads: &[Bytes]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    for payload in payloads {
        hasher.update(&(payload.len() as u32).to_be_bytes());
        hasher.update(payload);
    }
    u64::from(hasher.finalize())
}

/// Fixed-size header preceding every internal body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalHeader {
    pub kind: Kind,
    pub length: u32,
}

impl InternalHeader {
    pub const LEN: usize = 12;

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(INTERNAL_MAGIC);
        buf.put_u16(INTERNAL_VERSION);
        buf.put_u16(self.kind as u16);
        buf.put_u32(self.length);
    }

    pub fn decode(buf: &Bytes) -> Result<Self> {
        if buf.len() < Self::LEN {
            return Err(Error::Incomplete);
        }
        let mut head = buf.slice(0..Self::LEN);
        if head.get_u32() != INTERNAL_MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = head.get_u16();
        if version != INTERNAL_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let kind = Kind::from_u16(head.get_u16())?;
        let length = head.get_u32();
        if length > MAX_BODY_BYTES {
            return Err(Error::FrameTooLarge);
        }
        Ok(Self { kind, length })
    }
}

fn put_str(buf: &mut BytesMut, value: &str) -> Result<()> {
    if value.len() > MAX_IDENT_BYTES {
        return Err(Error::FrameTooLarge);
    }
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_str(buf: &mut Bytes) -> Result<String> {
    let len = take_u32(buf)? as usize;
    if len > MAX_IDENT_BYTES || len > buf.remaining() {
        return Err(Error::Incomplete);
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::InvalidUtf8)
}

fn take_u8(buf: &mut Bytes) -> Result<u8> {
    if buf.remaining() < 1 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u8())
}

fn take_u16(buf: &mut Bytes) -> Result<u16> {
    if buf.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u16())
}

fn take_u32(buf: &mut Bytes) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u32())
}

fn take_u64(buf: &mut Bytes) -> Result<u64> {
    if buf.remaining() < 8 {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u64())
}

/// Reject trailing bytes: a body that does not describe itself exactly means
/// the peer and this decoder disagree about the layout.
fn expect_empty(buf: &Bytes) -> Result<()> {
    if buf.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(())
}

#[cfg(test)]
#[path = "internal_tests.rs"]
mod tests;
