//! The broker's binary acknowledgement of an acked publish.
//!
//! ```text
//! u8  status        (0 = ok, 1 = error)
//! u64 request_id
//! u16 message_len   (0 when status = ok)
//! u8[message_len] message
//! ```
//!
//! With FLAG_BINARY_PUBLISH_ACK_OWNER, the batch was forwarded and the owner
//! follows:
//!
//! ```text
//! u16 node_id_len
//! u8[node_id_len] node_id
//! u16 addr_len      (0 when the owner's client address is not published)
//! u8[addr_len] addr
//! u64 generation
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::Error as SerdeError;

use crate::client::flags::{FLAG_BINARY_PUBLISH_ACK, FLAG_BINARY_PUBLISH_ACK_OWNER};
use crate::client::frame::{Frame, FrameHeader};
use crate::error::{Error, Result};

const ACK_STATUS_OK: u8 = 0;
const ACK_STATUS_ERROR: u8 = 1;

/// Broker → client acknowledgement for an acked binary publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub request_id: u64,
    /// `None` on success, `Some(message)` when the publish failed.
    pub error: Option<String>,
    /// Set when this batch was forwarded, naming the shard's owner.
    ///
    /// `None` means the broker handled it itself -- or predates the hint, or
    /// was talking to a client that did not advertise it. All three are the
    /// same thing to a caller: no better place to send the next batch is known.
    pub forwarded_to: Option<PublishOwner>,
}

/// The broker that owns the shard a forwarded batch went to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOwner {
    /// Who owns it, so a client recognises an owner it already knows.
    pub node_id: String,
    /// `host:port` the owner serves *clients* on. `None` when the cluster has
    /// not been told where clients reach it -- the same gap `NotLeader` has,
    /// and the same consequence: there is nowhere to route to, so a client
    /// keeps publishing where it is.
    pub addr: Option<String>,
    /// The ownership generation this answer was true for. A client holding a
    /// cached owner can tell a newer answer from an older one rather than
    /// letting two brokers mid-rebalance overwrite each other.
    pub generation: u64,
}

/// Encode a publish ack into a full framed buffer (header included).
pub fn encode_publish_ack_bytes(request_id: u64, error: Option<&str>) -> Result<Bytes> {
    encode_publish_ack_bytes_owned(request_id, error, None)
}

/// Encode a publish ack, optionally naming the owner a forwarded batch went to.
///
/// `forwarded_to` sets `FLAG_BINARY_PUBLISH_ACK_OWNER`, so the caller must only
/// pass it for a client that advertised the bit -- one that did not will reject
/// the frame, and the frame it rejects acknowledges a publish that succeeded.
pub fn encode_publish_ack_bytes_owned(
    request_id: u64,
    error: Option<&str>,
    forwarded_to: Option<&PublishOwner>,
) -> Result<Bytes> {
    let message = error.unwrap_or("");
    let message_bytes = message.as_bytes();
    let message_len = u16::try_from(message_bytes.len()).map_err(|_| Error::FrameTooLarge)?;
    let mut payload_len = 1 + 8 + 2 + message_bytes.len();

    let owner = match forwarded_to {
        Some(owner) => {
            let node_id = owner.node_id.as_bytes();
            let addr = owner.addr.as_deref().unwrap_or("").as_bytes();
            let node_id_len = u16::try_from(node_id.len()).map_err(|_| Error::FrameTooLarge)?;
            let addr_len = u16::try_from(addr.len()).map_err(|_| Error::FrameTooLarge)?;
            payload_len += 2 + node_id.len() + 2 + addr.len() + 8;
            Some((node_id, node_id_len, addr, addr_len, owner.generation))
        }
        None => None,
    };

    let flags = if owner.is_some() {
        FLAG_BINARY_PUBLISH_ACK | FLAG_BINARY_PUBLISH_ACK_OWNER
    } else {
        FLAG_BINARY_PUBLISH_ACK
    };

    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + payload_len);
    FrameHeader::new(flags, payload_len as u32).encode(&mut buf);
    buf.put_u8(if error.is_some() {
        ACK_STATUS_ERROR
    } else {
        ACK_STATUS_OK
    });
    buf.put_u64(request_id);
    buf.put_u16(message_len);
    buf.extend_from_slice(message_bytes);
    if let Some((node_id, node_id_len, addr, addr_len, generation)) = owner {
        buf.put_u16(node_id_len);
        buf.extend_from_slice(node_id);
        buf.put_u16(addr_len);
        buf.extend_from_slice(addr);
        buf.put_u64(generation);
    }
    Ok(buf.freeze())
}

/// Decode a publish ack frame.
pub fn decode_publish_ack(frame: &Frame) -> Result<PublishAck> {
    let mut buf = frame.payload.clone();
    if buf.remaining() < 11 {
        return Err(Error::Incomplete);
    }
    let status = buf.get_u8();
    let request_id = buf.get_u64();
    let message_len = buf.get_u16() as usize;
    if buf.remaining() < message_len {
        return Err(Error::Incomplete);
    }
    let message_bytes = buf.copy_to_bytes(message_len);
    let error = match status {
        ACK_STATUS_OK => None,
        ACK_STATUS_ERROR => Some(
            String::from_utf8(message_bytes.to_vec())
                .map_err(|_| Error::Deserialize(SerdeError::custom("invalid ack message")))?,
        ),
        _ => return Err(Error::Deserialize(SerdeError::custom("invalid ack status"))),
    };
    let forwarded_to = if frame.header.flags & FLAG_BINARY_PUBLISH_ACK_OWNER != 0 {
        if buf.remaining() < 2 {
            return Err(Error::Incomplete);
        }
        let node_id_len = buf.get_u16() as usize;
        if buf.remaining() < node_id_len + 2 {
            return Err(Error::Incomplete);
        }
        let node_id = String::from_utf8(buf.copy_to_bytes(node_id_len).to_vec())
            .map_err(|_| Error::Deserialize(SerdeError::custom("invalid owner node id")))?;
        let addr_len = buf.get_u16() as usize;
        if buf.remaining() < addr_len + 8 {
            return Err(Error::Incomplete);
        }
        let addr = String::from_utf8(buf.copy_to_bytes(addr_len).to_vec())
            .map_err(|_| Error::Deserialize(SerdeError::custom("invalid owner address")))?;
        let generation = buf.get_u64();
        Some(PublishOwner {
            node_id,
            // Empty means "not published", which is different from an address
            // that happens to be empty -- there is no such address.
            addr: (!addr.is_empty()).then_some(addr),
            generation,
        })
    } else {
        None
    };
    Ok(PublishAck {
        request_id,
        error,
        forwarded_to,
    })
}

#[cfg(test)]
mod tests;
