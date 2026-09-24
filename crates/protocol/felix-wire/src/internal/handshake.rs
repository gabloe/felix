//! The exchange that opens every peer connection.

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
