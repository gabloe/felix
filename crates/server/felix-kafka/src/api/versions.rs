//! `ApiVersions`: what this listener speaks.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::ApiVersionsResponse;
use kafka_protocol::messages::api_versions_response::ApiVersion;

use super::SUPPORTED;

pub(super) const UNSUPPORTED_VERSION: i16 = 35;

/// The advertised ranges, answered at a version this listener supports.
///
/// The group APIs answered only to refuse them are not advertised: a client
/// that finds `FindCoordinator` refused has no reason to try them.
pub(super) fn answer(version: i16) -> Result<(Bytes, i16)> {
    let response = ApiVersionsResponse::default().with_api_keys(
        SUPPORTED
            .iter()
            .map(|(key, min, max)| {
                ApiVersion::default()
                    .with_api_key(*key as i16)
                    .with_min_version(*min)
                    .with_max_version(*max)
            })
            .collect(),
    );
    super::encode(&response, version, 0)
}

/// `UNSUPPORTED_VERSION`, in the v0 shape whatever version was asked for.
///
/// The client cannot know which shape to parse until it learns its version
/// was refused, so the protocol fixes this one answer at v0 (KIP-511). It lists
/// the `ApiVersions` range so the client retries at a version that works;
/// librdkafka opens with v3 and falls back on exactly this.
pub(super) fn unsupported() -> Bytes {
    let (min, max) = SUPPORTED
        .iter()
        .find(|(key, _, _)| *key == kafka_protocol::messages::ApiKey::ApiVersions)
        .map(|(_, min, max)| (*min, *max))
        .unwrap_or((0, 0));
    let mut out = BytesMut::with_capacity(12);
    out.put_i16(UNSUPPORTED_VERSION);
    out.put_i32(1);
    out.put_i16(kafka_protocol::messages::ApiKey::ApiVersions as i16);
    out.put_i16(min);
    out.put_i16(max);
    out.freeze()
}
