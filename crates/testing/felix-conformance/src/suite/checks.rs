//! Small assertions on broker responses, each an error naming what differed.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_wire::Message;

pub(crate) fn parse_subscribe_response(response: Option<Message>) -> Result<u64> {
    match response {
        Some(Message::Subscribed {
            subscription_id, ..
        }) => Ok(subscription_id),
        other => Err(anyhow!("subscribe failed: {other:?}")),
    }
}

pub(crate) fn ensure_publish_ok(response: Option<Message>, request_id: u64) -> Result<()> {
    if response != Some(Message::PublishOk { request_id }) {
        return Err(anyhow!("publish failed: {response:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_ok_response(response: Option<Message>, context: &str) -> Result<()> {
    if response != Some(Message::Ok) {
        return Err(anyhow!("{context} failed: {response:?}"));
    }
    Ok(())
}

pub(crate) fn parse_cache_get_response(response: Option<Message>) -> Result<Option<Bytes>> {
    match response {
        Some(Message::CacheValue { value, .. }) => Ok(value),
        other => Err(anyhow!("unexpected cache response: {other:?}")),
    }
}

pub(crate) fn ensure_event_order(received: &[Vec<u8>]) -> Result<()> {
    if received != [b"alpha".to_vec(), b"beta".to_vec()] {
        return Err(anyhow!("unexpected event order: {received:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_cache_value(
    value: Option<Bytes>,
    expected: Bytes,
    context: &str,
) -> Result<()> {
    if value != Some(expected) {
        return Err(anyhow!("{context} mismatch: {value:?}"));
    }
    Ok(())
}

pub(crate) fn ensure_cache_expired(value: Option<Bytes>, context: &str) -> Result<()> {
    if value.is_some() {
        return Err(anyhow!("{context}"));
    }
    Ok(())
}

pub(crate) fn ensure_client_event(payload: &Bytes, expected: Bytes) -> Result<()> {
    if payload != &expected {
        return Err(anyhow!("client event mismatch: {:?}", payload));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
