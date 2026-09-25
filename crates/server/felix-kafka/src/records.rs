//! Felix log records as a Kafka v2 record batch, and a producer's batches as
//! Felix payloads (`decode`).
//!
//! Offsets are not translated: a Felix shard's offsets start at zero and are
//! contiguous, which is Kafka's model already. Timestamps are the broker's
//! append time in milliseconds. Felix records have no key and no headers.

pub(crate) mod decode;

use bytes::{Bytes, BytesMut};
use felix_storage::log::LogRecord;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

/// Encode `records` as one uncompressed batch.
///
/// The encoder starts a new batch wherever `offset - sequence` changes, so
/// each record's sequence is set to keep that constant. With no producer id
/// the batch's base sequence comes out as -1, which is what a client expects
/// from a non-idempotent batch.
pub(crate) fn encode_batch(records: &[LogRecord]) -> anyhow::Result<Bytes> {
    let Some(first) = records.first() else {
        return Ok(Bytes::new());
    };
    let first_offset = first.offset;
    let converted: Vec<Record> = records
        .iter()
        .map(|record| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: record.offset as i64,
            sequence: (record.offset - first_offset) as i32 - 1,
            timestamp: timestamp_ms(record),
            key: None,
            value: Some(record.payload.clone()),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &converted,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(buf.freeze())
}

/// A record's timestamp as Kafka carries it: milliseconds since the epoch.
pub(crate) fn timestamp_ms(record: &LogRecord) -> i64 {
    (record.timestamp_micros / 1_000) as i64
}

#[cfg(test)]
mod tests;
