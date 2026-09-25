//! A producer's record batches, as Felix payloads.
//!
//! Only v2 record batches are taken. The v0 and v1 message sets predate
//! idempotence and every client this listener supports writes v2, so a
//! legacy set is refused rather than half-supported.
//!
//! Decompression is bounded. A batch arrives inside a request of at most
//! 8 MiB, but a few megabytes of zstd can expand to far more memory than a
//! broker has; each batch stops inflating at [`MAX_BATCH_BYTES`].

use std::io::Read;

use anyhow::Context;
use bytes::{Buf, Bytes};
use kafka_protocol::records::{BatchDecodeInfo, Compression, RecordBatchDecoder};

/// The most a batch may decompress to.
pub(crate) const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// One producer batch, decoded.
#[derive(Debug)]
pub(crate) struct Batch {
    /// -1 for a producer that is not idempotent.
    pub(crate) producer_id: i64,
    pub(crate) base_sequence: i32,
    /// The record values. A null value is stored as an empty payload.
    pub(crate) values: Vec<Bytes>,
    /// Records that carried a key, and records that carried headers. Felix
    /// records have neither, so both are dropped; these say how many.
    pub(crate) keyed: u64,
    pub(crate) with_headers: u64,
}

/// Why a partition's records were not taken.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// A v0 or v1 message set.
    LegacyFormat,
    /// Part of a transaction, or a transaction marker.
    Transactional,
    /// Decompressed past [`MAX_BATCH_BYTES`].
    TooLarge,
    /// Truncated, a bad CRC, an unknown codec, or records that do not parse.
    Corrupt(String),
}

/// Decode every batch in a partition's `records`.
pub(crate) fn decode(records: Bytes) -> Result<Vec<Batch>, Refused> {
    let mut headers_view = records.clone();
    let infos = RecordBatchDecoder::decode_batch_info(&mut headers_view)
        .map_err(|err| Refused::Corrupt(format!("{err:#}")))?;
    // The header pass stops at the first batch that is not v2.
    if headers_view.has_remaining() {
        return Err(match magic(&headers_view) {
            Some(0 | 1) => Refused::LegacyFormat,
            _ => Refused::Corrupt("a record batch is not in the v2 format".to_string()),
        });
    }

    let mut rest = records;
    let mut batches = Vec::with_capacity(infos.len());
    for info in infos {
        if info.transactional || info.control {
            return Err(Refused::Transactional);
        }
        let set = RecordBatchDecoder::decode_with_custom_compression(
            &mut rest,
            Some(|data: &mut Bytes, compression| inflate(data, compression)),
        )
        .map_err(|err| {
            if err.downcast_ref::<TooLarge>().is_some() {
                Refused::TooLarge
            } else {
                Refused::Corrupt(format!("{err:#}"))
            }
        })?;
        batches.push(batch(&info, set.records));
    }
    Ok(batches)
}

fn batch(info: &BatchDecodeInfo, records: Vec<kafka_protocol::records::Record>) -> Batch {
    let mut keyed = 0;
    let mut with_headers = 0;
    let values = records
        .into_iter()
        .map(|record| {
            keyed += u64::from(record.key.is_some());
            with_headers += u64::from(!record.headers.is_empty());
            record.value.unwrap_or_default()
        })
        .collect();
    Batch {
        producer_id: info.producer_id,
        base_sequence: info.base_sequence,
        values,
        keyed,
        with_headers,
    }
}

/// The magic byte of the batch at the front of `buf`.
fn magic(buf: &Bytes) -> Option<i8> {
    buf.get(16).map(|byte| *byte as i8)
}

#[derive(Debug)]
struct TooLarge;

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a record batch decompresses to more than {MAX_BATCH_BYTES} bytes"
        )
    }
}

impl std::error::Error for TooLarge {}

fn inflate(data: &mut Bytes, compression: Compression) -> anyhow::Result<Bytes> {
    let data = data.split_to(data.len());
    match compression {
        Compression::None => Ok(data),
        Compression::Gzip => read_bounded(flate2::read::GzDecoder::new(data.reader())),
        Compression::Snappy => snappy(&data),
        Compression::Lz4 => read_bounded(lz4::Decoder::new(data.reader()).context("lz4 frame")?),
        Compression::Zstd => {
            read_bounded(zstd::stream::read::Decoder::new(data.reader()).context("zstd frame")?)
        }
    }
}

fn read_bounded(reader: impl Read) -> anyhow::Result<Bytes> {
    let mut out = Vec::new();
    reader
        .take(MAX_BATCH_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .context("decompress record batch")?;
    if out.len() > MAX_BATCH_BYTES {
        return Err(TooLarge.into());
    }
    Ok(out.into())
}

/// The Java client frames snappy the way snappy-java does (a magic header,
/// then length-prefixed blocks); librdkafka sends raw snappy. Both are taken.
fn snappy(data: &[u8]) -> anyhow::Result<Bytes> {
    const XERIAL: &[u8; 8] = b"\x82SNAPPY\x00";
    const XERIAL_HEADER: usize = 16;
    let mut blocks: Vec<&[u8]> = Vec::new();
    if data.starts_with(XERIAL) {
        let mut rest = data.get(XERIAL_HEADER..).context("snappy header")?;
        while !rest.is_empty() {
            let (len, tail) = rest
                .split_first_chunk::<4>()
                .context("snappy block length")?;
            let len = u32::from_be_bytes(*len) as usize;
            let block = tail.get(..len).context("snappy block")?;
            blocks.push(block);
            rest = &tail[len..];
        }
    } else {
        blocks.push(data);
    }
    let mut total = 0usize;
    for block in &blocks {
        total += snap::raw::decompress_len(block).context("snappy length")?;
        if total > MAX_BATCH_BYTES {
            return Err(TooLarge.into());
        }
    }
    let mut out = vec![0u8; total];
    let mut at = 0;
    for block in blocks {
        at += snap::raw::Decoder::new()
            .decompress(block, &mut out[at..])
            .context("snappy block")?;
    }
    out.truncate(at);
    Ok(out.into())
}

#[cfg(test)]
mod tests;
