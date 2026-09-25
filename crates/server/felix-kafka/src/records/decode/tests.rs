use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType,
};

use super::{MAX_BATCH_BYTES, Refused, decode};

fn record(sequence: i32, value: &[u8]) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: 42,
        producer_epoch: 0,
        timestamp_type: TimestampType::Creation,
        offset: i64::from(sequence),
        sequence,
        timestamp: 1_700_000_000_000,
        key: None,
        value: Some(Bytes::copy_from_slice(value)),
        headers: Default::default(),
    }
}

fn encode(records: &[Record], compression: Compression) -> Bytes {
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .expect("encode");
    buf.freeze()
}

#[test]
fn every_codec_decodes_to_the_values_and_the_producer_fields() {
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let batches = decode(encode(
            &[record(7, b"a"), record(8, b"bb"), record(9, b"ccc")],
            compression,
        ))
        .unwrap_or_else(|err| panic!("{compression:?}: {err:?}"));
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.values, vec!["a", "bb", "ccc"], "{compression:?}");
        assert_eq!((batch.producer_id, batch.base_sequence), (42, 7));
    }
}

#[test]
fn raw_snappy_as_librdkafka_sends_it_decodes_too() {
    // The same batch as the xerial-framed one the encoder writes, with its
    // records compressed as one raw snappy block instead.
    let plain = encode(&[record(0, b"hello")], Compression::None);
    let records = &plain[61..];
    let compressed = snap::raw::Encoder::new()
        .compress_vec(records)
        .expect("snappy");
    let mut batch = BytesMut::from(&plain[..61]);
    batch.put_slice(&compressed);
    // Length, then attributes with the snappy codec, then a fresh CRC.
    let length = (batch.len() - 12) as i32;
    batch[8..12].copy_from_slice(&length.to_be_bytes());
    batch[21..23].copy_from_slice(&2i16.to_be_bytes());
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let batches = decode(batch.freeze()).expect("decode");
    assert_eq!(batches[0].values, vec!["hello"]);
}

#[test]
fn keys_headers_and_null_values_are_counted_and_the_value_kept() {
    let mut keyed = record(0, b"v");
    keyed.key = Some(Bytes::from_static(b"k"));
    let mut headed = record(1, b"w");
    headed.headers.insert(
        StrBytes::from_static_str("trace"),
        Some(Bytes::from_static(b"1")),
    );
    let mut tombstone = record(2, b"");
    tombstone.value = None;
    let batches = decode(encode(&[keyed, headed, tombstone], Compression::None)).expect("decode");
    assert_eq!(batches[0].values, vec!["v", "w", ""]);
    assert_eq!((batches[0].keyed, batches[0].with_headers), (1, 1));
}

#[test]
fn a_legacy_message_set_is_refused() {
    let mut batch = BytesMut::from(&encode(&[record(0, b"a")], Compression::None)[..]);
    batch[16] = 1;
    assert_eq!(decode(batch.freeze()).unwrap_err(), Refused::LegacyFormat);
}

#[test]
fn a_transactional_batch_is_refused() {
    let mut txn = record(0, b"a");
    txn.transactional = true;
    assert_eq!(
        decode(encode(&[txn], Compression::None)).unwrap_err(),
        Refused::Transactional
    );
}

#[test]
fn a_bad_crc_is_corrupt() {
    let mut batch = BytesMut::from(&encode(&[record(0, b"abc")], Compression::None)[..]);
    let last = batch.len() - 1;
    batch[last] ^= 0xff;
    assert!(matches!(
        decode(batch.freeze()).unwrap_err(),
        Refused::Corrupt(_)
    ));
}

/// A small compressed batch that inflates past the bound stops there rather
/// than allocating whatever it asks for.
#[test]
fn decompression_is_bounded() {
    let huge = vec![0u8; MAX_BATCH_BYTES + 1024];
    for compression in [
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let batch = encode(&[record(0, &huge)], compression);
        assert!(batch.len() < 4 * 1024 * 1024, "{compression:?}");
        assert_eq!(
            decode(batch).unwrap_err(),
            Refused::TooLarge,
            "{compression:?}"
        );
    }
}

fn crc32c(data: &[u8]) -> u32 {
    // Castagnoli, bit by bit: slow and obviously right, which is what a test
    // wants.
    let mut crc = !0u32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}
