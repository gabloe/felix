use super::*;

#[test]
fn segment_header_round_trip() {
    let header = SegmentHeader::new(42, 1_700_000_000_000_000);
    let decoded = SegmentHeader::decode(&header.encode()).expect("decode");
    assert_eq!(decoded, header);
}

#[test]
fn segment_header_is_a_stable_golden_vector() {
    // Pinned bytes: a change here is a format change and must bump the
    // version and update docs/storage-format.md.
    let bytes = SegmentHeader::new(1, 2).encode();
    assert_eq!(
        bytes,
        [
            0x46, 0x4C, 0x53, 0x47, // magic "FLSG"
            0x00, 0x02, // version
            0x00, 0x00, // flags
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // base_offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // created_at_micros
            0x7A, 0x09, 0xE5, 0xB1, // header crc32
            0x00, 0x00, 0x00, 0x00, // reserved
        ]
    );
}

#[test]
fn record_is_a_stable_golden_vector() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 7, 9, b"hi");
    assert_eq!(
        buf,
        vec![
            0x00, 0x00, 0x00, 0x02, // payload_len
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, // timestamp
            0xC6, 0x54, 0xDE, 0x27, // header crc32 (bytes 0..20)
            0x24, 0x02, 0x15, 0x2C, // checksum (bytes 0..24 + payload)
            b'h', b'i',
        ]
    );
}

#[test]
fn record_round_trips_every_field() {
    let mut buf = Vec::new();
    let written = encode_record(&mut buf, 9, 1234, b"payload");
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(written, consumed);
    assert_eq!(consumed, buf.len() as u64);
    assert_eq!(decoded.header.offset, 9);
    assert_eq!(decoded.header.timestamp_micros, 1234);
    assert_eq!(decoded.header.payload_len, 7);
    assert_eq!(decoded.payload, Bytes::from_static(b"payload"));
}

#[test]
fn zero_length_payload_round_trips() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"");
    let (decoded, consumed) = decode_record(&buf).expect("decode");
    assert_eq!(consumed, RECORD_HEADER_LEN);
    assert!(decoded.payload.is_empty());
}

#[test]
fn records_decode_back_to_back() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 1, b"a");
    encode_record(&mut buf, 1, 2, b"bb");
    let (first, consumed) = decode_record(&buf).expect("first");
    assert_eq!(first.header.offset, 0);
    let (second, _) = decode_record(&buf[consumed as usize..]).expect("second");
    assert_eq!(second.header.offset, 1);
    assert_eq!(second.payload, Bytes::from_static(b"bb"));
}

#[test]
fn truncation_at_every_boundary_reports_truncated() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 3, 4, b"abcd");
    for cut in 0..buf.len() {
        let err = decode_record(&buf[..cut]).expect_err("short buffer");
        assert!(err.is_truncation(), "cut {cut} gave {err}");
    }
    assert!(decode_record(&buf).is_ok());
}

#[test]
fn payload_corruption_fails_the_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload");
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    let err = decode_record(&buf).expect_err("corrupt payload");
    assert!(matches!(err.kind, CorruptionKind::RecordChecksum { .. }));
    assert!(!err.is_truncation());
}

#[test]
fn header_corruption_fails_the_header_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, b"payload");
    // Flip a bit in the timestamp. The header checksum covers it, so this
    // is caught without reading the payload at all.
    buf[12] ^= 0x01;
    let err = decode_record(&buf).expect_err("corrupt header");
    assert!(matches!(
        err.kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn a_corrupt_length_is_caught_by_the_header_checksum() {
    let mut buf = Vec::new();
    encode_record(&mut buf, 4, 5, b"payload");
    // Damage only the length field. Before the header checksum this was
    // indistinguishable from an unfinished write: the claimed extent ran
    // past the data, so recovery truncated an acknowledged record. Now the
    // header itself reports the damage.
    buf[0..4].copy_from_slice(&9_999u32.to_be_bytes());
    let err = decode_record(&buf).expect_err("corrupt length");
    assert!(
        matches!(err.kind, CorruptionKind::RecordHeaderChecksum { .. }),
        "expected a header checksum failure, got {err}"
    );
}

#[test]
fn oversized_length_is_rejected_before_allocating() {
    // A header whose length is impossible but whose checksum is valid: the
    // shape that would reach an allocation if the bound were not checked.
    let mut buf = vec![0u8; RECORD_HEADER_LEN as usize];
    buf[0..4].copy_from_slice(&u32::MAX.to_be_bytes());
    let header_crc = crc32(&[&buf[0..20]]);
    buf[20..24].copy_from_slice(&header_crc.to_be_bytes());

    let err = decode_record(&buf).expect_err("oversized");
    assert!(
        matches!(
            err.kind,
            CorruptionKind::RecordTooLarge {
                payload_len: u32::MAX,
                limit: MAX_PAYLOAD_BYTES,
            }
        ),
        "got {err}"
    );
}

#[test]
fn a_garbage_header_is_rejected_before_its_length_is_believed() {
    // Random bytes almost never carry a valid header checksum, so the
    // length they happen to encode is never acted on.
    let buf = vec![0u8; RECORD_HEADER_LEN as usize];
    let err = decode_record(&buf).expect_err("garbage");
    assert!(matches!(
        err.kind,
        CorruptionKind::RecordHeaderChecksum { .. }
    ));
}

#[test]
fn max_size_payload_round_trips() {
    let payload = vec![0xA5u8; MAX_PAYLOAD_BYTES as usize];
    let mut buf = Vec::new();
    encode_record(&mut buf, 0, 0, &payload);
    let (decoded, _) = decode_record(&buf).expect("decode");
    assert_eq!(decoded.header.payload_len, MAX_PAYLOAD_BYTES);
    assert_eq!(decoded.payload.len(), payload.len());
}

#[test]
fn a_header_alone_locates_the_next_record() {
    let mut buf = Vec::new();
    let written = encode_record(&mut buf, 0, 0, b"some payload");
    // Only the header bytes are available, yet the next position is known
    // without ever touching the payload.
    let header = RecordHeader::decode(&buf[..RECORD_HEADER_LEN as usize]).expect("header");
    assert_eq!(header.encoded_len(), written);
}

#[test]
fn segment_header_rejects_bad_magic_version_and_flags() {
    let good = SegmentHeader::new(0, 0).encode();

    let mut bad = good;
    bad[0] ^= 0xFF;
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("magic").kind,
        CorruptionKind::SegmentMagic { .. }
    ));

    let mut bad = good;
    bad[4..6].copy_from_slice(&99u16.to_be_bytes());
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("version").kind,
        CorruptionKind::SegmentVersion { found: 99 }
    ));

    let mut bad = good;
    bad[6..8].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("flags").kind,
        CorruptionKind::SegmentFlags { found: 1 }
    ));

    let mut bad = good;
    bad[8] ^= 0xFF;
    assert!(matches!(
        SegmentHeader::decode(&bad).expect_err("checksum").kind,
        CorruptionKind::SegmentHeaderChecksum { .. }
    ));
}

#[test]
fn segment_header_truncation_is_reported_as_truncation() {
    let good = SegmentHeader::new(0, 0).encode();
    for cut in 0..good.len() {
        assert!(
            SegmentHeader::decode(&good[..cut])
                .expect_err("short")
                .is_truncation()
        );
    }
}

#[test]
fn index_header_and_entry_round_trip() {
    let header = IndexHeader { base_offset: 17 };
    assert_eq!(IndexHeader::decode(&header.encode()).expect("hdr"), header);

    let entry = IndexEntry {
        offset: 5,
        position: 64,
    };
    assert_eq!(IndexEntry::decode(&entry.encode()).expect("entry"), entry);
}

#[test]
fn index_header_rejects_bad_magic_and_version() {
    let good = IndexHeader { base_offset: 0 }.encode();
    let mut bad = good;
    bad[0] ^= 0xFF;
    assert!(matches!(
        IndexHeader::decode(&bad).expect_err("magic").kind,
        CorruptionKind::IndexMagic { .. }
    ));
    let mut bad = good;
    bad[4..6].copy_from_slice(&7u16.to_be_bytes());
    assert!(matches!(
        IndexHeader::decode(&bad).expect_err("version").kind,
        CorruptionKind::IndexVersion { found: 7 }
    ));
}

#[test]
fn offset_continuity_is_checked() {
    assert!(check_offset_continuity(4, 4).is_ok());
    let err = check_offset_continuity(4, 6).expect_err("gap");
    assert!(matches!(
        err.kind,
        CorruptionKind::OffsetOutOfOrder {
            expected: 4,
            found: 6
        }
    ));
}
