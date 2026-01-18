use bytes::Bytes;
use felix_wire::{Frame, Message};
use std::fs;

#[test]
fn vectors_match_frame_encoding() {
    let dir = "tests/vectors";
    for entry in fs::read_dir(dir).expect("read vectors dir") {
        let entry = entry.expect("entry");
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("read vector");
        let value: serde_json::Value = serde_json::from_str(&contents).expect("json");
        let payload_hex = value["payload_hex"].as_str().expect("payload_hex");
        let frame_hex = value["frame_hex"].as_str().expect("frame_hex");
        let payload = hex_to_bytes(payload_hex);
        let frame_expected = Bytes::from(hex_to_bytes(frame_hex));

        let frame = Frame::new(0, payload.into()).expect("frame");
        assert_eq!(
            frame.encode().as_ref(),
            frame_expected.as_ref(),
            "frame mismatch for {:?}",
            path
        );

        let decoded = Frame::decode(frame_expected.clone()).expect("decode");
        let message = Message::decode(decoded).expect("message decode");
        let encoded = message.encode().expect("message encode");
        assert_eq!(
            encoded.encode().as_ref(),
            frame_expected.as_ref(),
            "round trip mismatch for {:?}",
            path
        );
    }
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "hex length must be even");
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = from_hex_char(pair[0]) << 4;
            let lo = from_hex_char(pair[1]);
            hi | lo
        })
        .collect()
}

fn from_hex_char(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex char"),
    }
}
