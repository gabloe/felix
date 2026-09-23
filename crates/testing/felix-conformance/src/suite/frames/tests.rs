use bytes::Bytes;
use felix_wire::binary;

use super::*;

struct TestReader {
    data: Vec<u8>,
    pos: usize,
    read_calls: usize,
    error_on_call: Option<(usize, ReadExactError)>,
}

impl TestReader {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            pos: 0,
            read_calls: 0,
            error_on_call: None,
        }
    }

    fn with_error_on_call(mut self, call: usize, error: ReadExactError) -> Self {
        self.error_on_call = Some((call, error));
        self
    }
}

impl FrameReader for TestReader {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ReadExactError>> + 'a>> {
        Box::pin(async move {
            self.read_calls += 1;
            if let Some((call, err)) = &self.error_on_call
                && *call == self.read_calls
            {
                return Err(err.clone());
            }
            let remaining = self.data.len().saturating_sub(self.pos);
            if remaining < buf.len() {
                let read = remaining;
                if read > 0 {
                    buf[..read].copy_from_slice(&self.data[self.pos..self.pos + read]);
                    self.pos += read;
                }
                return Err(ReadExactError::FinishedEarly(read));
            }
            buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
            self.pos += buf.len();
            Ok(())
        })
    }
}

fn encode_frame_bytes(flags: u16, payload: &[u8]) -> Vec<u8> {
    let header = FrameHeader::new(flags, payload.len() as u32);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    header.encode_into(&mut header_bytes);
    let mut bytes = Vec::with_capacity(FrameHeader::LEN + payload.len());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(payload);
    bytes
}

fn message_frame(message: Message) -> Frame {
    message.encode().expect("encode message")
}

fn binary_event_frame(subscription_id: u64, payloads: &[Bytes]) -> Frame {
    let bytes =
        binary::encode_event_batch_bytes(subscription_id, payloads).expect("encode binary batch");
    Frame::decode(bytes).expect("decode frame")
}

#[tokio::test]
async fn read_frame_success_and_errors() {
    let payload = b"hello";
    let bytes = encode_frame_bytes(0, payload);
    let mut reader = TestReader::new(bytes);
    let frame = read_frame(&mut reader).await.expect("ok").expect("frame");
    assert_eq!(frame.payload, Bytes::from_static(payload));

    let mut early = TestReader::new(Vec::new());
    assert!(read_frame(&mut early).await.expect("ok").is_none());

    let mut error = TestReader::new(Vec::new())
        .with_error_on_call(1, ReadExactError::ReadError(quinn::ReadError::ClosedStream));
    assert!(read_frame(&mut error).await.is_err());

    let bad_header = encode_frame_bytes(0, payload);
    let mut bad_magic = TestReader::new(bad_header);
    bad_magic.data[0] = 0x00;
    assert!(read_frame(&mut bad_magic).await.is_err());

    let mut payload_error = TestReader::new(encode_frame_bytes(0, payload))
        .with_error_on_call(2, ReadExactError::ReadError(quinn::ReadError::ClosedStream));
    assert!(read_frame(&mut payload_error).await.is_err());

    let mut short_payload = TestReader::new(encode_frame_bytes(0, payload));
    short_payload.data.truncate(FrameHeader::LEN + 2);
    assert!(read_frame(&mut short_payload).await.is_err());
}

#[test]
fn handle_event_frame_binary_paths() {
    let mut pending = VecDeque::new();
    let frame = binary_event_frame(7, &[Bytes::from_static(b"one")]);
    assert!(handle_event_frame(7, frame.clone(), &mut pending, true).is_err());

    let mut pending = VecDeque::new();
    assert!(handle_event_frame(8, frame.clone(), &mut pending, false).is_err());

    let mut pending = VecDeque::new();
    handle_event_frame(7, frame, &mut pending, false).expect("ok");
    assert_eq!(pending.pop_front(), Some(b"one".to_vec()));

    let mut pending = VecDeque::new();
    let bad = Frame {
        header: FrameHeader::new(FLAG_BINARY_EVENT_BATCH, 2),
        payload: Bytes::from_static(b"hi"),
    };
    assert!(handle_event_frame(7, bad, &mut pending, false).is_err());
}

#[test]
fn handle_event_frame_message_paths() {
    let mut pending = VecDeque::new();
    let hello = message_frame(Message::EventStreamHello { subscription_id: 9 });
    assert!(handle_event_frame(9, hello.clone(), &mut pending, false).is_err());
    assert!(handle_event_frame(8, hello.clone(), &mut pending, true).is_err());
    handle_event_frame(9, hello, &mut pending, true).expect("ok");

    let mut pending = VecDeque::new();
    let other = message_frame(Message::Ok);
    assert!(handle_event_frame(1, other, &mut pending, false).is_err());
}
