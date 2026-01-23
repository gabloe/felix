// Decode error logging for frame/message parsing.
use felix_wire::Frame;

#[cfg(feature = "telemetry")]
use crate::counters::DECODE_ERROR_LOGS;

#[cfg(feature = "telemetry")]
const DECODE_ERROR_LOG_LIMIT: usize = 20;

#[cfg(feature = "telemetry")]
pub(crate) fn log_decode_error(context: &str, err: &anyhow::Error, frame: &Frame) {
    let count = DECODE_ERROR_LOGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if count >= DECODE_ERROR_LOG_LIMIT {
        return;
    }
    let preview_len = frame.payload.len().min(64);
    let preview = &frame.payload[..preview_len];
    let hex = preview
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let printable = preview
        .iter()
        .map(|b| {
            let c = *b as char;
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '.'
            }
        })
        .collect::<String>();
    eprintln!(
        "felix-client decode error: context={context} err={err} frame_len={} payload_len={} preview_hex=\"{hex}\" preview_printable=\"{printable}\"",
        frame.header.length,
        frame.payload.len()
    );
}

#[cfg(not(feature = "telemetry"))]
pub(crate) fn log_decode_error(_context: &str, _err: &anyhow::Error, _frame: &Frame) {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use felix_wire::FrameHeader;

    #[test]
    fn log_decode_error_does_not_panic() {
        let frame = Frame {
            header: FrameHeader {
                magic: 0x42,
                version: 1,
                flags: 0,
                length: 10,
            },
            payload: Bytes::from_static(b"test error"),
        };
        let err = anyhow::anyhow!("test error");
        log_decode_error("test_context", &err, &frame);
    }

    #[test]
    fn log_decode_error_with_non_printable_bytes() {
        let frame = Frame {
            header: FrameHeader {
                magic: 0x42,
                version: 1,
                flags: 0,
                length: 10,
            },
            payload: Bytes::from(vec![0x00, 0x01, 0x02, 0xFF, b'A', b'B', b'C']),
        };
        let err = anyhow::anyhow!("test error");
        log_decode_error("binary_data", &err, &frame);
    }

    #[test]
    fn log_decode_error_with_large_payload() {
        let mut data = vec![b'X'; 100];
        data[50] = 0x00; // Add non-printable char
        let frame = Frame {
            header: FrameHeader {
                magic: 0x42,
                version: 1,
                flags: 0,
                length: 100,
            },
            payload: Bytes::from(data),
        };
        let err = anyhow::anyhow!("test error");
        log_decode_error("large_payload", &err, &frame);
    }
}
