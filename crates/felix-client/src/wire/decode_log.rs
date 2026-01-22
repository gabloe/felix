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
