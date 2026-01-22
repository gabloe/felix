// Wire helpers for felix-wire framing, ack handling, and telemetry logging.
pub(crate) use self::ack::maybe_wait_for_ack;
pub(crate) use self::bench_ts::{
    bench_embed_ts_enabled, maybe_append_publish_ts, maybe_append_publish_ts_batch,
    record_e2e_latency,
};
pub(crate) use self::decode_log::log_decode_error;
pub(crate) use self::frame_io::{
    read_frame_cache_timed_into, read_frame_into, read_message, write_frame_parts, write_message,
};

mod ack;
mod bench_ts;
mod decode_log;
mod frame_io;
