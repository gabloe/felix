//! The request loop: frames in, frames out, one at a time.

use anyhow::{Context, Result, bail};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::Shared;
use crate::api::{self, Answer, Session};

/// The largest request accepted. A read-only listener's requests are small;
/// the cap is what keeps a hostile length prefix from allocating gigabytes.
const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

pub(super) async fn serve<S>(
    shared: &Shared,
    mut stream: S,
    shutdown: &CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = Session::new(shared);
    loop {
        let mut size = [0u8; 4];
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            read = stream.read_exact(&mut size) => {
                if read.is_err() {
                    // The client went away between requests: a normal close.
                    return Ok(());
                }
            }
        }
        let size = i32::from_be_bytes(size);
        let size = usize::try_from(size)
            .ok()
            .filter(|size| (8..=MAX_REQUEST_BYTES).contains(size))
            .with_context(|| {
                crate::metrics::refused("frame_size");
                format!("request size {size} is out of range")
            })?;
        let mut frame = vec![0u8; size];
        stream
            .read_exact(&mut frame)
            .await
            .context("read request body")?;

        match api::handle(shared, &mut session, Bytes::from(frame), shutdown).await? {
            Answer::Respond {
                correlation_id,
                header_version,
                body,
            } => {
                let mut out = BytesMut::with_capacity(body.len() + 9);
                let header_len = if header_version >= 1 { 5 } else { 4 };
                out.put_i32((header_len + body.len()) as i32);
                out.put_i32(correlation_id);
                if header_version >= 1 {
                    // No tagged fields.
                    out.put_u8(0);
                }
                out.put_slice(&body);
                stream.write_all(&out).await.context("write response")?;
            }
            Answer::Silent => {}
            Answer::Close(reason) => bail!("closing the connection: {reason}"),
        }
        if session.closing() {
            return Ok(());
        }
    }
}
