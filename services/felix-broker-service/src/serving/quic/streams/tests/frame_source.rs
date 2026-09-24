//! Frame sources that end without a frame.

use super::*;

#[tokio::test]
async fn delay_frame_source_returns_none() -> Result<()> {
    let mut source = DelayFrameSource {
        delay: Duration::from_millis(1),
    };
    let mut scratch = BytesMut::new();
    let frame = source.next_frame(1024, &mut scratch).await?;
    assert!(frame.is_none());
    Ok(())
}

#[tokio::test]
async fn pending_frame_source_returns_none() -> Result<()> {
    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let mut scratch = BytesMut::new();
    ready.store(true, Ordering::Relaxed);
    let frame = source.next_frame(1024, &mut scratch).await?;
    assert!(frame.is_none());
    Ok(())
}
