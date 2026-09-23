//! The in-flight byte budget every publish is admitted against.
//!
//! Shared by all of a client's publishers. A publish holds its bytes from
//! before it is queued until the broker answers, so a slow broker makes
//! callers wait for room rather than letting the client buffer without
//! limit, and a publish larger than the whole budget fails at once.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct PublishAdmission {
    semaphore: Arc<Semaphore>,
    limit: usize,
}

impl PublishAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.clamp(1, u32::MAX as usize);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    pub(super) async fn acquire(&self, wire_bytes: usize) -> Result<OwnedSemaphorePermit> {
        let wire_bytes = wire_bytes.max(1);
        if wire_bytes > self.limit {
            return Err(anyhow::anyhow!(
                "publish frame estimate {wire_bytes} exceeds in-flight byte limit {}",
                self.limit
            ));
        }
        self.semaphore
            .clone()
            .acquire_many_owned(wire_bytes as u32)
            .await
            .map_err(|_| anyhow::anyhow!("publish admission closed"))
    }
}
