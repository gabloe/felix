//! Unit tests for the publish path: admission, sharding, enqueue policy, depth
//! accounting, ack handling, and the control/uni handler entry points.

mod ack;
mod admission;
mod control_batch;
mod control_binary;
mod control_message;
mod idempotent_acks;
mod ingress;
mod ownership_gate;
mod routing;
mod stream_cache;
mod uni;

use super::ingress::{enqueue_publish, publish_worker_index};
use super::route::{PublishRoute, publish_target, resolve_route};
use super::stream_cache::{push_decimal, push_stream_cache_key};
use super::*;
use crate::serving::auth::AuthContext;
use crate::serving::forward::ForwardTarget;
use crate::serving::quic::errors::AckEnqueueError;
use crate::serving::quic::{
    ACK_HI_WATER, ACK_TIMEOUT_THRESHOLD, ACK_TIMEOUT_WINDOW, GLOBAL_ACK_DEPTH,
};
use bytes::Bytes;
use felix_authz::PermissionMatcher;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_wire::{Frame, Message};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::sync::{mpsc, watch};

// These publish-path tests don't exercise subscription delivery; this just gives
// `PublishContext::lane_manager` a real (if unused) instance to satisfy the type.
fn test_lane_manager() -> Arc<WriterLaneManager> {
    WriterLaneManager::new(&crate::config::BrokerConfig::default())
}

fn reset_global_ack_depth() {
    GLOBAL_ACK_DEPTH.store(0, Ordering::Relaxed);
}

fn make_publish_context(
    buffer: usize,
) -> (
    PublishContext,
    mpsc::Receiver<PublishJob>,
    mpsc::Sender<PublishJob>,
) {
    let (tx, rx) = mpsc::channel(buffer);
    let context = PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        workers: Arc::new(vec![tx.clone()]),
        worker_count: 1,
        depth: Arc::new(AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(100),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: test_lane_manager(),
        ingress_wait: false,
    };
    (context, rx, tx)
}

fn make_job() -> PublishJob {
    PublishJob {
        target: PublishTarget::Named {
            tenant_id: "tenant".to_string(),
            namespace: "ns".to_string(),
            stream: "stream".to_string(),
        },
        payloads: vec![Bytes::from_static(b"payload")],
        response: None,
        admission_permit: None,
    }
}

fn make_auth_ctx(tenant_id: &str, perms: &[&str]) -> AuthContext {
    let patterns = perms.iter().map(|p| (*p).to_string()).collect::<Vec<_>>();
    let matcher = PermissionMatcher::from_strings(&patterns).expect("parse perms");
    AuthContext {
        tenant_id: tenant_id.to_string(),
        matcher,
        token: "test-token".to_string(),
    }
}

fn make_binary_publish_frame(tenant_id: &str, namespace: &str, stream: &str) -> Frame {
    let payloads = vec![b"payload".to_vec()];
    felix_wire::binary::encode_publish_batch(tenant_id, namespace, stream, &payloads)
        .expect("encode publish batch")
}
