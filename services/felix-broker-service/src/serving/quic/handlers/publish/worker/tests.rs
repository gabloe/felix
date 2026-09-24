use anyhow::Result;
use bytes::Bytes;
use felix_storage::EphemeralCache;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn build_publish_context_clamps_worker_and_queue_minimums() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo", Default::default())
        .await?;

    let config = BrokerConfig {
        pub_workers_per_conn: 0,
        pub_queue_depth: 0,
        publish_queue_wait_timeout_ms: 17,
        ..BrokerConfig::default()
    };
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    assert_eq!(publish_ctx.worker_count, 1);
    assert_eq!(publish_ctx.workers.len(), 1);
    assert_eq!(publish_ctx.wait_timeout, Duration::from_millis(17));

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
            },
            payloads: vec![Bytes::from_static(b"ok")],
            response: Some(response_tx),
            admission_permit: None,
        })
        .await
        .expect("enqueue publish");
    response_rx.await.expect("worker response")?;
    Ok(())
}

#[tokio::test]
async fn build_publish_context_worker_returns_publish_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let config = BrokerConfig::default();
    let publish_ctx = build_publish_context(broker, &config, ClusterContext::default());

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            payloads: vec![Bytes::from_static(b"payload")],
            response: Some(response_tx),
            admission_permit: None,
        })
        .await
        .expect("enqueue publish");
    let err = response_rx
        .await
        .expect("worker response")
        .expect_err("publish should fail");
    assert!(err.to_string().contains("stream"));
    Ok(())
}

/// The write fence, on the worker. Each of these admits a publish while the
/// shard is served here, lets a move close the fence while the job waits in
/// the queue, and then lets the worker claim it -- which must refuse it and
/// write nothing, since the drained report may already have gone out.
mod fence {
    use std::collections::HashMap;

    use super::*;
    use crate::serving::quic::handlers::publish::route::{
        Authority, PublishRoute, local_shard_key, publish_target, resolve_route,
    };
    use crate::test_support::leader::{DURABLE, EPHEMERAL, Leader, NAMESPACE, TENANT, stream_key};

    #[derive(Clone, Copy)]
    enum Kind {
        Plain,
        Idempotent,
    }

    async fn admitted_then_fenced(stream: &str, kind: Kind) {
        let mut leader = Leader::start().await;
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                ..ClusterContext::default()
            },
        );

        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
            },
            &mut HashMap::new(),
            &mut String::new(),
            TENANT,
            NAMESPACE,
            stream,
            0,
        )
        .await;
        let target = match (kind, route) {
            (Kind::Plain, route) => publish_target(
                route,
                &publish_ctx,
                TENANT,
                NAMESPACE,
                stream,
                0,
                felix_wire::internal::AckMode::OnCommit,
                "",
            )
            .expect("admitted"),
            (Kind::Idempotent, PublishRoute::Local { handle, generation }) => {
                PublishTarget::Idempotent {
                    handle,
                    shard: local_shard_key(&publish_ctx, TENANT, NAMESPACE, stream, 0),
                    generation,
                    producer_id: leader.broker.new_producer_id(),
                    sequence: 0,
                }
            }
            (_, other) => panic!("admission should serve it here: {other:?}"),
        };

        // The move lands while the job sits in the queue.
        leader.fence_move(&stream_key(stream));

        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"late")],
                response: Some(response_tx),
                admission_permit: None,
            })
            .await
            .expect("enqueue publish");
        let answer = response_rx.await.expect("worker response");
        assert!(
            answer.is_err(),
            "a publish claimed after the fence closed was acknowledged"
        );
        let tail = leader.tail(stream).await;
        assert_eq!(tail, 0, "the refused publish was written anyway");
    }

    #[tokio::test]
    async fn a_durable_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(DURABLE, Kind::Plain).await;
    }

    #[tokio::test]
    async fn an_ephemeral_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(EPHEMERAL, Kind::Plain).await;
    }

    #[tokio::test]
    async fn an_idempotent_publish_claimed_after_the_fence_is_refused() {
        admitted_then_fenced(DURABLE, Kind::Idempotent).await;
    }

    /// The control: the same path with no move in between is acknowledged,
    /// so the refusals above are the fence and not a broken fixture.
    #[tokio::test]
    async fn a_publish_with_no_move_in_between_is_written() {
        let leader = Leader::start().await;
        let publish_ctx = build_publish_context(
            Arc::clone(&leader.broker),
            &BrokerConfig::default(),
            ClusterContext {
                ingress: Some(Arc::clone(&leader.ingress)),
                ..ClusterContext::default()
            },
        );
        let route = resolve_route(
            &leader.broker,
            Authority {
                ingress: Some(&leader.ingress),
                lease: None,
            },
            &mut HashMap::new(),
            &mut String::new(),
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
        )
        .await;
        let target = publish_target(
            route,
            &publish_ctx,
            TENANT,
            NAMESPACE,
            DURABLE,
            0,
            felix_wire::internal::AckMode::OnCommit,
            "",
        )
        .expect("admitted");
        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                target,
                payloads: vec![Bytes::from_static(b"on time")],
                response: Some(response_tx),
                admission_permit: None,
            })
            .await
            .expect("enqueue publish");
        response_rx
            .await
            .expect("worker response")
            .expect("acknowledged");
        let tail = leader.tail(DURABLE).await;
        assert_eq!(tail, 1);
    }
}
