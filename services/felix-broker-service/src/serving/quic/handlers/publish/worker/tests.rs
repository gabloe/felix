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
