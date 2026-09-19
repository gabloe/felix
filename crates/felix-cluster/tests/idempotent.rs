//! Idempotent producers against a real cluster: a batch sent twice lands
//! once, a batch sent to the wrong broker is told where the leader is, and a
//! producer that skips ahead is stopped.
//!
//! Through the client-facing API, on a `Quorum` stream replicated three ways,
//! because that is the case the feature exists for: a `Quorum` publish whose
//! acknowledgement never arrived, re-sent without a second copy landing.
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{PublishRefusalReason, PublishRefused};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, client};
use serial_test::serial;

const STREAM: &str = "orders";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    }
}

/// Every record on the stream, replayed from the start on the leader. The
/// harness publishes one probe of its own while the cluster settles; that is
/// not the test's.
async fn replay(cluster: &Cluster, owner: &str, expected: usize) -> Result<Vec<Vec<u8>>> {
    let (_client, mut subscription) = cluster.replay_on(owner, STREAM).await?;
    let mut records = Vec::new();
    while records.len() < expected {
        let event = tokio::time::timeout(DELIVERY_TIMEOUT, subscription.next_event())
            .await
            .context("no delivery within the timeout")??
            .context("subscription closed")?;
        if event.payload.as_ref() == b"harness-probe" {
            continue;
        }
        records.push(event.payload.to_vec());
    }
    // One more would be a duplicate; give it a moment to show up.
    if let Ok(Ok(Some(extra))) =
        tokio::time::timeout(Duration::from_millis(500), subscription.next_event()).await
    {
        anyhow::bail!(
            "an extra record landed: {:?}",
            String::from_utf8_lossy(&extra.payload)
        );
    }
    Ok(records)
}

/// **A batch sent twice lands once**, and the second send is acknowledged
/// like the first: the producer cannot tell, which is the point.
///
/// The producer here goes through a cluster client connected wherever, so
/// the first batch is refused by a non-leader and followed to the leader;
/// then a batch is re-sent by hand under a sequence the leader already holds,
/// which is what a producer does after an acknowledgement went missing.
#[tokio::test]
#[serial]
async fn a_re_sent_batch_lands_once() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    let producer = cluster_client.idempotent_producer().await?;

    for i in 0..5u32 {
        producer
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("record-{i}").into_bytes(),
            )
            .await
            .with_context(|| format!("publish record-{i}"))?;
    }

    // The acknowledgement for sequence 2 "never arrived": send it again,
    // straight to the leader, under the same producer and sequence.
    let leader = cluster.node(&owner).context("leader node")?;
    let direct = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    direct
        .publisher()
        .await?
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"record-2".to_vec()],
            producer.producer_id(),
            2,
        )
        .await
        .context("the re-send must be acknowledged, not refused")?;

    let records = replay(&cluster, &owner, 5).await?;
    let expected: Vec<Vec<u8>> = (0..5).map(|i| format!("record-{i}").into_bytes()).collect();
    assert_eq!(
        records, expected,
        "the re-sent batch landed twice, or out of order"
    );
    Ok(())
}

/// **A non-leader names the leader.** Only the leader holds the sequences,
/// so a batch arriving anywhere else is refused with where to go rather than
/// forwarded — forwarded, two ingress brokers could land one batch twice.
#[tokio::test]
#[serial]
async fn a_non_leader_names_the_leader() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let elsewhere = cluster
        .nodes
        .iter()
        .find(|node| node.node_id != owner)
        .context("a broker that is not the leader")?;
    let leader = cluster.node(&owner).context("leader node")?;

    let direct = client::connect(
        elsewhere.client_addr,
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    let producer = direct.idempotent_producer().await?;
    let err = producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"misrouted".to_vec(),
        )
        .await
        .expect_err("a non-leader accepted an idempotent publish");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    match &refused.reason {
        PublishRefusalReason::NotLeader { node_id, addr } => {
            assert_eq!(node_id, &owner);
            assert_eq!(
                addr.as_deref(),
                Some(leader.client_addr.to_string().as_str())
            );
        }
        other => panic!("expected not_leader, got {other:?}"),
    }

    // Nothing landed anywhere, and the producer is not ended: the batch can
    // go to the leader under the same sequence.
    let via_leader = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    via_leader
        .publisher()
        .await?
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"routed".to_vec()],
            producer.producer_id(),
            0,
        )
        .await?;
    assert_eq!(replay(&cluster, &owner, 1).await?, vec![b"routed".to_vec()]);
    Ok(())
}

/// **A gap is refused, names what was expected, and appends nothing.** The
/// producer skipped a batch the leader never saw; continuing would leave a
/// hole the producer believes is filled.
#[tokio::test]
#[serial]
async fn a_gap_is_refused_with_the_expected_sequence() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let leader = cluster.node(&owner).context("leader node")?;
    let direct = client::connect(
        leader.client_addr,
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    let producer_id = direct.producer_init().await?;
    let publisher = direct.publisher().await?;

    publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"first".to_vec()],
            producer_id,
            0,
        )
        .await?;
    let err = publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"skipped ahead".to_vec()],
            producer_id,
            5,
        )
        .await
        .expect_err("a gap was appended");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    assert_eq!(
        refused.reason,
        PublishRefusalReason::SequenceGap { expected: 1 }
    );

    // And a producer the leader has never seen may not begin mid-way.
    let err = publisher
        .publish_idempotent_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            vec![b"unknown".to_vec()],
            producer_id + 1,
            3,
        )
        .await
        .expect_err("an unknown producer was accepted mid-sequence");
    let refused = err
        .downcast_ref::<PublishRefused>()
        .with_context(|| format!("not a typed refusal: {err:#}"))?;
    assert_eq!(refused.reason, PublishRefusalReason::UnknownProducer);

    assert_eq!(replay(&cluster, &owner, 1).await?, vec![b"first".to_vec()]);
    Ok(())
}

/// A cancelled publish stops the producer rather than silently losing records.
///
/// The sequence mechanism makes a re-send safe *because the number does not
/// move*. That holds only while the client knows whether the number was used.
/// Drop the publish future mid-flight and it does not: the batch may have
/// landed under that sequence, and the cursor still points at it.
///
/// Without the guard, the next batch goes out under the spent number, the
/// broker answers a remembered sequence from memory **without appending**, and
/// the caller is told `Ok` while its records are discarded. This asserts the
/// producer refuses instead — the loss is not recoverable, so the only honest
/// answer is to stop.
#[tokio::test]
#[serial]
async fn a_cancelled_publish_stops_the_producer_rather_than_reusing_its_sequence() -> Result<()> {
    let cluster = Cluster::start(config()).await?;
    let owner = cluster.owner(STREAM).await?;
    let cluster_client = client::connect_cluster(
        &cluster.broker_addrs(),
        &cluster.tenant_id,
        &cluster.client_token,
    )
    .await?;
    let producer = cluster_client.idempotent_producer().await?;

    // One that lands, so the producer is past its first sequence and the
    // cancellation below is not confused with a producer that never started.
    producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"landed".to_vec(),
        )
        .await?;

    // Cancelled as tightly as possible: the future is dropped while the batch
    // is in flight, which is exactly what a `timeout` around a publish does.
    let cancelled = tokio::time::timeout(
        Duration::from_nanos(1),
        producer.publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"in-doubt".to_vec(),
        ),
    )
    .await;
    assert!(cancelled.is_err(), "the publish was meant to be cancelled");

    // Different records under what may be a spent sequence. This is the call
    // that used to answer `Ok` and drop them.
    let after = producer
        .publish(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            b"would-be-lost".to_vec(),
        )
        .await;

    let err = after.expect_err("publishing after a cancelled batch must not report success");
    let message = format!("{err:#}");
    assert!(
        message.contains("cancelled before the broker answered"),
        "the refusal must say why the producer stopped: {message}",
    );

    // And the records that did land are intact: refusing is not the same as
    // damaging what came before.
    //
    // A tolerant drain rather than `replay`, which refuses an extra record.
    // Here an extra is the expected outcome and half the point: the cancelled
    // batch usually *does* land, which is precisely why reusing its sequence
    // would have discarded the next one. Whether it lands is a race with the
    // cancellation, so neither count is asserted -- only that nothing before it
    // was disturbed, and that the refused batch is nowhere on the stream.
    let (_client, mut subscription) = cluster.replay_on(&owner, STREAM).await?;
    let mut records: Vec<Vec<u8>> = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_millis(500), subscription.next_event()).await
    {
        records.push(event.payload.to_vec());
    }
    assert!(
        records.iter().any(|record| record == b"landed"),
        "the batch that completed before the cancellation is missing: {records:?}",
    );
    assert!(
        !records.iter().any(|record| record == b"would-be-lost"),
        "the refused batch reached the stream, so the refusal was not the reason \
         it did not: {records:?}",
    );

    cluster.shutdown().await;
    Ok(())
}
