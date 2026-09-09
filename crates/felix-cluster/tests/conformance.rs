//! Cross-broker conformance: the same assertions on one node and on three.
//!
//! Every scenario runs against both deployments. A cluster is not allowed to
//! have different semantics from a single broker — that equivalence is the
//! claim, and running one set of assertions against both is the only way to
//! keep making it as the cluster path grows.
//!
//! Everything here goes through the client-facing API. A test that reached into
//! broker internals could not tell the difference between a publish that was
//! routed correctly and one that was handled by the wrong broker, which is the
//! failure this suite exists to catch.
use anyhow::Result;
use felix_cluster::scenarios::{self, Ingress, Outcome};
use felix_cluster::{Cluster, ClusterConfig};
use serial_test::serial;

/// A scenario's future, boxed so `on_both` can name it for any borrow of the
/// cluster it is handed.
type ScenarioFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Outcome>> + Send + 'a>>;

const STREAM: &str = "orders";
/// A second stream, so placement has more than one thing to distribute and the
/// suite is not describing a cluster where every shard happens to land together.
const OTHER: &str = "events";

fn config(nodes: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![(STREAM.to_string(), 1), (OTHER.to_string(), 1)],
        ..Default::default()
    }
}

/// Run one scenario against both deployments.
///
/// The single-node run is what makes this a conformance suite rather than a
/// cluster test: a scenario that only passes with three brokers, or only with
/// one, is a semantic difference and fails here.
async fn on_both<F>(name: &str, scenario: F) -> Result<()>
where
    F: for<'a> Fn(&'a Cluster) -> ScenarioFuture<'a>,
{
    for nodes in [1usize, 3] {
        let cluster = Cluster::start(config(nodes)).await?;
        let outcome = scenario(&cluster).await;
        // Torn down before the result is unwrapped, so a failing scenario does
        // not also leave broker processes behind.
        cluster.shutdown().await;
        let outcome = outcome.map_err(|err| {
            anyhow::anyhow!("{name} failed on a {nodes}-node deployment: {err:#}")
        })?;

        if let Outcome::Skipped(why) = outcome {
            // Printed rather than silent: a scenario that skips everywhere is
            // covering nothing, and that should be visible in the log.
            println!("{name}: skipped on {nodes} node(s) — {why}");
        }
    }
    Ok(())
}

#[serial]
#[tokio::test]
async fn delivery_through_the_owner() -> Result<()> {
    on_both("delivery via owner", |cluster| {
        Box::pin(scenarios::delivery(cluster, STREAM, Ingress::Owner))
    })
    .await
}

#[serial]
#[tokio::test]
async fn delivery_through_a_non_owner() -> Result<()> {
    on_both("delivery via non-owner", |cluster| {
        Box::pin(scenarios::delivery(cluster, STREAM, Ingress::NonOwner))
    })
    .await
}

#[serial]
#[tokio::test]
async fn ordering_and_no_duplicates_through_the_owner() -> Result<()> {
    on_both("ordering via owner", |cluster| {
        Box::pin(scenarios::ordering_and_integrity(
            cluster,
            STREAM,
            Ingress::Owner,
            25,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn ordering_and_no_duplicates_through_a_non_owner() -> Result<()> {
    on_both("ordering via non-owner", |cluster| {
        Box::pin(scenarios::ordering_and_integrity(
            cluster,
            STREAM,
            Ingress::NonOwner,
            25,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn payloads_survive_the_crossing() -> Result<()> {
    on_both("payload integrity via non-owner", |cluster| {
        Box::pin(scenarios::payload_integrity(
            cluster,
            STREAM,
            Ingress::NonOwner,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn an_unknown_stream_is_refused_everywhere() -> Result<()> {
    on_both("unknown stream via non-owner", |cluster| {
        Box::pin(scenarios::unknown_stream_is_refused(
            cluster,
            Ingress::NonOwner,
        ))
    })
    .await?;
    on_both("unknown stream via owner", |cluster| {
        Box::pin(scenarios::unknown_stream_is_refused(
            cluster,
            Ingress::Owner,
        ))
    })
    .await
}

/// Routing must not launder authorization. A forwarded publish crosses a trust
/// boundary the client never sees.
#[serial]
#[tokio::test]
async fn a_forwarded_publish_is_still_authorized() -> Result<()> {
    on_both("unauthorized publish via non-owner", |cluster| {
        Box::pin(scenarios::unauthorized_publish_is_refused(
            cluster,
            STREAM,
            Ingress::NonOwner,
        ))
    })
    .await
}

/// A shard that moves converges: the old owner stops serving it locally and
/// starts forwarding to the new one, within a bounded time.
///
/// # The stale-ownership window is real
///
/// Ownership reaches a broker through its watch, so between the control plane
/// moving a shard and the old owner noticing, that broker still believes it owns
/// the shard and **serves publishes locally**. Those records land in its log and
/// are invisible to subscribers on the new owner.
///
/// Nothing in M4 closes that window — there is no fencing, and the generation
/// check only protects a *forwarded* publish, which is not what this is. What is
/// promised is convergence, and convergence is what this asserts. The window
/// itself is a gap, recorded in `docs/cluster-harness.md` rather than hidden
/// behind a test that waits long enough not to see it.
#[serial]
#[tokio::test]
async fn a_moved_shard_converges_on_the_new_owner() -> Result<()> {
    let cluster = Cluster::start(config(3)).await?;

    let key = format!("{}/{}/{}/0", cluster.tenant_id, cluster.namespace, STREAM);
    let before = cluster.shard_assignments().await?;
    let before = before.get(&key).expect("assigned").clone();

    let new_owner = cluster.move_shard(STREAM).await?;
    assert_ne!(new_owner, before.leader, "the shard did not move");

    let after = cluster.shard_assignments().await?;
    let after = after.get(&key).expect("assigned").clone();
    assert!(
        after.generation > before.generation,
        "a move must advance the generation: {} -> {}",
        before.generation,
        after.generation,
    );

    // Publish through the broker that used to own it until it forwards rather
    // than serving locally. That transition is convergence; the loop bounds how
    // long it may take.
    let old_owner = before.leader.clone();
    let mut converged = false;
    for _ in 0..200 {
        let forwards_before = cluster
            .metric(&old_owner, "felix_broker_forwards_total")
            .await?
            .unwrap_or(0.0);
        let published = cluster
            .publish_via(&old_owner, STREAM, b"probe".to_vec())
            .await
            .is_ok();
        let forwards_after = cluster
            .metric(&old_owner, "felix_broker_forwards_total")
            .await?
            .unwrap_or(0.0);
        if published && forwards_after > forwards_before {
            converged = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        converged,
        "{old_owner} never started forwarding {key} after it moved to {new_owner} \
         (generation {} -> {})",
        before.generation, after.generation,
    );

    // Converged, so a record published through the old owner must now reach a
    // subscriber on the new one.
    let (_client, mut subscription) = cluster.subscribe_on(&new_owner, STREAM).await?;
    let payload = b"after-the-move".to_vec();
    cluster
        .publish_via(&old_owner, STREAM, payload.clone())
        .await?;

    let event = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        subscription.next_event(),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("the record never reached the new owner {new_owner} (shard={key})")
    })?
    .expect("subscription failed")
    .expect("subscription closed");
    assert_eq!(event.payload, payload);

    cluster.shutdown().await;
    Ok(())
}
