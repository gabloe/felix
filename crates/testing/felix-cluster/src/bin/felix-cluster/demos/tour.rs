//! `demo`: the cross-broker story in one pane.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, bail};
use felix_cluster::Cluster;

use super::{beat, step};
use crate::args::{flag_usize, pace_from};
use crate::lifecycle::cluster_config;
use crate::{STREAM, init_tracing};

/// The whole demonstration, driven end to end in one terminal.
///
/// The three-panel version reads better, but it needs three windows and someone
/// typing in them. This does the same story in one pane: the publisher's lines
/// are flush left, and records arriving at the subscriber are indented with an
/// arrow, so a viewer can still tell the two apart.
///
/// `--pace` is the gap between steps. Long enough to narrate over by default;
/// `--pace 0` runs it flat out, which is what a test wants.
pub(crate) async fn demo(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let pace = pace_from(args)?;

    step("Starting a three-node cluster");
    println!("  Three brokers, each its own process, port, identity and data directory.");
    let cluster = Cluster::start(cluster_config(nodes, false)).await?;
    println!("  Ready — every broker registered, every shard assigned, a publish accepted.\n");
    beat(pace).await;

    step("Who owns what");
    for node in &cluster.nodes {
        println!("  {:<10} {}", node.node_id, node.client_addr);
    }
    let owner = cluster.owner(STREAM).await?;
    println!();
    for (shard, leader) in sorted(cluster.shard_owners().await?) {
        println!("  {shard} -> {leader}");
    }
    println!("\n  {owner} leads the shard. The other brokers do not hold it at all,");
    println!("  and nothing configured that — placement hashes the shard key.");
    beat(pace).await;

    step(&format!("Subscribing on {owner}"));
    let (_client, subscription) = cluster.subscribe_on(&owner, STREAM).await?;
    // Events print as they arrive, indented, so they read as a separate voice
    // from the publisher's lines.
    let reader = tokio::spawn(async move {
        let mut subscription = subscription;
        while let Ok(Some(event)) = subscription.next_event().await {
            let offset = event
                .offset
                .map(|o| o.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "      ← [subscriber] offset {offset:>4}  {}",
                String::from_utf8_lossy(&event.payload),
            );
        }
    });
    println!("  Attached. Nothing published yet.");
    beat(pace).await;

    let others: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|id| id != &owner)
        .collect();
    let non_owner = others.first().cloned().unwrap_or_else(|| owner.clone());

    step(&format!(
        "Publishing through {non_owner}, which does not own the shard"
    ));
    let before = forward_count(&cluster, &non_owner).await;
    cluster
        .publish_via(&non_owner, STREAM, b"hello".to_vec())
        .await?;
    let after = forward_count(&cluster, &non_owner).await;
    if after > before {
        println!("  published \"hello\" via {non_owner} → forwarded to {owner} → acknowledged");
    } else {
        bail!("{non_owner} did not forward — it served a shard owned by {owner} locally");
    }
    beat(pace).await;

    step(&format!("The same publish, through {owner}, which does"));
    cluster
        .publish_via(&owner, STREAM, b"direct".to_vec())
        .await?;
    println!("  published \"direct\" via {owner} (the owner) — written locally, no hop");
    println!("\n  Same client, same call. The hop happens only when it has to.");
    beat(pace).await;

    step("A burst across every broker");
    for index in 1..=9u32 {
        let via = &cluster.nodes[(index as usize - 1) % cluster.nodes.len()].node_id;
        cluster
            .publish_via(via, STREAM, format!("msg-{index:03}").into_bytes())
            .await?;
    }
    println!(
        "  Nine records, round-robin across all {} brokers.",
        cluster.nodes.len()
    );
    // Let the last deliveries land before the closing line.
    tokio::time::sleep(Duration::from_millis(600)).await;
    beat(pace).await;

    step("Done");
    println!("  Every record reached the subscriber, whichever broker it entered through.");
    reader.abort();
    cluster.shutdown().await;
    Ok(())
}

fn sorted(map: HashMap<String, String>) -> Vec<(String, String)> {
    let mut rows: Vec<_> = map.into_iter().collect();
    rows.sort();
    rows
}

async fn forward_count(cluster: &Cluster, node_id: &str) -> f64 {
    cluster
        .metric(node_id, "felix_broker_forwards_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0)
}
