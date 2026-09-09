//! Start a local Felix cluster and inspect it.
//!
//! ```text
//! cargo run -p felix-cluster -- up       # start, print addresses, hold until Ctrl-C
//! cargo run -p felix-cluster -- status   # start, print membership and ownership, exit
//! cargo run -p felix-cluster -- smoke    # publish through a non-owner, receive from the owner
//! ```
//!
//! `--nodes N` sets the cluster size (default 3).
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_cluster::{Cluster, ClusterConfig};

const STREAM: &str = "orders";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("up");
    let nodes = parse_nodes(&args)?;

    let config = ClusterConfig {
        nodes,
        streams: vec![(STREAM.to_string(), 1)],
        // A cluster a person started should say what it is doing.
        inherit_output: command == "up" || std::env::var("FELIX_CLUSTER_VERBOSE").is_ok(),
        ..Default::default()
    };

    eprintln!("starting a {nodes}-node cluster...");
    let mut cluster = Cluster::start(config).await?;
    eprintln!("cluster up.\n");
    print_topology(&cluster).await?;

    match command {
        "up" => {
            eprintln!("\nholding the cluster. press Ctrl-C to tear it down.");
            tokio::signal::ctrl_c().await.context("wait for Ctrl-C")?;
            eprintln!("\ntearing down...");
        }
        "status" => {}
        "smoke" => smoke(&mut cluster).await?,
        other => bail!("unknown command {other}; expected up, status, or smoke"),
    }

    cluster.shutdown().await;
    Ok(())
}

fn parse_nodes(args: &[String]) -> Result<usize> {
    match args.iter().position(|arg| arg == "--nodes") {
        Some(index) => args
            .get(index + 1)
            .ok_or_else(|| anyhow::anyhow!("--nodes needs a value"))?
            .parse()
            .context("parse --nodes"),
        None => Ok(3),
    }
}

async fn print_topology(cluster: &Cluster) -> Result<()> {
    println!("control plane   {}", cluster.control_plane_url());
    println!();
    println!("{:<10} {:<22} {:<22} metrics", "node", "client", "internal");
    for node in &cluster.nodes {
        println!(
            "{:<10} {:<22} {:<22} {}",
            node.node_id,
            node.client_addr.to_string(),
            node.internal_addr.to_string(),
            node.metrics_addr,
        );
    }
    println!();
    println!("placeable: {}", cluster.placeable_nodes().await?.join(", "));
    println!("shard ownership:");
    let mut owners: Vec<_> = cluster.shard_owners().await?.into_iter().collect();
    owners.sort();
    for (shard, leader) in owners {
        println!("  {shard} -> {leader}");
    }
    Ok(())
}

/// The scenario the harness exists to make easy: a publish that arrives at a
/// broker which does not own the shard still reaches a subscriber on the one
/// that does.
async fn smoke(cluster: &mut Cluster) -> Result<()> {
    let (owner, non_owner) = cluster.owner_and_non_owner(STREAM).await?;
    println!("\nsmoke: publishing via {non_owner}, subscribing on {owner}");

    let (_client, mut subscription) = cluster.subscribe_on(&owner, STREAM).await?;
    let payload = b"cross-broker".to_vec();
    cluster
        .publish_via(&non_owner, STREAM, payload.clone())
        .await?;

    // Assert the publish actually crossed a node boundary before waiting on
    // delivery. Without this a local write on the wrong broker looks identical
    // to a delivery that is merely slow.
    let forwarded = cluster
        .metric(&non_owner, "felix_broker_forwards_total")
        .await?;
    match forwarded {
        Some(count) if count > 0.0 => println!("smoke: {non_owner} forwarded {count} publish(es)"),
        _ => bail!(
            "{non_owner} did not forward anything: the publish was served locally by a broker \
             that does not own the shard"
        ),
    }

    let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
        .await
        .context("timed out waiting for the forwarded record")?
        .context("subscription failed")?
        .context("subscription closed before the record arrived")?;

    if event.payload != payload {
        bail!(
            "expected {:?}, received {:?}",
            String::from_utf8_lossy(&payload),
            String::from_utf8_lossy(&event.payload),
        );
    }
    println!("smoke: ok — the record crossed the node boundary");
    Ok(())
}
