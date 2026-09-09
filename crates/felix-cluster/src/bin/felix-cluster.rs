//! Start a local Felix cluster, and talk to it from another terminal.
//!
//! ```text
//! # window 1
//! task cluster:up
//!
//! # window 2
//! task cluster:subscribe
//!
//! # window 3
//! task cluster:publish -- hello
//! ```
//!
//! `up` writes the addresses and a credential to a session file so the other
//! commands need nothing copied into them.
//!
//! Every line names the broker it went through and whether the record crossed a
//! node boundary, because that is the only part of this a cluster does
//! differently from a single broker.
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_cluster::session::{self, Session};
use felix_cluster::{Cluster, ClusterConfig};

const STREAM: &str = "orders";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("up");

    match command {
        "up" => up(&args).await,
        "status" => status(&args).await,
        "smoke" => smoke_command(&args).await,
        "subscribe" => subscribe(&args).await,
        "publish" => publish(&args).await,
        "owners" => owners().await,
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        other => {
            print_help();
            bail!("unknown command {other}")
        }
    }
}

fn print_help() {
    eprintln!(
        "\
felix-cluster — a local multi-node Felix cluster

  up [--nodes N]            start a cluster and hold it until Ctrl-C
  status [--nodes N]        start, print membership and ownership, exit
  smoke [--nodes N]         publish through a non-owner, receive from the owner
  owners                          who leads each shard of a running cluster
  subscribe STREAM [--on NODE]    stream events from a running cluster
  publish STREAM MSG [--via NODE] publish to a running cluster

`up` first; the rest attach to it."
    );
}

// ---------------------------------------------------------------- lifecycle

async fn up(args: &[String]) -> Result<()> {
    init_tracing(true);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    eprintln!("starting a {nodes}-node cluster...");
    let cluster = Cluster::start(cluster_config(nodes, true)).await?;

    let path = session::default_path();
    if let Some(existing) = session::live_session(&path).await {
        eprintln!(
            "\nwarning: a cluster is already running at {}.\n\
             \x20        it keeps its ports and processes, but `subscribe` and `publish`\n\
             \x20        will now talk to this one instead. stop it with Ctrl-C in its\n\
             \x20        own window.",
            existing.control_plane,
        );
    }
    let session = cluster.session();
    session.write(&path)?;

    eprintln!("\ncluster up.\n");
    print_topology(&cluster).await?;
    println!("\nsession   {}", path.display());
    println!("\n  felix-cluster subscribe        # in another window");
    println!("  felix-cluster publish hello    # in a third");
    eprintln!("\nholding the cluster. press Ctrl-C to tear it down.");

    stop_signal().await?;
    eprintln!("\ntearing down...");
    // Only if it still describes this cluster: a second cluster may have taken
    // the file over, and deleting that one leaves it running and unreachable.
    session.remove_if_ours(&path);
    cluster.shutdown().await;
    Ok(())
}

async fn status(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let cluster = Cluster::start(cluster_config(nodes, false)).await?;
    print_topology(&cluster).await?;
    cluster.shutdown().await;
    Ok(())
}

async fn smoke_command(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let mut cluster = Cluster::start(cluster_config(nodes, false)).await?;
    print_topology(&cluster).await?;
    let result = smoke(&mut cluster).await;
    cluster.shutdown().await;
    result
}

// ------------------------------------------------------------- attaching

/// Who leads each shard, read from a running cluster.
async fn owners() -> Result<()> {
    let session = Session::read(&session::default_path())?;
    let owners = shard_owners(&session).await?;
    let mut rows: Vec<_> = owners.into_iter().collect();
    rows.sort();
    for (shard, leader) in rows {
        println!("{shard} -> {leader}");
    }
    Ok(())
}

/// Stream events until interrupted.
async fn subscribe(args: &[String]) -> Result<()> {
    init_tracing(false);
    let session = Session::read(&session::default_path())?;
    let stream = match positionals(args).first() {
        Some(stream) => stream.clone(),
        None => bail!("subscribe takes a stream: felix-cluster subscribe {STREAM}"),
    };

    // Defaults to the owner, because that is the only broker that serves a
    // subscription today -- a non-owner has no copy to read from, and routing a
    // subscribe is M6 (see docs/subscribe-routing.md).
    let owner = owner_of(&session, &stream).await?;
    let node_id = flag(args, "--on")?.unwrap_or_else(|| owner.clone());
    let node = session
        .node(&node_id)
        .with_context(|| format!("unknown node {node_id}"))?;

    let role = if node_id == owner {
        "owner"
    } else {
        "NOT the owner"
    };
    println!("subscribing to {stream} on {node_id} ({role})");
    if node_id != owner {
        println!(
            "  note: {node_id} does not own this shard, so it has nothing to deliver.\n\
             \x20       subscribe routing is M6; see docs/subscribe-routing.md."
        );
    }
    println!("waiting for events. Ctrl-C to stop.\n");

    let client =
        felix_cluster::client::connect(node.client_addr, &session.tenant_id, &session.client_token)
            .await?;
    let mut subscription = client
        .subscribe(&session.tenant_id, &session.namespace, &stream)
        .await
        .context("subscribe")?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nstopped.");
                return Ok(());
            }
            event = subscription.next_event() => {
                match event.context("subscription failed")? {
                    Some(event) => {
                        let offset = event
                            .offset
                            .map(|o| o.to_string())
                            .unwrap_or_else(|| "-".to_string());
                        println!(
                            "[{node_id}] offset {offset:>6}  {}",
                            String::from_utf8_lossy(&event.payload),
                        );
                    }
                    None => {
                        println!("\nsubscription closed by the broker.");
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Publish one message, and say whether it crossed a node boundary.
async fn publish(args: &[String]) -> Result<()> {
    init_tracing(false);
    let session = Session::read(&session::default_path())?;
    let words = positionals(args);
    let (stream, message) = match words.split_first() {
        // The rest is the message, so it can contain spaces without quoting --
        // `publish orders order placed` reads better on camera than escaping.
        Some((stream, rest)) if !rest.is_empty() => (stream.clone(), rest.join(" ")),
        _ => bail!(
            "publish takes a stream and a message: \
             felix-cluster publish {STREAM} \"order placed\""
        ),
    };

    let owner = owner_of(&session, &stream).await?;
    // Defaults to a broker that does *not* own the shard, because that is the
    // interesting path: it is the one a single-node broker cannot show.
    let node_id = match flag(args, "--via")? {
        Some(explicit) => explicit,
        None => session
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .find(|id| id != &owner)
            .unwrap_or_else(|| owner.clone()),
    };
    let node = session
        .node(&node_id)
        .with_context(|| format!("unknown node {node_id}"))?;

    let before = forwards(&session, &node_id).await;
    let client =
        felix_cluster::client::connect(node.client_addr, &session.tenant_id, &session.client_token)
            .await?;
    let publisher = client.publisher().await.context("open publisher")?;
    publisher
        .publish(
            &session.tenant_id,
            &session.namespace,
            &stream,
            message.clone().into_bytes(),
            felix_wire::AckMode::PerMessage,
        )
        .await
        .with_context(|| format!("publish via {node_id}"))?;
    let after = forwards(&session, &node_id).await;

    // The acknowledgement already proves it was written. What a cluster adds is
    // *where*, and the forward counter is how that is visible from outside.
    // The stream and the payload are named on both sides on purpose: a viewer
    // watching two terminals has nothing else linking what was published to what
    // arrived.
    if node_id == owner {
        println!(
            "published {message:?} to {stream} via {node_id} (the owner) — written locally, no hop"
        );
    } else if after > before {
        println!(
            "published {message:?} to {stream} via {node_id} → forwarded to {owner} → acknowledged"
        );
    } else {
        println!(
            "published {message:?} to {stream} via {node_id}, but it did not forward — \
             it served a shard owned by {owner} locally"
        );
    }
    Ok(())
}

/// Wait for Ctrl-C, or for a `kill`.
///
/// Both, because the brokers are child processes: this process dying without
/// running its teardown orphans three of them, each holding a port and a data
/// directory. SIGINT alone leaves `kill` and most process supervisors doing
/// exactly that.
#[cfg(unix)]
async fn stop_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C")?,
        _ = term.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn stop_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
}

// ---------------------------------------------------------------- helpers

fn cluster_config(nodes: usize, inherit_output: bool) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![(STREAM.to_string(), 1)],
        inherit_output: inherit_output && std::env::var("FELIX_CLUSTER_VERBOSE").is_ok(),
        ..Default::default()
    }
}

fn init_tracing(verbose: bool) {
    let default = if verbose { "info" } else { "warn" };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default)),
        )
        .try_init();
}

fn flag(args: &[String], name: &str) -> Result<Option<String>> {
    match args.iter().position(|arg| arg == name) {
        Some(index) => Ok(Some(
            args.get(index + 1)
                .cloned()
                .with_context(|| format!("{name} needs a value"))?,
        )),
        None => Ok(None),
    }
}

fn flag_usize(args: &[String], name: &str) -> Result<Option<usize>> {
    match flag(args, name)? {
        Some(value) => Ok(Some(
            value.parse().with_context(|| format!("parse {name}"))?,
        )),
        None => Ok(None),
    }
}

/// Everything that is neither the subcommand nor a flag or its value, in order.
fn positionals(args: &[String]) -> Vec<String> {
    let mut found = Vec::new();
    let mut skip_next = false;
    for arg in args.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg.starts_with("--") {
            skip_next = true;
            continue;
        }
        found.push(arg.clone());
    }
    found
}

async fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .no_proxy()
        .build()
        .expect("build HTTP client")
}

async fn shard_owners(session: &Session) -> Result<std::collections::HashMap<String, String>> {
    #[derive(serde::Deserialize)]
    struct Response {
        items: Vec<Row>,
    }
    #[derive(serde::Deserialize)]
    struct Row {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        leader: String,
    }

    let url = format!("{}/v1/shard-assignments", session.control_plane);
    let response = http()
        .await
        .get(&url)
        .bearer_auth(&session.admin_token)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("GET {url}: {status}");
    }
    let response: Response = response.json().await.context("decode assignments")?;
    Ok(response
        .items
        .into_iter()
        .map(|row| {
            (
                format!(
                    "{}/{}/{}/{}",
                    row.tenant_id, row.namespace, row.stream, row.shard
                ),
                row.leader,
            )
        })
        .collect())
}

async fn owner_of(session: &Session, stream: &str) -> Result<String> {
    let key = format!("{}/{}/{}/0", session.tenant_id, session.namespace, stream);
    shard_owners(session)
        .await?
        .remove(&key)
        .with_context(|| format!("no owner for {key}"))
}

/// The broker's forward counter, or 0 if it has never forwarded.
async fn forwards(session: &Session, node_id: &str) -> f64 {
    let Some(node) = session.node(node_id) else {
        return 0.0;
    };
    let url = format!("http://{}/metrics", node.metrics_addr);
    let Ok(response) = http().await.get(&url).send().await else {
        return 0.0;
    };
    let Ok(body) = response.text().await else {
        return 0.0;
    };
    body.lines()
        .filter(|line| line.starts_with("felix_broker_forwards_total"))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn print_topology(cluster: &Cluster) -> Result<()> {
    println!("control plane   {}", cluster.control_plane_url());
    println!();
    println!("{:<10} {:<22} metrics", "node", "client");
    for node in &cluster.nodes {
        println!(
            "{:<10} {:<22} {}",
            node.node_id,
            node.client_addr.to_string(),
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
