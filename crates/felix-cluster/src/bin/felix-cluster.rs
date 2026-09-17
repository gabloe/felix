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

use anyhow::{Context, Result, anyhow, bail};
use felix_cluster::session::{self, Session};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec, wait};

const STREAM: &str = "orders";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("up");

    match command {
        "up" => up(&args).await,
        "status" => status(&args).await,
        "smoke" => smoke_command(&args).await,
        "demo" => demo(&args).await,
        "failover" => failover(&args).await,
        "consistency" => consistency(&args).await,
        "subscribe" => subscribe(&args).await,
        "publish" => publish(&args).await,
        "owners" => owners().await,
        "client-fixture" => client_fixture(&args[1..]).await,
        "nodes" => nodes(),
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

  up [--nodes N]                  start a cluster and hold it until Ctrl-C
  status [--nodes N]              start, print membership and ownership, exit
  smoke [--nodes N]               publish through a non-owner, receive from the owner
  demo [--pace SECONDS]           the whole cross-broker story, start to finish
  failover [--pace SECONDS]       replicate, kill the leader, keep publishing
  consistency [--pace SECONDS]    what Quorum buys, and what Leader costs, under one fault
  client-fixture [--out PATH]     a cluster for a client conformance suite to run against
  nodes                           every broker in a running cluster, one per line
  owners                          who leads each shard of a running cluster
  subscribe STREAM [--on NODE]    stream events from a running cluster
  publish STREAM MSG [--via NODE] publish to a running cluster

`up` first; the rest attach to it."
    );
}

/// What `Quorum` buys and what `Leader` costs, under the same fault.
///
/// The failover demo shows a quorum-acknowledged record surviving. This is its
/// counterpart, and the harder half to look at: the same fault applied to a
/// `Leader` stream loses an acknowledged record.
///
/// The fault is a leader cut off from its replicas — frozen followers, so the
/// leader is healthy and alone. Each stream's own followers are frozen in turn,
/// so neither run depends on the two streams happening to share a leader, and
/// the demo cannot flake on where placement put them.
async fn consistency(args: &[String]) -> Result<()> {
    init_tracing(false);
    let pace = pace_from(args)?;
    const QUORUM_STREAM: &str = "orders-quorum";
    const LEADER_STREAM: &str = "orders-leader";

    step("Two streams, identical but for one field");
    println!("  Three brokers. Both streams are replicated three ways, so every");
    println!("  broker holds every record. The only difference is `consistency`:");
    println!("    {QUORUM_STREAM:<16} Quorum — a majority must hold it before it is acknowledged");
    println!("    {LEADER_STREAM:<16} Leader — the leader alone decides, which is the default");
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![
            StreamSpec::quorum(QUORUM_STREAM, 1, 3),
            StreamSpec::replicated(LEADER_STREAM, 1, 3),
        ],
        ..Default::default()
    })
    .await?;
    beat(pace).await;

    // ---- the same fault, applied to each stream in turn -------------------
    // Each phase returns whether the record published during the isolation was
    // acknowledged, and whether the shard ended up serving without it.
    let (quorum_acked, _) =
        run_consistency_phase(&mut cluster, QUORUM_STREAM, "Quorum", pace, false).await?;
    let (_, leader_served_without_it) =
        run_consistency_phase(&mut cluster, LEADER_STREAM, "Leader", pace, true).await?;
    let quorum_refused = !quorum_acked;

    step("The trade, stated plainly");
    println!("  The same fault — a leader cut off from its replicas — asked of both:");
    println!();
    println!("    Quorum   refused the write, while the shard stayed available");
    println!("             throughout. The publisher was told it could not be");
    println!("             vouched for, and could decide what to do about that.");
    println!("    Leader   took the write, and the shard went unavailable when the");
    println!("             leader died. Nothing was served that contradicted the");
    println!("             acknowledgement, but nothing was served at all.");
    println!();
    println!("  Neither of those is data loss, and that is the point worth taking away.");
    println!(
        "  `Leader` does not trade safety for latency — it trades \x1b[1mavailability\x1b[0m for"
    );
    println!("  latency, and it moves the moment you find out. Quorum tells you at");
    println!("  publish time, when you still hold the record and can retry. Leader");
    println!("  tells you at failover time, when the only copy is on a dead broker's");
    println!("  disk and you are waiting for it to come back.");
    println!();
    println!("  `Leader` is the default because one round trip beats two and most");
    println!("  streams would rather have the latency than the guarantee. Set");
    println!("  `consistency: Quorum` on the ones that would rather be refused.");

    if !quorum_refused {
        bail!("a Quorum publish was acknowledged with no reachable majority");
    }
    if leader_served_without_it {
        bail!(
            "a shard was served without a record its leader had acknowledged — that is \
             silent data loss, not the unavailability this demo expects"
        );
    }
    Ok(())
}

/// Publish under one consistency level with the leader cut off from its
/// replicas, then kill the leader and read the stream back from the promotion.
///
/// Returns `(acknowledged, served_without_it)`: whether the record published
/// during the isolation was acknowledged, and whether the shard was afterwards
/// served by a broker that does not have it. The second is the one that would
/// be silent data loss.
async fn run_consistency_phase(
    cluster: &mut Cluster,
    stream: &str,
    level: &str,
    pace: Duration,
    kill_the_leader: bool,
) -> Result<(bool, bool)> {
    step(&format!("{level}: two records the normal way"));
    let leader = cluster.owner(stream).await?;
    println!("  {stream} is led by {leader}.");
    for index in 1..=2u32 {
        cluster
            .publish_via_any(stream, format!("{level}-before-{index}").into_bytes())
            .await?;
        println!("  acknowledged: {level}-before-{index}");
    }
    wait::until(Duration::from_secs(20), "replication to settle", || async {
        matches!(
            cluster
                .metric(&leader, "felix_broker_replication_lag_records")
                .await,
            Ok(Some(lag)) if lag == 0.0
        )
    })
    .await?;
    println!("  Replication lag on {leader}: 0 — both replicas hold both records.");
    beat(pace).await;

    step(&format!("{level}: cutting {leader} off from its replicas"));
    let followers: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|id| id != &leader)
        .collect();
    for follower in &followers {
        cluster.pause_node(follower)?;
    }
    println!(
        "  {} frozen. Not killed — {leader} still believes they are there,",
        followers.join(" and ")
    );
    println!("  and keeps trying to ship to them. It is healthy and alone.");
    beat(pace).await;

    step(&format!("{level}: publishing while alone"));
    let attempted = format!("{level}-while-alone");
    let outcome = cluster
        .publish_via_any(stream, attempted.clone().into_bytes())
        .await;
    let acknowledged = outcome.is_ok();
    match &outcome {
        Ok(()) => {
            println!("  acknowledged: {attempted}");
            println!("  The leader had it durably, and that was enough to answer the client.");
        }
        Err(err) => {
            println!("  refused: {attempted}");
            // `{:#}` so the reason the broker gave is shown, and not just the
            // harness's context line wrapped around it.
            println!("  \"{err:#}\"");
            println!("  No majority could be reached, so the write was not acknowledged.");
            println!("  A publisher that sees this knows to retry. That is the point.");
        }
    }
    beat(pace).await;

    if !kill_the_leader {
        for follower in &followers {
            cluster.resume_node(follower)?;
        }
        println!("\n  Replicas resumed.");
        step(&format!("{level}: reading the stream back"));
        let survived = replay_contains(cluster, &leader, stream, &attempted).await?;
        report_replay(acknowledged, survived, &attempted);
        beat(pace).await;
        // The leader is still up, so nothing was served in its absence.
        return Ok((acknowledged, false));
    }

    // The leader dies while its replicas are still frozen, so they never
    // receive what it acknowledged alone. Resuming first would let replication
    // catch up and hand the record over, which is the opposite of the point.
    step(&format!(
        "{level}: killing {leader} before its replicas can catch up"
    ));
    cluster.kill_node(&leader)?;
    println!("  Gone, with the replicas still frozen. Whatever it acknowledged");
    println!("  alone went with it.");
    for follower in &followers {
        cluster.resume_node(follower)?;
    }
    println!("  Replicas resumed. They are what is left of the shard.");
    beat(pace).await;

    step(&format!("{level}: nobody is promoted"));
    // Long enough to be a conclusion rather than a race: the failover demo sees
    // a promotion inside a second when one is possible at all.
    let settle = Duration::from_secs(15);
    let deadline = std::time::Instant::now() + settle;
    let mut promoted = None;
    while std::time::Instant::now() < deadline {
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(stream).await
            && owner != leader
        {
            promoted = Some(owner);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    if let Some(promoted) = promoted {
        println!("  {promoted} was promoted after all.");
        let survived = replay_contains(cluster, &promoted, stream, &attempted).await?;
        report_replay(acknowledged, survived, &attempted);
        return Ok((acknowledged, acknowledged && !survived));
    }

    println!("  Still no owner after {settle:?}, and the control plane says why:");
    println!();
    println!("      the leader is gone and no replica holding this shard's log can take over");
    println!();
    println!("  Both replicas are alive and reachable. Neither is allowed to serve,");
    println!("  because {leader} acknowledged a record neither of them has. Promoting");
    println!("  one would open the shard \x1b[1mwithout\x1b[0m `{attempted}` — and a client that");
    println!("  read it would be told, in effect, that the record never existed.");
    println!();
    println!(
        "  \x1b[1mSo the shard is unavailable instead.\x1b[0m That is the cost of `Leader`, and"
    );
    println!("  it is the deliberate choice: a silently empty shard is worse than an");
    println!("  absent one, because nothing downstream can detect it. The shard comes");
    println!("  back when {leader} does, with its disk — that is where the record is.");
    beat(pace).await;
    // Unavailable, not lost: nothing was served that contradicts the ack.
    Ok((acknowledged, false))
}

/// Replay a stream from one broker and say whether `needle` is in it, printing
/// what was actually read so the answer is visible rather than asserted.
async fn replay_contains(
    cluster: &Cluster,
    node_id: &str,
    stream: &str,
    needle: &str,
) -> Result<bool> {
    let (_client, mut subscription) = cluster.replay_on(node_id, stream).await?;
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).to_string();
        // The harness publishes a probe per broker at startup to prove the
        // cluster can serve before anything is measured; it is not this demo's.
        if payload == "harness-probe" {
            continue;
        }
        let offset = event
            .offset
            .map(|o| o.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("      ← offset {offset:>4}  {payload}");
        seen.push(payload);
    }
    Ok(seen.iter().any(|payload| payload == needle))
}

/// The four combinations of "was it acknowledged" and "is it still there", so
/// the one that matters is named rather than left to be noticed.
fn report_replay(acknowledged: bool, survived: bool, record: &str) {
    println!();
    match (acknowledged, survived) {
        (true, true) => println!("  `{record}` was acknowledged and is still here."),
        (true, false) => {
            println!("  \x1b[1m`{record}` was acknowledged and is gone.\x1b[0m");
            println!("  Nothing lied about it afterwards — the cluster simply does not");
            println!("  have a record it told a client it had.");
        }
        (false, false) => {
            println!("  \x1b[1m`{record}` was refused and is not here.\x1b[0m");
            println!("  The two answers agree, which is the only thing being asked of it.");
        }
        (false, true) => {
            println!("  `{record}` was refused and is here anyway — the write landed after");
            println!("  the answer. Not loss, but a publisher cannot tell the difference,");
            println!("  which is why a refusal means \"unknown\" and not \"did not happen\".")
        }
    }
}

// ---------------------------------------------------------------- lifecycle

/// A cluster for a client conformance suite, in any language, to run against.
///
/// The suite needs more than an address: a durable stream to observe offsets
/// on, a cache, a credential that may publish and one that may not, a stream
/// name that is deliberately absent, and the broker's certificate so a client
/// can verify properly instead of skipping verification. This starts all of
/// that and writes it where the suite can read it.
///
/// Three nodes by default, because the semantics that matter most to a client
/// only exist in a cluster: a redirect needs a broker that does not own the
/// shard, and reconnection needs somewhere to reconnect *to*. Each broker
/// exports its own certificate and they are concatenated into one PEM bundle,
/// which is what a trust store is allowed to be.
async fn client_fixture(args: &[String]) -> Result<()> {
    init_tracing(false);
    let node_count = flag_usize(args, "--nodes")?.unwrap_or(3);
    let out = flag_value(args, "--out")?.unwrap_or_else(|| {
        std::env::temp_dir()
            .join("felix-client-fixture.json")
            .display()
            .to_string()
    });
    let ca_file = flag_value(args, "--ca-file")?.unwrap_or_else(|| {
        std::env::temp_dir()
            .join("felix-client-fixture-ca.pem")
            .display()
            .to_string()
    });

    const DURABLE_STREAM: &str = "conformance";
    const CACHE: &str = "conformance";
    // Never registered. A client proving it reports "unknown stream"
    // distinguishably needs a name the broker will genuinely refuse.
    const MISSING_STREAM: &str = "conformance-absent";

    eprintln!("starting a {node_count}-node cluster for a client conformance suite...");
    let cluster = Cluster::start(ClusterConfig {
        nodes: node_count,
        // Several shards so the stream is spread across brokers: a redirect
        // scenario needs a shard whose owner is not the broker the client
        // reached, and one shard on one node cannot produce that.
        //
        // Replicated across the whole cluster because the reconnect scenario
        // asks whether the *client* survives losing a broker — and with one
        // copy per shard the answer would be confounded by the shard not
        // surviving either. A client cannot reconnect its way to data that is
        // gone.
        streams: vec![StreamSpec::replicated(DURABLE_STREAM, 4, node_count as u32)],
        caches: vec![CacheSpec::replicated(CACHE, 4, node_count as u32)],
        inherit_output: std::env::var("FELIX_CLUSTER_VERBOSE").is_ok(),
        ..Default::default()
    })
    .await?;

    // Each broker wrote its own certificate; a client needs to trust all of
    // them, and a PEM bundle holding several is exactly how that is spelled.
    let mut bundle = String::new();
    for node in &cluster.nodes {
        let path = node.data_dir.join("broker-cert.pem");
        let pem = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "read the certificate {} exported to {}",
                node.node_id,
                path.display()
            )
        })?;
        bundle.push_str(&pem);
        if !bundle.ends_with('\n') {
            bundle.push('\n');
        }
    }
    std::fs::write(&ca_file, &bundle)
        .with_context(|| format!("write the certificate bundle to {ca_file}"))?;

    let fixture = felix_conformance::kit::Fixture {
        addrs: cluster
            .nodes
            .iter()
            .map(|node| node.client_addr.to_string())
            .collect(),
        tenant_id: cluster.tenant_id.clone(),
        namespace: cluster.namespace.clone(),
        token: cluster.client_token.clone(),
        unauthorized_token: cluster.subscribe_only_token.clone(),
        ca_file: ca_file.clone(),
        durable_stream: DURABLE_STREAM.to_string(),
        cache: CACHE.to_string(),
        missing_stream: MISSING_STREAM.to_string(),
    };
    let body = serde_json::to_vec_pretty(&fixture).context("encode the fixture")?;
    std::fs::write(&out, body).with_context(|| format!("write the fixture to {out}"))?;

    println!("fixture   {out}");
    println!("broker    {}", fixture.addrs.join(", "));
    println!("ca        {ca_file}");
    eprintln!("\nholding the fixture. press Ctrl-C to tear it down.");

    // A placement pass on a timer, which is what makes the reconnect scenario
    // answerable from outside. When a broker dies its shards need a new
    // leader, and in this harness placement is driven rather than swept — the
    // Rust tests call `place_shards` themselves while they wait. A suite in
    // another language has no such handle, so the fixture does it here: an
    // unowned shard is reassigned within a second or so, exactly as a
    // control plane's own sweep would.
    let cluster = std::sync::Arc::new(cluster);
    let placement = tokio::spawn({
        let held = std::sync::Arc::clone(&cluster);
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                held.place_shards().await;
            }
        }
    });

    stop_signal().await?;
    placement.abort();
    eprintln!("\ntearing down...");
    let _ = std::fs::remove_file(&out);
    drop(cluster);
    Ok(())
}

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

/// The whole demonstration, driven end to end in one terminal.
///
/// The three-panel version reads better, but it needs three windows and someone
/// typing in them. This does the same story in one pane: the publisher's lines
/// are flush left, and records arriving at the subscriber are indented with an
/// arrow, so a viewer can still tell the two apart.
///
/// `--pace` is the gap between steps. Long enough to narrate over by default;
/// `--pace 0` runs it flat out, which is what a test wants.
async fn demo(args: &[String]) -> Result<()> {
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

/// Replication, quorum acknowledgement and failover, end to end.
///
/// The point it is making: a record the cluster acknowledged under `Quorum` is
/// still there after the broker that acknowledged it is killed, and a client
/// given more than one way in carries on without being rebuilt.
async fn failover(args: &[String]) -> Result<()> {
    init_tracing(false);
    let nodes = flag_usize(args, "--nodes")?.unwrap_or(3);
    let pace = pace_from(args)?;

    step("A three-node cluster, replicated three ways");
    println!("  Every shard is held by three brokers, and a publish is acknowledged");
    println!("  only once a majority of them has it durably.");
    let mut cluster = Cluster::start(ClusterConfig {
        nodes,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await?;
    let leader = cluster.owner(STREAM).await?;
    for node in &cluster.nodes {
        let role = if node.node_id == leader {
            "leader"
        } else {
            "replica"
        };
        println!("  {:<10} {:<22} {role}", node.node_id, node.client_addr);
    }
    beat(pace).await;

    step("A client that knows the cluster, not one broker");
    // One address on purpose. Handing it all three would hide the thing worth
    // showing: an application is configured with one endpoint far more often
    // than with a correct list of every broker.
    let seed = cluster.broker_addrs()[0];
    let client =
        felix_cluster::client::connect_cluster(&[seed], &cluster.tenant_id, &cluster.client_token)
            .await?;
    println!("  Configured with exactly one address: {seed}");
    let known = client.endpoints().await;
    println!(
        "  It asked that broker who else was there, and now knows {}:",
        plural(known.len(), "broker", "brokers"),
    );
    for addr in &known {
        println!("      {addr}");
    }
    println!("  The address it was given stays in that list. A cluster that");
    println!("  answers wrongly can never leave a client worse off than before.");
    beat(pace).await;

    step("Publishing under Quorum");
    for index in 1..=3u32 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("before-{index}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: before-{index}   (a majority holds it, not just the leader)");
    }
    wait::until(Duration::from_secs(20), "replication to settle", || async {
        matches!(
            cluster
                .metric(&leader, "felix_broker_replication_lag_records")
                .await,
            Ok(Some(lag)) if lag == 0.0
        )
    })
    .await?;
    println!("\n  Replication lag on {leader}: 0 — every replica holds everything it does.");
    beat(pace).await;

    step(&format!(
        "Killing {leader}, the broker that acknowledged them"
    ));
    let killed_at = std::time::Instant::now();
    cluster.kill_node(&leader)?;
    println!("  Gone. Not a graceful shutdown — the process is dead.");
    wait::until(
        Duration::from_secs(30),
        "a replica to be promoted",
        || async {
            cluster.place_shards().await;
            matches!(cluster.owner(STREAM).await, Ok(owner) if owner != leader)
        },
    )
    .await?;
    let promoted = cluster.owner(STREAM).await?;
    println!(
        "  {promoted} promoted after {:?}.",
        Duration::from_millis(killed_at.elapsed().as_millis() as u64)
    );
    println!("  It was chosen because it holds the log — a broker that did not");
    println!("  would serve an empty shard, and the failover would be the data loss.");
    beat(pace).await;

    step("The same client, still publishing");
    for index in 1..=2u32 {
        client
            .publish_at_least_once(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                format!("after-{index}").into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await?;
        println!("  acknowledged: after-{index}");
    }
    println!("\n  It reconnected to a surviving broker on its own. Nothing was rebuilt.");
    beat(pace).await;

    step("Reading the whole stream back from the broker that took over");
    let (_client, mut subscription) = cluster.replay_on(&promoted, STREAM).await?;
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).to_string();
        let offset = event
            .offset
            .map(|o| o.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("      ← offset {offset:>4}  {payload}");
        seen.push(payload);
    }

    let survived = (1..=3).all(|i| seen.iter().any(|p| p == &format!("before-{i}")));
    println!();
    if !survived {
        bail!("a record acknowledged under Quorum did not survive the failover");
    }
    println!("  Every record acknowledged before the kill is still here, read from a");
    println!("  broker that was a replica a moment ago.");

    // Two things in that listing look like bugs and are not, so say what they
    // are rather than leaving them to be noticed.
    let probes = seen
        .iter()
        .filter(|p| p.as_str() == "harness-probe")
        .count();
    if probes > 1 {
        println!(
            "\n  ({probes} \"harness-probe\" records: the harness publishes one per broker at"
        );
        println!("  startup to prove the cluster can serve before the demo begins.)");
    }
    let duplicated: Vec<&String> = seen
        .iter()
        .filter(|p| seen.iter().filter(|other| other == p).count() > 1)
        .filter(|p| p.starts_with("after-"))
        .collect();
    if !duplicated.is_empty() {
        println!(
            "\n  ({} appears twice. That is `publish_at_least_once` doing what its",
            duplicated[0]
        );
        println!("  name says: the publish failed after the broker had already written the");
        println!("  record, and sending it again is the only way not to lose it. Only the");
        println!("  application holds an identity that could tell the two apart.)");
    }

    cluster.shutdown().await;
    Ok(())
}

/// "1 broker" rather than "1 brokers", which reads as a bug in the demo.
fn plural(count: usize, one: &str, many: &str) -> String {
    if count == 1 {
        format!("{count} {one}")
    } else {
        format!("{count} {many}")
    }
}

fn step(title: &str) {
    println!("\n\x1b[1m── {title}\x1b[0m");
}

async fn beat(pace: Duration) {
    if !pace.is_zero() {
        tokio::time::sleep(pace).await;
    }
}

fn sorted(map: std::collections::HashMap<String, String>) -> Vec<(String, String)> {
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

/// Every broker in the cluster, one per line.
///
/// Distinct from `owners`, which lists shard leaders: a broker holding no shard
/// is still a broker you can publish through, and that is exactly the case the
/// demo is about.
fn nodes() -> Result<()> {
    let session = Session::read(&session::default_path())?;
    for node in &session.nodes {
        println!("{}", node.node_id);
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
        streams: vec![StreamSpec::new(STREAM, 1)],
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

/// How long each narrated step pauses, so the demos can be read as they run.
fn pace_from(args: &[String]) -> Result<Duration> {
    Ok(Duration::from_millis(
        (flag(args, "--pace")?
            .map(|value| value.parse::<f64>())
            .transpose()
            .context("parse --pace")?
            .unwrap_or(2.5)
            * 1000.0) as u64,
    ))
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

/// A `--flag value` pair, or `None` when the flag is absent.
fn flag_value(args: &[String], name: &str) -> Result<Option<String>> {
    let Some(position) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    args.get(position + 1)
        .cloned()
        .map(Some)
        .ok_or_else(|| anyhow!("{name} needs a value"))
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
