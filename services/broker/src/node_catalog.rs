//! The address book: which broker is where.
//!
//! Shard assignments name an owner by node id. Forwarding needs an address, and
//! only the control plane's node catalog has one. Without this the router
//! resolves every remote shard to "owner unavailable" — the assignment is known
//! and the node behind it is not.
//!
//! Read with the same credential as the assignment feed: `/v1/nodes` requires
//! `node.view:cluster:*`, which a broker already holds to read assignments at
//! all.
use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use felix_router::NodeRef;
use felix_wire::BrokerEndpoint;
use serde::Deserialize;

/// One fetch, read two ways.
///
/// The same listing answers "where does this broker forward to" and "where may
/// a client connect", and they are different addresses on different listeners.
/// Fetching once and splitting here keeps them from drifting apart.
#[derive(Debug, Default)]
pub struct NodeCatalog {
    pub nodes: HashMap<String, NodeRef>,
    /// Sorted by node id, so the answer a client gets does not reshuffle
    /// between refreshes that changed nothing.
    pub client_endpoints: Vec<BrokerEndpoint>,
}

#[derive(Debug, Deserialize)]
struct NodeListResponse {
    items: Vec<NodeView>,
}

#[derive(Debug, Deserialize)]
struct NodeView {
    node: Node,
    placement: Placement,
}

#[derive(Debug, Deserialize)]
struct Node {
    node_id: String,
    spec: NodeSpec,
}

#[derive(Debug, Deserialize)]
struct NodeSpec {
    advertise_addr: String,
    /// Absent on a broker that predates client discovery, or one whose operator
    /// has not said where clients reach it.
    #[serde(default)]
    client_addr: Option<String>,
    region: String,
}

#[derive(Debug, Deserialize)]
struct Placement {
    eligible: bool,
}

/// Fetch the catalog.
///
/// A node whose advertised address does not parse is skipped rather than
/// failing the fetch: one malformed registration must not cost this broker every
/// other route it knows.
pub async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
) -> Result<NodeCatalog> {
    let mut request = client.get(format!("{base_url}/v1/nodes"));
    if let Some(bearer) = bearer {
        request = request.bearer_auth(bearer);
    }
    let response = request.send().await.context("send node list request")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("{status}: {body}"));
    }
    let response: NodeListResponse = response.json().await.context("decode node list")?;
    Ok(into_catalog(response))
}

/// Turn the control plane's answer into routable entries.
///
/// Separate from the request so the skipping rule above is testable without a
/// server standing in for the control plane.
fn into_catalog(response: NodeListResponse) -> NodeCatalog {
    let mut catalog = HashMap::with_capacity(response.items.len());
    let mut client_endpoints = Vec::new();
    for item in response.items {
        // Offered to clients only while the cluster considers this broker able
        // to serve, and only when it said where clients reach it. Sending a
        // client to a broker that is down, or to the internal listener that
        // would refuse it, is worse than sending it nowhere.
        if item.placement.eligible
            && let Some(client_addr) = &item.node.spec.client_addr
            && client_addr.parse::<std::net::SocketAddr>().is_ok()
        {
            client_endpoints.push(BrokerEndpoint {
                node_id: item.node.node_id.clone(),
                addr: client_addr.clone(),
            });
        }
        let Ok(advertise_addr) = item.node.spec.advertise_addr.parse() else {
            tracing::warn!(
                node_id = %item.node.node_id,
                advertise_addr = %item.node.spec.advertise_addr,
                "skipping node: advertised address does not parse",
            );
            continue;
        };
        catalog.insert(
            item.node.node_id.clone(),
            NodeRef {
                node_id: item.node.node_id,
                advertise_addr,
                region: item.node.spec.region,
                // The control plane's own verdict, which folds in lifecycle and
                // heartbeat age together. A broker re-deriving that from the
                // lifecycle alone would keep forwarding to a node whose
                // heartbeat has lapsed but whose sweep has not yet run.
                live: item.placement.eligible,
            },
        );
    }
    client_endpoints.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    NodeCatalog {
        nodes: catalog,
        client_endpoints,
    }
}

#[cfg(test)]
#[path = "node_catalog_tests.rs"]
mod tests;
