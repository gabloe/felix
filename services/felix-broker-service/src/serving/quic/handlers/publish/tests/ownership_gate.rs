//! The gate on the publish path itself, not just the decision behind it.

use std::collections::HashMap;

use felix_router::{RegionRouter, ShardRouter};

use super::*;
use crate::shards::routing::{IngressRouter, routing_table_from};
use crate::shards::{ShardKey as WatchKey, watch::ShardAssignment};

/// A broker with the stream registered, so only the ownership gate can
/// refuse anything below.
async fn broker_with_stream() -> Broker {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "stream",
            felix_broker::StreamMetadata::default(),
        )
        .await
        .expect("stream");
    broker
}

fn watch_key() -> WatchKey {
    WatchKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "stream".to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn ingress_for(leader: &str, servable: bool) -> IngressRouter {
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let assignment = ShardAssignment {
        key: watch_key(),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 1,
        state: "active".to_string(),
    };
    let assignments: HashMap<WatchKey, ShardAssignment> =
        [(watch_key(), assignment)].into_iter().collect();
    // A catalog with both brokers in it, because a route is only usable when
    // the owner's id has an address behind it. An empty catalog would make
    // every remote shard look unavailable rather than forwardable, which is
    // the wrong thing for these tests to be asserting against.
    let nodes: HashMap<String, felix_router::NodeRef> = ["broker-a", "broker-b"]
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            (
                node_id.to_string(),
                felix_router::NodeRef {
                    node_id: node_id.to_string(),
                    advertise_addr: std::net::SocketAddr::from((
                        [127, 0, 0, 1],
                        7000 + index as u16,
                    )),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    router.publish(routing_table_from(&assignments, &nodes), &nodes);

    let ingress = IngressRouter::new(router, Arc::default());
    if servable {
        ingress.fence().open(&watch_key(), 1);
        ingress.publish_servable([(watch_key(), 1)].into_iter().collect());
    }
    ingress
}

/// A shard this broker owns and has opened resolves normally.
#[tokio::test]
async fn an_owned_shard_resolves() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", true);
    let mut cache = HashMap::new();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(
        matches!(route, PublishRoute::Local { .. }),
        "an owned, open shard must be servable",
    );
}

/// The acceptance criterion, on the real path: a broker must not write a
/// shard it does not own, however healthy the stream is locally. It routes
/// the publish to the owner instead.
#[tokio::test]
async fn a_shard_owned_elsewhere_is_forwarded_not_served_locally() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-b", false);
    let mut cache = HashMap::new();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    match route {
        PublishRoute::Forward(target) => assert_eq!(target.node_id, "broker-b"),
        other => panic!("must route to the owner, not serve locally: {other:?}"),
    }
}

/// Owned but not yet opened. The stream exists locally, so only the gate
/// can refuse this.
#[tokio::test]
async fn an_owned_but_unopened_shard_is_refused() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", false);
    let mut cache = HashMap::new();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: Some(&ingress),
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    let PublishRoute::Refused(refusal) = route else {
        panic!("an unopened shard must not accept writes");
    };
    // And the client is told why, in a form it can act on.
    let felix_wire::Message::Error { code, detail, .. } = refusal.into_message() else {
        unreachable!()
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

/// A subscribe to a shard nobody can serve names the reason, not only prose.
#[tokio::test]
async fn an_unservable_subscribe_is_shard_unavailable() {
    let ingress = ingress_for("broker-a", false);
    let answer = crate::serving::quic::handlers::redirect::redirect_for(
        Some(&ingress),
        None,
        "t1",
        "ns",
        "stream",
        0,
        crate::shards::ShardKind::Stream,
        felix_wire::FEATURE_ERROR_CODES,
    );
    let Some(felix_wire::Message::Error {
        code,
        retry,
        detail,
        ..
    }) = answer
    else {
        panic!("expected an error, got {answer:?}");
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(retry, Some(felix_wire::RetryClass::Retry));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

/// Ownership is checked outside the handle cache, so a reassignment takes
/// effect immediately rather than after the cache TTL.
#[tokio::test]
async fn losing_a_shard_takes_effect_without_waiting_for_the_cache() {
    let broker = broker_with_stream().await;
    let ingress = ingress_for("broker-a", true);
    let mut cache = HashMap::new();
    let mut key = String::new();

    assert!(
        matches!(
            resolve_route(
                &broker,
                Authority {
                    ingress: Some(&ingress),
                    lease: None
                },
                &mut cache,
                &mut key,
                "t1",
                "ns",
                "stream",
                0
            )
            .await,
            PublishRoute::Local { .. }
        ),
        "warm the handle cache while the shard is ours",
    );

    // The shard moves away. The stream handle is still cached and valid, so
    // only the gate can notice.
    let moved = ingress_for("broker-b", false);
    assert!(
        matches!(
            resolve_route(
                &broker,
                Authority {
                    ingress: Some(&moved),
                    lease: None
                },
                &mut cache,
                &mut key,
                "t1",
                "ns",
                "stream",
                0
            )
            .await,
            PublishRoute::Forward(_)
        ),
        "a cached handle must not outlive ownership",
    );
}

/// The single-node path: no router, no gate, unchanged behaviour.
#[tokio::test]
async fn a_single_node_broker_is_unaffected() {
    let broker = broker_with_stream().await;
    let mut cache = HashMap::new();
    let mut key = String::new();

    let route = resolve_route(
        &broker,
        Authority {
            ingress: None,
            lease: None,
        },
        &mut cache,
        &mut key,
        "t1",
        "ns",
        "stream",
        0,
    )
    .await;
    assert!(matches!(route, PublishRoute::Local { .. }));
}
