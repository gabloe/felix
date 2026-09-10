//! Parsing the control plane's node list.
//!
//! The skipping rule is the part worth testing: one broker registered with an
//! address nobody can parse must not cost this broker every other route it
//! knows, because losing the catalog means refusing every remote publish.
use super::*;

fn response(json: serde_json::Value) -> NodeListResponse {
    serde_json::from_value(json).expect("decode")
}

fn node(node_id: &str, addr: &str, eligible: bool) -> serde_json::Value {
    serde_json::json!({
        "node": {
            "node_id": node_id,
            "spec": { "advertise_addr": addr, "region": "us-west-2" },
        },
        "placement": { "eligible": eligible },
    })
}

#[test]
fn a_registered_node_becomes_a_routable_entry() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "10.0.0.4:7000", true)],
    })));

    let entry = catalog.get("broker-a").expect("broker-a");
    assert_eq!(entry.node_id, "broker-a");
    assert_eq!(entry.advertise_addr.to_string(), "10.0.0.4:7000");
    assert_eq!(entry.region, "us-west-2");
    assert!(entry.live);
}

/// The control plane's `eligible` is taken as-is. Re-deriving liveness from the
/// lifecycle alone would keep forwarding to a node whose heartbeat has lapsed
/// but whose expiry sweep has not run yet.
#[test]
fn placement_eligibility_decides_liveness() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "10.0.0.4:7000", false)],
    })));
    assert!(!catalog.get("broker-a").expect("broker-a").live);
}

/// One unparseable address costs that node and nothing else.
#[test]
fn a_node_with_an_unusable_address_is_skipped_and_the_rest_survive() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node("broker-a", "10.0.0.4:7000", true),
            node("broker-b", "not-an-address", true),
            node("broker-c", "10.0.0.6:7000", true),
        ],
    })));

    assert!(catalog.contains_key("broker-a"));
    assert!(catalog.contains_key("broker-c"));
    assert!(
        !catalog.contains_key("broker-b"),
        "an address that does not parse cannot be forwarded to",
    );
    assert_eq!(
        catalog.len(),
        2,
        "the malformed entry must not take others with it"
    );
}

/// A hostname is not an address. The catalog holds `SocketAddr`, and resolving
/// names is not something the publish path can afford to do.
#[test]
fn a_hostname_is_not_accepted_as_an_address() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "broker-a.internal:7000", true)],
    })));
    assert!(catalog.is_empty());
}

#[test]
fn an_empty_catalog_decodes_to_an_empty_map() {
    let catalog = into_catalog(response(serde_json::json!({ "items": [] })));
    assert!(catalog.is_empty());
}

/// Two registrations for one id cannot both be routable; the map holds one.
#[test]
fn a_repeated_node_id_yields_one_entry() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node("broker-a", "10.0.0.4:7000", true),
            node("broker-a", "10.0.0.9:7000", true),
        ],
    })));
    assert_eq!(catalog.len(), 1);
}

/// IPv6 is a valid advertised address and must not be dropped as malformed.
#[test]
fn an_ipv6_address_is_routable() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "[::1]:7000", true)],
    })));
    assert!(catalog.contains_key("broker-a"));
}

/// The request half, against a real HTTP server.
///
/// A stub client would not exercise the status check or the bearer header, and
/// those are the two ways this call fails in a deployment: a control plane that
/// is up but refusing, and one that answers something other than a node list.
mod over_http {
    use super::*;
    use axum::Router;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        (format!("http://{addr}"), task)
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .no_proxy()
            .build()
            .expect("client")
    }

    #[tokio::test]
    async fn a_node_list_is_fetched_and_parsed() {
        let body = serde_json::json!({ "items": [node("broker-a", "10.0.0.4:7000", true)] });
        let app = Router::new().route(
            "/v1/nodes",
            get(move || {
                let body = body.clone();
                async move { axum::Json(body) }
            }),
        );
        let (base, task) = serve(app).await;

        let catalog = fetch(&client(), &base, Some("a-token"))
            .await
            .expect("fetch");
        assert!(catalog.contains_key("broker-a"));

        task.abort();
    }

    /// The credential is sent. Without it the control plane answers 403 and the
    /// broker silently loses every route it could forward to.
    #[tokio::test]
    async fn the_bearer_token_is_sent() {
        let app = Router::new().route(
            "/v1/nodes",
            get(|headers: HeaderMap| async move {
                match headers.get(axum::http::header::AUTHORIZATION) {
                    Some(value) if value == "Bearer a-token" => {
                        Ok(axum::Json(serde_json::json!({ "items": [] })))
                    }
                    _ => Err(StatusCode::FORBIDDEN),
                }
            }),
        );
        let (base, task) = serve(app).await;

        fetch(&client(), &base, Some("a-token"))
            .await
            .expect("the token should have been accepted");

        task.abort();
    }

    /// A refusal is an error, not an empty catalog. Treating 403 as "no nodes"
    /// would drop every route and look identical to a cluster with no brokers.
    #[tokio::test]
    async fn a_refusal_is_an_error_rather_than_an_empty_catalog() {
        let app = Router::new().route(
            "/v1/nodes",
            get(|| async { (StatusCode::FORBIDDEN, "missing node.view:cluster:*") }),
        );
        let (base, task) = serve(app).await;

        let err = fetch(&client(), &base, None)
            .await
            .expect_err("403 must not read as an empty catalog");
        let message = err.to_string();
        assert!(message.contains("403"), "{message}");
        assert!(
            message.contains("missing node.view"),
            "the body says why, and it should survive: {message}",
        );

        task.abort();
    }

    #[tokio::test]
    async fn a_body_that_is_not_a_node_list_is_an_error() {
        let app = Router::new().route("/v1/nodes", get(|| async { "not json" }));
        let (base, task) = serve(app).await;

        assert!(fetch(&client(), &base, None).await.is_err());

        task.abort();
    }

    /// Nothing listening at all, which is a control plane that is down.
    #[tokio::test]
    async fn an_unreachable_control_plane_is_an_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        assert!(
            fetch(&client(), &format!("http://{addr}"), None)
                .await
                .is_err()
        );
    }
}
