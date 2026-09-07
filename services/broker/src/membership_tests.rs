//! Membership client behaviour against a stub control plane.
use super::*;
use crate::test_support::http_test::{
    build_test_client, spawn_axum_with_shutdown, wait_for_listen,
};
use axum::Json;
use axum::extract::{Path, State};
use axum::routing::post;
use serde_json::json;
use std::sync::Mutex;

/// Records what the broker sent, so a test can assert on the calls rather than
/// on the client's own bookkeeping.
#[derive(Default)]
struct Calls {
    registrations: Vec<serde_json::Value>,
    heartbeats: Vec<u64>,
    drained: Vec<String>,
    deregistered: Vec<String>,
    /// Heartbeats to reject before succeeding, so backoff can be exercised.
    fail_heartbeats: usize,
}

type Shared = Arc<Mutex<Calls>>;

fn stub_control_plane(state: Shared) -> axum::Router {
    axum::Router::new()
        .route(
            "/v1/nodes",
            post(
                |State(state): State<Shared>, Json(body): Json<serde_json::Value>| async move {
                    let incarnation = {
                        let mut calls = state.lock().expect("lock");
                        calls.registrations.push(body);
                        calls.registrations.len() as u64 - 1
                    };
                    Json(json!({
                        "node": { "status": { "incarnation": incarnation, "lifecycle": "live" } },
                        "heartbeat_interval_ms": 20,
                        "expiry_timeout_ms": 60,
                    }))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/heartbeat",
            post(
                |State(state): State<Shared>,
                 Path(_): Path<String>,
                 Json(body): Json<serde_json::Value>| async move {
                    let mut calls = state.lock().expect("lock");
                    if calls.fail_heartbeats > 0 {
                        calls.fail_heartbeats -= 1;
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    calls
                        .heartbeats
                        .push(body["incarnation"].as_u64().unwrap_or_default());
                    Ok(Json(
                        json!({ "lifecycle": "live", "heartbeat_interval_ms": 20 }),
                    ))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            post(
                |State(state): State<Shared>, Path(id): Path<String>| async move {
                    state.lock().expect("lock").drained.push(id);
                    Json(json!({}))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/deregister",
            post(
                |State(state): State<Shared>, Path(id): Path<String>| async move {
                    state.lock().expect("lock").deregistered.push(id);
                    Json(json!({}))
                },
            ),
        )
        .with_state(state)
}

async fn serve(
    state: Shared,
) -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, stub_control_plane(state));
    wait_for_listen(addr).await.expect("listen");
    (format!("http://{addr}"), stop, handle)
}

fn config() -> MembershipConfig {
    MembershipConfig {
        node_id: "broker-a".to_string(),
        advertise_addr: "10.0.0.4:7000".to_string(),
        region: "us-west-2".to_string(),
    }
}

#[tokio::test]
async fn registration_sends_the_identity_and_returns_the_incarnation() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let registration = register(&client, &base_url, &config())
        .await
        .expect("register");
    assert_eq!(registration.node_id, "broker-a");
    assert_eq!(registration.incarnation, 0);
    assert_eq!(registration.heartbeat_interval_ms, 20);

    let sent = calls.lock().expect("lock").registrations[0].clone();
    assert_eq!(sent["node_id"], "broker-a");
    assert_eq!(sent["advertise_addr"], "10.0.0.4:7000");
    assert_eq!(sent["region"], "us-west-2");
    // Observed status is the control plane's to set.
    assert!(sent.get("status").is_none());

    let _ = stop.send(());
    let _ = handle.await;
}

/// A broker that cannot register is not a cluster member, so this must not be
/// swallowed into "carry on and hope".
#[tokio::test]
async fn a_rejected_registration_is_an_error() {
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { (axum::http::StatusCode::CONFLICT, "address in use") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let err = register(&client, &format!("http://{addr}"), &config())
        .await
        .expect_err("should fail");
    assert!(err.to_string().contains("rejected registration"), "{err}");

    let _ = stop.send(());
    let _ = handle.await;
}

/// Every heartbeat carries this process's incarnation, so one delayed past a
/// restart is rejected rather than counted for its successor.
#[tokio::test]
async fn heartbeats_carry_the_registered_incarnation() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let mut registration = register(&client, &base_url, &config())
        .await
        .expect("register");
    registration.incarnation = 7;

    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::clone(&failures),
    ));

    // Wait for a few beats rather than a fixed sleep.
    for _ in 0..200 {
        if calls.lock().expect("lock").heartbeats.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    beating.await.expect("heartbeat task");

    let sent = calls.lock().expect("lock").heartbeats.clone();
    assert!(
        sent.len() >= 2,
        "expected repeated heartbeats, got {sent:?}"
    );
    assert!(sent.iter().all(|i| *i == 7), "{sent:?}");
    assert_eq!(failures.load(Ordering::Acquire), 0);

    let _ = stop.send(());
    let _ = handle.await;
}

/// The control plane being briefly unreachable must not stop a broker that is
/// otherwise serving. The failure has to be visible, not fatal.
#[tokio::test]
async fn heartbeat_failures_are_counted_and_then_recovered_from() {
    let calls: Shared = Arc::new(Mutex::new(Calls {
        fail_heartbeats: 3,
        ..Calls::default()
    }));
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let registration = register(&client, &base_url, &config())
        .await
        .expect("register");
    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::clone(&failures),
    ));

    for _ in 0..400 {
        if !calls.lock().expect("lock").heartbeats.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    beating.await.expect("heartbeat task");

    assert!(
        !calls.lock().expect("lock").heartbeats.is_empty(),
        "the loop should recover once the control plane does",
    );
    // Reset on success, so the counter reads "currently failing", not "ever failed".
    assert_eq!(failures.load(Ordering::Acquire), 0);

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn draining_and_deregistering_hit_the_right_endpoints() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    drain(&client, &base_url, "broker-a").await.expect("drain");
    deregister(&client, &base_url, "broker-a")
        .await
        .expect("deregister");

    {
        let calls = calls.lock().expect("lock");
        assert_eq!(calls.drained, vec!["broker-a".to_string()]);
        assert_eq!(calls.deregistered, vec!["broker-a".to_string()]);
    }

    let _ = stop.send(());
    let _ = handle.await;
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    let interval = Duration::from_millis(100);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), interval * 2);
    assert_eq!(backoff(interval, 3), interval * 4);
    // Capped, so a long outage does not turn into an hours-long retry gap.
    assert_eq!(backoff(interval, 60), MAX_RETRY_BACKOFF);
    assert_eq!(backoff(Duration::from_secs(120), 1), MAX_RETRY_BACKOFF);
}

/// Jitter only ever delays, and only within the documented fraction: a
/// heartbeat pulled earlier would tighten the cadence the control plane sized
/// its expiry window against.
#[test]
fn jitter_delays_within_its_bound() {
    let delay = Duration::from_millis(1000);
    for _ in 0..50 {
        let jittered = jittered(delay);
        assert!(
            jittered >= delay,
            "{jittered:?} moved earlier than {delay:?}"
        );
        assert!(
            jittered <= delay.mul_f64(1.0 + JITTER_FRACTION),
            "{jittered:?} exceeded the jitter bound",
        );
    }
}

/// The acceptance criterion: a refusal and an outage must be tellable apart. A
/// misconfigured broker retrying forever looks exactly like a flaky network if
/// both increment one counter.
#[tokio::test]
async fn a_refusal_and_an_outage_are_different_kinds() {
    // 409 from a live control plane: the server answered and said no.
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { (axum::http::StatusCode::CONFLICT, "address in use") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let refused = register(&client, &format!("http://{addr}"), &config())
        .await
        .expect_err("should be refused");
    assert_eq!(refused.kind(), crate::membership_metrics::KIND_REJECTED);

    let _ = stop.send(());
    let _ = handle.await;

    // Nothing listening: no answer at all.
    let outage = register(&client, &format!("http://{addr}"), &config())
        .await
        .expect_err("should be unavailable");
    assert_eq!(outage.kind(), crate::membership_metrics::KIND_UNAVAILABLE);
}

/// A 5xx is the control plane failing, not refusing, so it is retryable like an
/// outage rather than terminal like a rejection.
#[tokio::test]
async fn a_server_error_counts_as_an_outage_not_a_refusal() {
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let err = register(&client, &format!("http://{addr}"), &config())
        .await
        .expect_err("should fail");
    assert_eq!(err.kind(), crate::membership_metrics::KIND_UNAVAILABLE);
    assert!(
        matches!(err, MembershipError::Unavailable(_)),
        "a 5xx must stay retryable",
    );

    let _ = stop.send(());
    let _ = handle.await;
}
