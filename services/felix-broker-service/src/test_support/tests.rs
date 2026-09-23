use super::*;
use axum::http::StatusCode;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn wait_for_listen_succeeds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    wait_for_listen(addr).await.expect("ready");
}

#[tokio::test]
#[serial]
async fn wait_for_listen_times_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let err = wait_for_listen(addr).await.expect_err("should timeout");
    assert!(
        err.to_string().contains("server not ready"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
#[serial]
async fn get_with_context_reports_phase() {
    let client = build_test_client().expect("client");
    let url = "http://127.0.0.1:1/metrics";
    let err = get_with_context(&client, url, "connect")
        .await
        .expect_err("expected error");
    assert!(
        err.to_string().contains("connect GET"),
        "missing phase context: {err}"
    );
}

#[tokio::test]
#[serial]
async fn get_with_context_succeeds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = axum::Router::new().route("/ok", axum::routing::get(|| async { "ok" }));
    let (shutdown_tx, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("ready");

    let client = build_test_client().expect("client");
    let url = format!("http://{}/ok", addr);
    let response = get_with_context(&client, &url, "GET /ok")
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::OK);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("server shutdown");
}
