//! Helpers shared by the integration tests.
//!
//! Each test binary compiles its own copy of this module, and no single
//! binary uses every helper — so per-target dead-code analysis is noise here.
#![allow(dead_code)]

use axum::body::Body;
use axum::http::Request;
pub(crate) fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(crate) async fn read_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}
