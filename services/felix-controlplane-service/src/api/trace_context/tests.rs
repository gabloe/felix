use opentelemetry::trace::{TraceContextExt, TraceId};

use super::*;

#[test]
fn header_extractor_reads_values() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    headers.insert("tracestate", "congo=t61rcWkgMzE".parse().unwrap());
    let extractor = HeaderMapExtractor(&headers);

    assert!(extractor.get("traceparent").is_some());
    let keys = extractor.keys();
    assert!(keys.contains(&"traceparent"));
    assert!(keys.contains(&"tracestate"));
}

#[test]
fn header_extractor_ignores_invalid_utf8() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "traceparent",
        axum::http::HeaderValue::from_bytes(b"\xFF").unwrap(),
    );
    let extractor = HeaderMapExtractor(&headers);
    assert!(extractor.get("traceparent").is_none());
}

#[test]
fn trace_context_extracts_span_context() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    let context = trace_context_from_headers(&headers);
    let binding = context.span();
    let span_ctx = binding.span_context();
    assert!(span_ctx.is_valid());
    assert_eq!(
        span_ctx.trace_id(),
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
    );
}
