//! Continuing a caller's trace: W3C `traceparent` headers in, a parent
//! OpenTelemetry context out.
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::propagation::Extractor;

/// Installs the propagator on first use, so extraction works even where
/// observability was never initialized, as in router tests.
static PROPAGATOR_INIT: OnceLock<()> = OnceLock::new();

/// The parent context `headers` carry; empty when they carry none.
pub(crate) fn trace_context_from_headers(
    headers: &axum::http::HeaderMap,
) -> opentelemetry::Context {
    PROPAGATOR_INIT.get_or_init(|| {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
    });
    global::get_text_map_propagator(|prop| prop.extract(&HeaderMapExtractor(headers)))
}

struct HeaderMapExtractor<'a>(&'a axum::http::HeaderMap);

impl<'a> Extractor for HeaderMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests;
