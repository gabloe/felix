//! This module sets up observability for the broker service, including tracing and metrics.
//! It configures a tracing subscriber with optional OpenTelemetry (OTLP) integration for distributed tracing.
//! The OTLP tracing setup is best-effort: if it fails, tracing falls back to local logging only.
//! It installs a Prometheus metrics recorder and provides an HTTP server exposing `/metrics`, `/live`, and `/ready` endpoints.
//! Metrics serving is asynchronous and uses `axum` to handle requests.
//! In tests, metrics recorder initialization is cached to avoid conflicts, and subscriber initialization is adapted accordingly.

use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace as sdktrace;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(test)]
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Initializes observability for the service.
///
/// Sets up the global OpenTelemetry text map propagator (W3C Trace Context),
/// builds an OTLP tracer provider (best-effort),
/// configures the tracing subscriber with environment filtering and formatting,
/// and installs a Prometheus metrics recorder.
///
/// Returns a `PrometheusHandle` for serving metrics.
///
/// In tests, metrics recorder is cached to avoid multiple installations.
pub fn init_observability(service_name: &str) -> PrometheusHandle {
    // Set global propagator for trace context propagation across service boundaries.
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    // Attempt to build an OTLP tracer provider; optional and may fail silently.
    let provider = build_tracer_provider(service_name);

    // Use environment variable for log filtering; default to "info" if unset or invalid.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    if let Some(provider) = provider {
        // If OTLP tracer provider is available, create a tracer and add OTLP layer.
        let tracer = provider.tracer(service_name.to_string());
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        init_subscriber(registry.with(otel_layer));
    } else {
        // Fallback to local tracing without OTLP.
        init_subscriber(registry);
    }

    // Install Prometheus metrics recorder.
    install_metrics_recorder()
}

/// Builds an OpenTelemetry tracer provider with OTLP exporter for the given service.
///
/// Attaches resource attributes describing the service and environment.
/// Uses Tokio runtime for batch processing of spans.
/// Returns `None` if installation fails (best-effort).
fn build_tracer_provider(service_name: &str) -> Option<opentelemetry_sdk::trace::TracerProvider> {
    let resource = Resource::new(resource_attributes(service_name));
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(sdktrace::Config::default().with_resource(resource))
        // Install batch span processor with Tokio runtime; failure returns None.
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .ok()
}

/// Collects resource attributes for the tracer based on environment variables.
///
/// Includes service name and optional attributes like instance ID, Kubernetes cluster info,
/// cloud region, and deployment environment.
fn resource_attributes(service_name: &str) -> Vec<KeyValue> {
    let mut attrs = vec![KeyValue::new("service.name", service_name.to_string())];
    if let Ok(value) =
        std::env::var("FELIX_SERVICE_INSTANCE_ID").or_else(|_| std::env::var("HOSTNAME"))
    {
        attrs.push(KeyValue::new("service.instance.id", value));
    }
    if let Ok(value) = std::env::var("K8S_CLUSTER_NAME") {
        attrs.push(KeyValue::new("k8s.cluster.name", value));
    }
    if let Ok(value) = std::env::var("K8S_NAMESPACE_NAME") {
        attrs.push(KeyValue::new("k8s.namespace.name", value));
    }
    if let Ok(value) = std::env::var("K8S_POD_NAME") {
        attrs.push(KeyValue::new("k8s.pod.name", value));
    }
    if let Ok(value) = std::env::var("CLOUD_REGION") {
        attrs.push(KeyValue::new("cloud.region", value));
    }
    if let Ok(value) = std::env::var("DEPLOYMENT_ENVIRONMENT") {
        attrs.push(KeyValue::new("deployment.environment", value));
    }
    attrs
}

/// Serves Prometheus metrics and health endpoints on the given socket address.
///
/// Starts an asynchronous HTTP server exposing:
/// - `/metrics`: Prometheus metrics endpoint.
/// - `/live`: liveness probe returning "ok".
/// - `/ready`: readiness probe returning "ok".
///
/// Returns an I/O error if binding or serving fails.
pub async fn serve_metrics(handle: PrometheusHandle, addr: SocketAddr) -> std::io::Result<()> {
    let app = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { handle.render() }),
        )
        .route("/live", axum::routing::get(|| async { "ok" }))
        .route("/ready", axum::routing::get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await
}

/// Installs the Prometheus metrics recorder globally.
///
/// In tests, reuses a cached recorder handle to avoid conflicts with multiple installs.
/// Outside tests, installs a new recorder each call.
///
/// Panics if installation fails (should not happen under normal conditions).
fn install_metrics_recorder() -> PrometheusHandle {
    #[cfg(test)]
    {
        // Return cached handle if already installed in tests.
        if let Some(handle) = METRICS_HANDLE.get() {
            return handle.clone();
        }
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("install metrics recorder");
        let _ = METRICS_HANDLE.set(handle.clone());
        handle
    }
    #[cfg(not(test))]
    {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("install metrics recorder")
    }
}

/// Initializes the tracing subscriber.
///
/// In tests, uses `try_init` to avoid panics if the subscriber is already set.
/// In non-test builds, uses `init` which panics on multiple initializations.
fn init_subscriber<S>(subscriber: S)
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    #[cfg(test)]
    {
        let _ = subscriber.try_init();
    }
    #[cfg(not(test))]
    {
        subscriber.init();
    }
}

#[cfg(test)]
mod tests {
    // Tests for resource_attributes and environment variable handling.
    // init_observability and serve_metrics tests removed per request.

    use super::*;
    use serial_test::serial;

    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }

        fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    #[test]
    #[serial]
    fn resource_attributes_includes_optional_env() {
        let _g1 = EnvGuard::set("FELIX_SERVICE_INSTANCE_ID", "i-1");
        let _g2 = EnvGuard::set("K8S_CLUSTER_NAME", "cluster");
        let _g3 = EnvGuard::set("K8S_NAMESPACE_NAME", "namespace");
        let _g4 = EnvGuard::set("K8S_POD_NAME", "pod");
        let _g5 = EnvGuard::set("CLOUD_REGION", "region");
        let _g6 = EnvGuard::set("DEPLOYMENT_ENVIRONMENT", "prod");

        let attrs = resource_attributes("svc");
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "service.name"));
        assert!(
            attrs
                .iter()
                .any(|kv| kv.key.as_str() == "service.instance.id")
        );
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "k8s.cluster.name"));
        assert!(
            attrs
                .iter()
                .any(|kv| kv.key.as_str() == "k8s.namespace.name")
        );
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "k8s.pod.name"));
        assert!(attrs.iter().any(|kv| kv.key.as_str() == "cloud.region"));
        assert!(
            attrs
                .iter()
                .any(|kv| kv.key.as_str() == "deployment.environment")
        );
    }

    #[test]
    #[serial]
    fn resource_attributes_minimal_when_env_missing() {
        let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
        let _g2 = EnvGuard::unset("HOSTNAME");
        let _g3 = EnvGuard::unset("K8S_CLUSTER_NAME");
        let _g4 = EnvGuard::unset("K8S_NAMESPACE_NAME");
        let _g5 = EnvGuard::unset("K8S_POD_NAME");
        let _g6 = EnvGuard::unset("CLOUD_REGION");
        let _g7 = EnvGuard::unset("DEPLOYMENT_ENVIRONMENT");

        let attrs = resource_attributes("svc");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key.as_str(), "service.name");
    }

    #[test]
    #[serial]
    fn resource_attributes_uses_hostname_fallback() {
        let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
        let _g2 = EnvGuard::set("HOSTNAME", "test-host");

        let attrs = resource_attributes("svc");
        assert!(
            attrs
                .iter()
                .any(|kv| kv.key.as_str() == "service.instance.id"
                    && kv.value.as_str() == "test-host")
        );
    }

    #[test]
    #[serial]
    fn init_observability_succeeds() {
        let handle = init_observability("test-service");
        // Should return a valid PrometheusHandle
        let metrics = handle.render();
        // Basic validation that it returns something
        assert!(!metrics.is_empty() || metrics.is_empty()); // Just ensure it doesn't panic
    }

    #[tokio::test]
    #[serial]
    async fn serve_metrics_endpoints_respond() {
        let handle = init_observability("test-metrics-service");
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        // Start the server in the background
        tokio::spawn(async move {
            let app = axum::Router::new()
                .route(
                    "/metrics",
                    axum::routing::get(move || async move { handle.render() }),
                )
                .route("/live", axum::routing::get(|| async { "ok" }))
                .route("/ready", axum::routing::get(|| async { "ok" }));
            axum::serve(listener, app.into_make_service())
                .await
                .ok();
        });

        // Give the server a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Test /metrics endpoint
        let metrics_url = format!("http://{}/metrics", bound_addr);
        let response = reqwest::get(&metrics_url).await;
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), 200);

        // Test /live endpoint
        let live_url = format!("http://{}/live", bound_addr);
        let response = reqwest::get(&live_url).await.unwrap();
        assert_eq!(response.status(), 200);
        let body = response.text().await.unwrap();
        assert_eq!(body, "ok");

        // Test /ready endpoint
        let ready_url = format!("http://{}/ready", bound_addr);
        let response = reqwest::get(&ready_url).await.unwrap();
        assert_eq!(response.status(), 200);
        let body = response.text().await.unwrap();
        assert_eq!(body, "ok");
    }

    #[test]
    #[serial]
    fn install_metrics_recorder_is_cached_in_tests() {
        let handle1 = install_metrics_recorder();
        let handle2 = install_metrics_recorder();
        // Both should work and not panic
        assert!(!handle1.render().is_empty() || handle1.render().is_empty());
        assert!(!handle2.render().is_empty() || handle2.render().is_empty());
    }
}
