use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace as sdktrace;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static OBS_INIT: OnceLock<()> = OnceLock::new();
static PROPAGATOR_INIT: OnceLock<()> = OnceLock::new();

pub fn init_observability(service_name: &str) -> PrometheusHandle {
    OBS_INIT.get_or_init(|| {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let provider = build_tracer_provider(service_name);
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer();
        let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);
        if let Some(provider) = provider {
            let tracer = provider.tracer(service_name.to_string());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            let _ = registry.with(otel_layer).try_init();
        } else {
            let _ = registry.try_init();
        }
    });

    install_metrics_recorder()
}

fn build_tracer_provider(service_name: &str) -> Option<opentelemetry_sdk::trace::TracerProvider> {
    let resource = Resource::new(resource_attributes(service_name));
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(sdktrace::Config::default().with_resource(resource))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .ok()
}

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

pub fn trace_context_from_headers(headers: &axum::http::HeaderMap) -> opentelemetry::Context {
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

pub async fn serve_metrics(handle: PrometheusHandle, addr: SocketAddr) -> std::io::Result<()> {
    serve_metrics_with_shutdown(handle, addr, std::future::pending()).await
}

async fn serve_metrics_with_shutdown<F>(
    handle: PrometheusHandle,
    addr: SocketAddr,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move { handle.render() }),
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown)
        .await
}

fn install_metrics_recorder() -> PrometheusHandle {
    if let Some(handle) = METRICS_HANDLE.get() {
        return handle.clone();
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("install metrics recorder");
    let _ = METRICS_HANDLE.set(handle.clone());
    handle
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{TraceContextExt, TraceId};
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
    fn resource_attributes_capture_env() {
        let _g1 = EnvGuard::set("FELIX_SERVICE_INSTANCE_ID", "instance-1");
        let _g2 = EnvGuard::set("K8S_CLUSTER_NAME", "cluster-a");
        let _g3 = EnvGuard::set("K8S_NAMESPACE_NAME", "ns-a");
        let _g4 = EnvGuard::set("K8S_POD_NAME", "pod-a");
        let _g5 = EnvGuard::set("CLOUD_REGION", "region-a");
        let _g6 = EnvGuard::set("DEPLOYMENT_ENVIRONMENT", "staging");

        let attrs = resource_attributes("controlplane");
        let mut found = std::collections::HashMap::new();
        for attr in attrs {
            found.insert(attr.key.as_str().to_string(), attr.value.to_string());
        }

        assert_eq!(found.get("service.name"), Some(&"controlplane".to_string()));
        assert_eq!(
            found.get("service.instance.id"),
            Some(&"instance-1".to_string())
        );
        assert_eq!(
            found.get("k8s.cluster.name"),
            Some(&"cluster-a".to_string())
        );
        assert_eq!(found.get("k8s.namespace.name"), Some(&"ns-a".to_string()));
        assert_eq!(found.get("k8s.pod.name"), Some(&"pod-a".to_string()));
        assert_eq!(found.get("cloud.region"), Some(&"region-a".to_string()));
        assert_eq!(
            found.get("deployment.environment"),
            Some(&"staging".to_string())
        );
    }

    #[test]
    #[serial]
    fn resource_attributes_hostname_fallback() {
        let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
        let _g2 = EnvGuard::set("HOSTNAME", "host-1");

        let attrs = resource_attributes("controlplane");
        let instance = attrs
            .iter()
            .find(|attr| attr.key.as_str() == "service.instance.id")
            .map(|attr| attr.value.to_string());
        assert_eq!(instance, Some("host-1".to_string()));
    }

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
}
