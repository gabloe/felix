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
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(test)]
static FORCE_TRACER_FAILURE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_observability(service_name: &str) -> PrometheusHandle {
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let provider = build_tracer_provider(service_name);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);
    if let Some(provider) = provider {
        let tracer = provider.tracer(service_name.to_string());
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        init_subscriber(registry.with(otel_layer));
    } else {
        init_subscriber(registry);
    }

    install_metrics_recorder()
}

fn build_tracer_provider(service_name: &str) -> Option<opentelemetry_sdk::trace::TracerProvider> {
    #[cfg(test)]
    if FORCE_TRACER_FAILURE.load(Ordering::Relaxed) {
        return None;
    }
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

fn install_metrics_recorder() -> PrometheusHandle {
    #[cfg(test)]
    {
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
pub(crate) fn set_force_tracer_failure(enabled: bool) {
    FORCE_TRACER_FAILURE.store(enabled, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
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
    fn init_observability_handles_provider_variants() {
        set_force_tracer_failure(true);
        let _handle = init_observability("svc-test");
        set_force_tracer_failure(false);
        let _handle = init_observability("svc-test");
    }

    #[tokio::test]
    #[serial]
    async fn serve_metrics_exposes_routes() -> Result<(), Box<dyn std::error::Error>> {
        let handle = super::install_metrics_recorder();
        let addr = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await?
            .local_addr()?;
        let server = tokio::spawn(async move {
            let _ = serve_metrics(handle, addr).await;
        });

        let base = format!("http://{}", addr);
        let metrics = reqwest::get(format!("{}/metrics", base))
            .await?
            .text()
            .await?;
        assert!(metrics.contains("# HELP") || metrics.is_empty());
        let live = reqwest::get(format!("{}/live", base)).await?.text().await?;
        assert_eq!(live, "ok");
        let ready = reqwest::get(format!("{}/ready", base))
            .await?
            .text()
            .await?;
        assert_eq!(ready, "ok");

        server.abort();
        Ok(())
    }
}
