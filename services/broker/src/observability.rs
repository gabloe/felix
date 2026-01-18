use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace as sdktrace;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_observability(service_name: &str) -> PrometheusHandle {
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let provider = build_tracer_provider(service_name);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);
    if let Some(provider) = provider {
        let tracer = provider.tracer(service_name.to_string());
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        registry.with(otel_layer).init();
    } else {
        registry.init();
    }

    PrometheusBuilder::new()
        .install_recorder()
        .expect("install metrics recorder")
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

pub async fn serve_metrics(handle: PrometheusHandle, addr: SocketAddr) -> std::io::Result<()> {
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move { handle.render() }),
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await
}
