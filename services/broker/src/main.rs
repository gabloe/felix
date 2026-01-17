// Broker service main entry point.
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Configure logging from environment for easy local tweaking.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Start an in-process broker with an ephemeral cache backend.
    let broker = Broker::new(EphemeralCache::new());
    tracing::info!("broker started");

    // Block until SIGINT so the process stays alive.
    let _ = tokio::signal::ctrl_c().await;
    drop(broker);
    tracing::info!("broker stopped");
}
