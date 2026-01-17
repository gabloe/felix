// Console demo that exercises publish/subscribe and cache behavior.
use bytes::Bytes;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() {
    // Keep the demo output readable and step-by-step.
    println!("== Felix Demo ==");
    println!("Goal: show in-process pub/sub and cache behavior.");
    println!("Step 1/4: starting in-process broker.");

    let broker = Arc::new(Broker::new(EphemeralCache::new()));
    // Spawn a listener so we can observe broadcasts immediately.
    let listener = start_listener(Arc::clone(&broker), "demo-topic");

    println!("Step 2/4: subscribing to topic 'demo-topic'.");
    println!("Step 3/4: publishing two messages.");
    broker
        .publish("demo-topic", Bytes::from_static(b"hello"))
        .await
        .expect("publish");
    broker
        .publish("demo-topic", Bytes::from_static(b"world"))
        .await
        .expect("publish");

    println!("Step 4/4: writing to cache with 1s TTL, then reading before/after expiry.");
    // Show that the cache honors a TTL with lazy expiry.
    broker
        .cache()
        .put(
            "demo-key",
            Bytes::from_static(b"cached"),
            Some(Duration::from_secs(1)),
        )
        .await;

    let cached = broker.cache().get("demo-key").await;
    println!(
        "Cache read (immediate): {:?}",
        cached.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let expired = broker.cache().get("demo-key").await;
    println!(
        "Cache read (after TTL): {:?}",
        expired.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    listener.abort();
    println!("Demo complete.");
}

fn start_listener(broker: Arc<Broker>, topic: &str) -> JoinHandle<()> {
    // Subscribe on a separate task so publish can proceed.
    let topic = topic.to_string();
    tokio::spawn(async move {
        let mut receiver = broker.subscribe(&topic).await.expect("subscribe");
        while let Ok(msg) = receiver.recv().await {
            println!("Subscriber received: {}", String::from_utf8_lossy(&msg));
        }
    })
}
