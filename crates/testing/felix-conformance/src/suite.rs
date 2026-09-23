//! The protocol suite: an in-process broker, checked twice — once with frames
//! built by hand over raw QUIC, once through `felix-client`.

mod checks;
mod fixture;
mod frames;
mod raw;
mod sdk;

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_broker::{Broker, CacheMetadata};
use felix_broker_service::serving::quic;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};

use fixture::{build_auth_fixture, build_quinn_client_config, build_server_config};
use raw::{run_cache, run_pubsub};
use sdk::{run_client_cache, run_client_pubsub};

pub(crate) const MAX_TEST_FRAME_BYTES: usize = 64 * 1024;

pub(crate) async fn run_protocol_suite() -> Result<()> {
    println!("== Felix Conformance Runner ==");
    let auth = build_auth_fixture()?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", CacheMetadata::default())
        .await?;
    broker
        .register_stream("t1", "default", "conformance", Default::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.broker_auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    run_pubsub(&connection, &auth).await?;
    run_cache(&connection, &auth).await?;
    run_client_pubsub(addr, cert.clone(), &auth).await?;
    run_client_cache(addr, cert, &auth).await?;

    drop(connection);
    server_task.abort();
    println!("Conformance checks passed.");
    Ok(())
}

#[cfg(test)]
mod tests;
