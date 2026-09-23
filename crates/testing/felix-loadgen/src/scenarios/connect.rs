//! Connecting to the cluster under test.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use felix_client::{Client, ClusterClient};

use super::Common;

pub(super) async fn cluster(common: &Common) -> Result<ClusterClient> {
    let config = crate::tls::client_config(&common.tenant, &common.token)?;
    ClusterClient::connect(&common.brokers, "localhost", config)
        .await
        .context("connect a cluster client")
}

pub(super) async fn client(common: &Common, addr: SocketAddr) -> Result<Client> {
    let config = crate::tls::client_config(&common.tenant, &common.token)?;
    Client::connect(addr, "localhost", config)
        .await
        .with_context(|| format!("connect to {addr}"))
}
