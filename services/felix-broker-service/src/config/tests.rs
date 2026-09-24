//! Broker configuration: the environment, the YAML file, and the checks that
//! span both.

mod credential;
mod cross_field;
mod from_env;
mod listeners;
mod membership;
mod peer_transport;
mod printing;
mod yaml_file;
mod yaml_overrides;

use super::file::BrokerConfigOverride;
use super::*;
use felix_broker::SubQueuePolicy;
use serial_test::serial;
use std::env;
use std::fs;
use tempfile::TempDir;

// Helper to clear all Felix env vars
fn clear_felix_env() {
    for (key, _) in env::vars() {
        if key.starts_with("FELIX_") {
            unsafe {
                env::remove_var(key);
            }
        }
    }
}
