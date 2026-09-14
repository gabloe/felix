//! The metadata state machine: the in-memory store, driven by commands.
//!
//! This is the application half of the Raft seam
//! ([`crate::raft::AppStateMachine`]), exactly as the design promised: no
//! new state container, just [`InMemoryStore`] — already implementing every
//! store trait, already contract-tested against Postgres — fed committed
//! [`MetaCommand`]s in log order. Its snapshot is the store's deterministic
//! export, so two replicas that applied the same log serialize
//! byte-identical state; the harness in `state_machine_tests.rs` holds it
//! to that.
//!
//! Nothing here consults a clock or generates a value: heartbeat and expiry
//! commands carry their timestamps, bootstrap carries its candidate keys.
//! If a command cannot be decoded — a newer envelope version, an unknown
//! operation — the answer is an [`MetaError::Unsupported`] *response*,
//! identical on every replica; silently skipping a committed command would
//! fork this replica's state from the group's.
use std::sync::Arc;

use crate::raft::AppStateMachine;
use crate::store::command::{MetaCommand, MetaResponse, MetaResult, decode_command, encode_result};
use crate::store::memory::InMemoryStore;
use crate::store::{AuthStore, ControlPlaneStore};

pub struct MetadataStateMachine {
    store: Arc<InMemoryStore>,
}

impl MetadataStateMachine {
    pub fn new(store: Arc<InMemoryStore>) -> Self {
        Self { store }
    }

    /// The applied state, for serving reads. Reads need no consensus hop —
    /// the watch contract is pull-based and eventually consistent — which is
    /// the whole reason the state machine is the serving store itself.
    pub fn store(&self) -> &Arc<InMemoryStore> {
        &self.store
    }

    /// Apply one already-decoded command. Public for the determinism
    /// harness; the Raft path arrives through [`AppStateMachine::apply`].
    pub async fn dispatch(&self, command: MetaCommand) -> MetaResult {
        let store = self.store.as_ref();
        match command {
            MetaCommand::CreateTenant { tenant } => store
                .create_tenant(tenant)
                .await
                .map(|tenant| MetaResponse::Tenant { tenant })
                .map_err(Into::into),
            MetaCommand::DeleteTenant { tenant_id } => store
                .delete_tenant(&tenant_id)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateNamespace { namespace } => store
                .create_namespace(namespace)
                .await
                .map(|namespace| MetaResponse::Namespace { namespace })
                .map_err(Into::into),
            MetaCommand::DeleteNamespace { key } => store
                .delete_namespace(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateStream { stream } => store
                .create_stream(stream)
                .await
                .map(|stream| MetaResponse::Stream { stream })
                .map_err(Into::into),
            MetaCommand::PatchStream { key, patch } => store
                .patch_stream(&key, patch)
                .await
                .map(|stream| MetaResponse::Stream { stream })
                .map_err(Into::into),
            MetaCommand::DeleteStream { key } => store
                .delete_stream(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::CreateCache { cache } => store
                .create_cache(cache)
                .await
                .map(|cache| MetaResponse::Cache { cache })
                .map_err(Into::into),
            MetaCommand::PatchCache { key, patch } => store
                .patch_cache(&key, patch)
                .await
                .map(|cache| MetaResponse::Cache { cache })
                .map_err(Into::into),
            MetaCommand::DeleteCache { key } => store
                .delete_cache(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::RegisterNode { node } => store
                .register_node(node)
                .await
                .map(|node| MetaResponse::Node { node })
                .map_err(Into::into),
            MetaCommand::PatchNode { node_id, patch } => store
                .patch_node(&node_id, patch)
                .await
                .map(|node| MetaResponse::Node { node })
                .map_err(Into::into),
            MetaCommand::DeleteNode { node_id } => store
                .delete_node(&node_id)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::RecordNodeHeartbeat {
                node_id,
                incarnation,
                at_millis,
            } => store
                .record_node_heartbeat(&node_id, incarnation, at_millis)
                .await
                .map(|node| MetaResponse::Node { node })
                .map_err(Into::into),
            MetaCommand::ExpireStaleNodes {
                expiry_before_millis,
            } => store
                .expire_stale_nodes(expiry_before_millis)
                .await
                .map(|nodes| MetaResponse::Nodes { nodes })
                .map_err(Into::into),
            MetaCommand::SetNodeLifecycle { node_id, lifecycle } => store
                .set_node_lifecycle(&node_id, lifecycle)
                .await
                .map(|node| MetaResponse::MaybeNode { node })
                .map_err(Into::into),
            MetaCommand::PutShardAssignment { assignment } => store
                .put_shard_assignment(assignment)
                .await
                .map(|assignment| MetaResponse::Assignment { assignment })
                .map_err(Into::into),
            MetaCommand::DeleteShardAssignment { key } => store
                .delete_shard_assignment(&key)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::UpsertIdpIssuer { tenant_id, issuer } => store
                .upsert_idp_issuer(&tenant_id, issuer)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::DeleteIdpIssuer { tenant_id, issuer } => store
                .delete_idp_issuer(&tenant_id, &issuer)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::AddRbacPolicy { tenant_id, policy } => store
                .add_rbac_policy(&tenant_id, policy)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::AddRbacGrouping {
                tenant_id,
                grouping,
            } => store
                .add_rbac_grouping(&tenant_id, grouping)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::SetTenantSigningKeys { tenant_id, keys } => store
                .set_tenant_signing_keys(&tenant_id, keys)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::SetTenantAuthBootstrapped {
                tenant_id,
                bootstrapped,
            } => store
                .set_tenant_auth_bootstrapped(&tenant_id, bootstrapped)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::SeedRbac {
                tenant_id,
                policies,
                groupings,
            } => store
                .seed_rbac_policies_and_groupings(&tenant_id, policies, groupings)
                .await
                .map(|()| MetaResponse::Unit)
                .map_err(Into::into),
            MetaCommand::BootstrapTenantAuth { tenant_id, seed } => store
                .bootstrap_tenant_auth(&tenant_id, seed)
                .await
                .map(|keys| MetaResponse::SigningKeys { keys })
                .map_err(Into::into),
        }
    }
}

#[async_trait::async_trait]
impl AppStateMachine for MetadataStateMachine {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        let result: MetaResult = match decode_command(command) {
            Ok(command) => self.dispatch(command).await,
            Err(err) => Err(err),
        };
        encode_result(&result)
    }

    async fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(&self.store.export_state().await)
            .expect("exported state serializes by construction")
    }

    async fn restore(&self, snapshot: &[u8]) {
        let state = serde_json::from_slice(snapshot).expect("snapshot produced by export_state");
        self.store
            .import_state(state)
            .await
            .expect("snapshot version produced by this cluster");
    }
}

#[cfg(test)]
#[path = "state_machine_tests.rs"]
mod tests;
