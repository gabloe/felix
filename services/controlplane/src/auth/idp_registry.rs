//! Identity provider issuer configuration models.
//!
//! # Purpose
//! Defines IdP issuer settings and claim mapping configuration used by OIDC
//! validation and bootstrap/admin endpoints.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClaimMappings {
    pub subject_claim: String,
    pub groups_claim: Option<String>,
}

impl Default for ClaimMappings {
    fn default() -> Self {
        Self {
            subject_claim: "sub".to_string(),
            groups_claim: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdpIssuerConfig {
    pub issuer: String,
    pub audiences: Vec<String>,
    pub discovery_url: Option<String>,
    pub jwks_url: Option<String>,
    pub claim_mappings: ClaimMappings,
}
