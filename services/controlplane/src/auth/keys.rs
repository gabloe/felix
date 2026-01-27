//! Signing key generation helpers.
//!
//! # Purpose
//! Generates RSA key material and key IDs for tenant JWT signing.
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use anyhow::Result;
use jsonwebtoken::Algorithm;
use rand::RngCore;
use rsa::RsaPrivateKey;
use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey};

pub fn generate_signing_keys() -> Result<TenantSigningKeys> {
    let private_key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048)?;
    let public_key = private_key.to_public_key();
    let private_pem = private_key.to_pkcs1_pem(Default::default())?;
    let public_pem = public_key.to_pkcs1_pem(Default::default())?;

    let mut kid_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut kid_bytes);
    let kid = hex::encode(kid_bytes);

    Ok(TenantSigningKeys {
        current: SigningKey {
            kid,
            alg: Algorithm::RS256,
            private_key_pem: private_pem.as_bytes().to_vec(),
            public_key_pem: public_pem.as_bytes().to_vec(),
        },
        previous: Vec::new(),
    })
}
