// Lightweight crypto metadata types (no actual crypto operations yet).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyId(pub String);

/// Metadata describing an encryption profile.
///
/// ```
/// use felix_crypto::EncryptionProfile;
///
/// let profile = EncryptionProfile::new("key-1", "AES-256-GCM");
/// assert_eq!(profile.algorithm, "AES-256-GCM");
/// ```
#[derive(Debug, Clone)]
pub struct EncryptionProfile {
    pub key_id: KeyId,
    pub algorithm: String,
}

impl EncryptionProfile {
    // Simple constructor to keep call sites tidy.
    pub fn new(key_id: impl Into<String>, algorithm: impl Into<String>) -> Self {
        Self {
            key_id: KeyId(key_id.into()),
            algorithm: algorithm.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_builds() {
        // Sanity-check the field wiring.
        let profile = EncryptionProfile::new("key-1", "AES-256-GCM");
        assert_eq!(profile.key_id.0, "key-1");
        assert_eq!(profile.algorithm, "AES-256-GCM");
    }

    #[test]
    fn key_id_equality() {
        let a = KeyId("k1".to_string());
        let b = KeyId("k1".to_string());
        assert_eq!(a, b);
    }
}
