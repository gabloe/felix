// serde `with` adapters that base64-encode binary fields in JSON messages.
// Referenced by absolute path from `#[serde(with = "...")]` attributes in `message.rs`.

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) mod base64_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Vec<u8> as base64 string for JSON payloads.
    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }

    // Decode base64 string into Vec<u8>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(D::Error::custom)
    }
}

pub(crate) mod base64_bytes_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Bytes as base64 string for JSON payloads.
    pub fn serialize<S>(value: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        serializer.serialize_str(&encoded)
    }

    // Decode base64 string into Bytes.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(D::Error::custom)?;
        Ok(Bytes::from(decoded))
    }
}

pub(crate) mod base64_option_bytes {
    use super::*;
    use serde::de::Error;

    // Encode Option<Bytes> as nullable base64 string.
    pub fn serialize<S>(
        value: &Option<Bytes>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(bytes) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                serializer.serialize_some(&encoded)
            }
            None => serializer.serialize_none(),
        }
    }

    // Decode optional base64 string into Option<Bytes>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<Bytes>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = Option::<String>::deserialize(deserializer)?;
        match encoded {
            Some(value) => base64::engine::general_purpose::STANDARD
                .decode(value.as_bytes())
                .map(|decoded| Some(Bytes::from(decoded)))
                .map_err(D::Error::custom),
            None => Ok(None),
        }
    }
}

pub(crate) mod base64_vec {
    use super::*;
    use serde::de::Error;

    // Encode Vec<Vec<u8>> as base64 array.
    pub fn serialize<S>(values: &[Vec<u8>], serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded: Vec<String> = values
            .iter()
            .map(|value| base64::engine::general_purpose::STANDARD.encode(value))
            .collect();
        encoded.serialize(serializer)
    }

    // Decode base64 array into Vec<Vec<u8>>.
    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Vec<Vec<u8>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = Vec::<String>::deserialize(deserializer)?;
        encoded
            .into_iter()
            .map(|value| {
                base64::engine::general_purpose::STANDARD
                    .decode(value.as_bytes())
                    .map_err(D::Error::custom)
            })
            .collect()
    }
}
