//! Strongly typed ids, so a region id cannot be passed where a tenant id is
//! expected. Each is a UUID underneath.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
        pub struct $name(Uuid);

        impl $name {
            /// A new random id.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wrap an existing UUID, e.g. one decoded from storage.
            pub fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// The underlying UUID.
            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(input: &str) -> Result<Self> {
                // Preserve the original input for clearer error messages.
                let uuid = Uuid::parse_str(input).map_err(|_| Error::InvalidId(input.into()))?;
                Ok(Self(uuid))
            }
        }
    };
}

id_type!(RegionId);
id_type!(TenantId);
id_type!(NamespaceId);
id_type!(StreamId);
id_type!(TopicId);
id_type!(ShardId);

#[cfg(test)]
mod tests;
