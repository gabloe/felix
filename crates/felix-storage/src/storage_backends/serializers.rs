use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, from_bytes, to_bytes};

#[macro_export]
macro_rules! derive_serialization {
    ($($name:ident),+) => {
        // Repeat for each provided type name
        $(
            // Generate the empty implementation block
            impl SerializeToBytes for $name {}
            impl DeserializeFromBytes for $name {}
        )+
    };
}

#[macro_export]
macro_rules! derive_serialize_to_bytes {
    // The macro takes one or more type names as idents (identifiers)
    ($($name:ident),+) => {
        // Repeat for each provided type name
        $(
            // Generate the empty implementation block
            impl SerializeToBytes for $name {}
        )+
    };
}

#[macro_export]
macro_rules! derive_deserialize_from_bytes {
    // The macro takes one or more type names as idents (identifiers)
    ($($name:ident),+) => {
        // Repeat for each provided type name
        $(
            // Generate the empty implementation block
            impl SerializeToBytes for $name {}
        )+
    };
}

pub(crate) trait SerializeToBytes:
    Sized + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>>
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = to_bytes(self)?;
        Ok(bytes.to_vec())
    }
}

pub(crate) trait DeserializeFromBytes:
    Sized
    + Archive<
        Archived: for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>
                      + Deserialize<Self, Strategy<Pool, rkyv::rancor::Error>>,
    >
where
    Self::Archived: for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>
        + Deserialize<Self, Strategy<Pool, rkyv::rancor::Error>>,
{
    fn from_bytes(bytes: &Vec<u8>) -> anyhow::Result<Self> {
        let value = from_bytes::<Self, rkyv::rancor::Error>(bytes)?;
        Ok(value)
    }
}
