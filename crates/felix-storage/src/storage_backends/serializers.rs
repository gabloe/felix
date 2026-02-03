use futures::AsyncWriteExt;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, from_bytes, to_bytes};
use std::fmt::Debug;

use anyhow::Result;
use futures::AsyncWrite;
use futures::io::BufWriter;

#[macro_export]
macro_rules! derive_serialization {
    ($($name:ident),+) => {
        // Repeat for each provided type name
        $(
            // Generate the empty implementation block
            impl SerializeToStream for $name {}
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
            impl SerializeToStream for $name {}
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
            impl DeserializeFromBytes for $name {}
        )+
    };
}

pub(crate) trait SerializeToStream:
    Debug + Sized + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>
{
    async fn write<T>(&self, writer: &mut BufWriter<T>) -> Result<usize>
    where
        T: AsyncWrite + Unpin,
    {
        let b = to_bytes::<Error>(self)?;
        writer.write_all(&b).await?;
        writer.flush().await?;
        Ok(b.len())
    }
}

pub(crate) trait DeserializeFromBytes:
    Debug
    + Sized
    + Archive<
        Archived: for<'a> CheckBytes<HighValidator<'a, Error>>
                      + Deserialize<Self, Strategy<Pool, Error>>,
    >
{
    async fn read(bytes: &[u8]) -> Result<Self> {
        Ok(from_bytes::<Self, Error>(bytes)?)
    }
}
