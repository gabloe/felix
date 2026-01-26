use futures::AsyncWriteExt;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::fmt::Debug;

use anyhow::Result;
use futures::io::BufWriter;
use futures::{AsyncWrite, SinkExt};
use log::debug;
use rkyv_codec::{RkyvWriter, VarintLength, archive_stream};

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
    async fn write<'b, T>(&self, writer: & 'b  mut BufWriter<T>) -> Result<&'b mut T>
    where
        T: AsyncWrite + Unpin,
    {
        let mut codec = RkyvWriter::<&mut BufWriter<T>, VarintLength>::new(writer);
        codec.send(self).await?;
        writer.flush().await?;
        Ok(writer.get_mut())
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
    async fn read(writer: &mut BufWriter<impl AsyncWrite + Unpin>) -> Result<Self> {
        let codec = RkyvWriter::<&mut BufWriter<_>, VarintLength>::new(writer);
        let mut reader = codec.inner().buffer();
        debug!("Buffer length: {}", reader.len());
        let mut buffer = AlignedVec::new();
        let result = archive_stream::<_, Self, VarintLength>(&mut reader, &mut buffer).await?;
        Ok(rkyv::deserialize::<Self, Error>(result)?)
    }
}
