mod data_file;
mod index_file;

use futures::AsyncSeekExt;
use futures::io::BufWriter;
use log::debug;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio_util::compat::TokioAsyncReadCompatExt;

mod simple_file;

use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToStream};
pub use simple_file::SimpleFileStorage;

async fn read_from_file<T>(file: &File, offset: u64) -> anyhow::Result<T>
where
    T: DeserializeFromBytes,
{
    let file = file.try_clone().await?;
    let mapped_file = unsafe { memmap2::Mmap::map(&file) }?;
    let offset = &mapped_file[offset as usize..];
    T::read(offset).await
}

async fn write_to_file<T>(file: &File, entry: &T, offset: u64) -> anyhow::Result<u64>
where
    T: SerializeToStream,
{
    let file = file.try_clone().await?;
    #[cfg(debug_assertions)]
    let mut writer = BufWriter::new(file.compat());
    writer.seek(SeekFrom::Start(offset)).await?;
    let bytes = entry.write(&mut writer).await? as u64;

    debug!("Wrote {bytes} bytes");

    Ok(bytes)
}
