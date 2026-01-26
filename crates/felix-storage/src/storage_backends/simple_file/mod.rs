mod data_file;
mod index_file;

use futures::io::BufWriter;
use log::debug;
use std::io::SeekFrom;
use tokio::fs::File;
use futures::AsyncSeekExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

mod simple_file;

use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToStream};
pub use simple_file::SimpleFileStorage;

async fn read_from_file<T>(file: &File, offset: u64) -> anyhow::Result<T>
where
    T: DeserializeFromBytes,
{
    let file = file.try_clone().await?;
    let mut writer = BufWriter::new(file.compat());
    writer.seek(SeekFrom::Start(offset)).await?;
    T::read(&mut writer).await
}

async fn write_to_file<T>(file: &File, entry: &T, offset: u64) -> anyhow::Result<()>
where
    T: SerializeToStream,
{
    let file = file.try_clone().await?;
    let mut writer = BufWriter::new(file.compat());
    writer.seek(SeekFrom::Start(offset)).await?;
    let compat = entry.write(&mut writer).await?;

    let pos = compat.stream_position().await?;
    debug!("Wrote {} bytes", pos - offset);

    Ok(())
}
