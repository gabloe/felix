mod data_file;
mod index_file;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

mod simple_file;

use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToBytes};
pub use simple_file::SimpleFileStorage;

fn read_from_file<T>(file: &mut File, offset: u64) -> anyhow::Result<T>
where
    T: DeserializeFromBytes,
{
    let size = size_of::<T>();
    let mut data = vec![0u8; size];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut data)?;

    T::from_bytes(&data)
}

fn write_to_file<T>(file: &File, entry: &T, offset: u64) -> anyhow::Result<()>
where
    T: SerializeToBytes,
{
    let mut file = file.clone();
    file.seek(SeekFrom::Start(offset))?;
    let data = entry.to_bytes()?;
    file.write_all(&data)?;
    file.flush()?;
    Ok(())
}
