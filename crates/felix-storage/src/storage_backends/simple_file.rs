use crate::StorageApi;

use anyhow::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

// Serialization
use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToBytes};
use rkyv::{Archive, Deserialize, Serialize};

trait List : Sized {
    fn next() -> Option<Self>;
}

/// Constants
const DATA_FILE_IDENTIFIER: u64 = 0x4441544146494C45; // "DATAFILE" in ASCII
const INDEX_FILE_IDENTIFIER: u64 = 0x49445846494C45; // "IDXFILE" in ASCII

struct Files {
    data_file: File,
    index_file: File,
}

pub struct SimpleFileStorage {
    path_data_file: PathBuf,
    path_index_file: PathBuf,
    files: RwLock<Files>,
    index_free_list: Vec<FreeListEntry>,
    data_free_list: Vec<FreeListEntry>,
}

// Using 16 bits because you never know.

#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(u16)]
enum IndexVersion {
    V0,
}

/// This will be the header of the index file. It will contain all information about the file.
#[repr(C)]
#[derive(Archive, Serialize, Deserialize, Debug)]
struct IndexHeader {
    /// Identifies this file as an IndexFile.
    identifier: u64,

    /// Indicates the version of the index file.
    version: IndexVersion,

    /// The starting offset of the free list.
    free_list_start_offset: u64,

    /// The number of free entries.
    free_list_count: u64,

    /// The starting offset of the used list.
    used_list_start_offset: u64,

    /// The number of used entries
    used_list_count: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
struct IndexEntry {
    /// The offset in the data file.
    data_offset: u64,
    /// The next index entry
    next_offset: u64,
}

// Using 16 bits because you never know.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(u16)]
enum DataVersion {
    V0,
}

/// This is the header of the data file. It is 512 bytes long and contains metadata about the files
/// content.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(C)]
struct DataHeader {
    /// Identifies this file as a DataFile.
    identifier: u64,

    /// Indicates the version of the data file.
    version: DataVersion,

    free_list_count: u64,

    /// Indicates the offset to the first free list is.
    free_list_start_offset: u64,
}

#[repr(C)]
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct FreeListEntry {
    size: u64,
    next_free_list_offset: u64,
}

impl SerializeToBytes for DataHeader {}
impl SerializeToBytes for IndexEntry {}
impl SerializeToBytes for IndexHeader {}
impl SerializeToBytes for FreeListEntry {}
impl DeserializeFromBytes for DataHeader {}
impl DeserializeFromBytes for IndexEntry {}
impl DeserializeFromBytes for IndexHeader {}
impl DeserializeFromBytes for FreeListEntry {}

/// This is a very simple file storage meant for testing purposes only. There are
/// two files: index and data.
///
/// Index holds the metadata about each data entry. It is composed of:
///     * tenant_id
///     * namespace
///     * cache
///     * length
///     * file_position
///     * used
///
/// The first three, tenant_id, namespace, and cache, are all user provided values. The length
/// and file_position are computed by us when we write the user data to disk.The used variable
/// indicates if this entry is used or not.
///
/// Data holds the raw data that the user has given to us. As this is variable in length we just
/// append the data to the file.
///
/// TODO : Implement compaction.
impl SimpleFileStorage {
    pub fn new(path: PathBuf) -> Result<SimpleFileStorage> {
        let path_data_file = path.join("index");
        let path_index_file = path.join("index");

        // Create the files if they do not exist, otherwise we just use them as is.
        let data_file = File::create(path_data_file.clone().to_str().unwrap().to_string())?;
        let index_file = File::create(path_index_file.clone().to_str().unwrap().to_string())?;

        // If we create the files we will need to "format" them to our understanding.
        Self::ensure_index_file_format(&index_file)?;
        Self::ensure_data_file_format(&data_file)?;

        Ok(Self {
            path_data_file,
            path_index_file,
            index_free_list: vec![],
            data_free_list: vec![],
            files: RwLock::new(Files {
                data_file,
                index_file,
            }),
        })
    }

    fn read_from_file<T>(file: &mut File, offset: u64, size: usize) -> Result<T>
    where
        T: DeserializeFromBytes,
    {
        let mut data = vec![0u8; size];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut data)?;

        T::from_bytes(&data)
    }

    fn write_to_file<T>(file: &File, entry: &T, offset: u64) -> Result<()>
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

    fn ensure_data_file_format(file: &File) -> Result<Vec<FreeListEntry>> {
        // If the file is empty, we need to create it.
        if let Ok(metadata) = file.metadata()
            && metadata.len() == 0
        {
            let header = DataHeader {
                identifier: DATA_FILE_IDENTIFIER, // "DATAFILE"
                version: DataVersion::V0,
                free_list_count: 0,
                free_list_start_offset: 0,
            };

            Self::write_to_file(file, &header, 0)?;

            return Ok(vec![]);
        }

        // Read the first u64 to ensure it has the correct value
        let mut identifier_bytes = [0u8; 8];
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut identifier_bytes)?;
        let identifier = u64::from_le_bytes(identifier_bytes);
        if identifier != DATA_FILE_IDENTIFIER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data file has invalid identifier",
            ))?;
        }

        // We need to iterate through the free list and load it into memory.
        let header: DataHeader = Self::read_from_file(&mut f, 0, size_of::<DataHeader>())?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let entry: FreeListEntry =
                Self::read_from_file(&mut f, current_offset, size_of::<DataHeader>())?;
            free_list.push(entry.clone());
            current_offset = entry.next_free_list_offset;
        }

        Ok(free_list)
    }

    /// This function ensures that the index file has the correct header. If the file is empty
    /// we initialize it and return an empty list of free entries. Otherwise we will load the
    /// header and load the free list into memory.
    fn ensure_index_file_format(file: &File) -> Result<Vec<FreeListEntry>> {
        // If the file is empty, we need to create it.
        if let Ok(metadata) = file.metadata()
            && metadata.len() == 0
        {
            let header = IndexHeader {
                identifier: INDEX_FILE_IDENTIFIER, // "DATAFILE"
                version: IndexVersion::V0,
                free_list_start_offset: 0,
                free_list_count: 0,
                used_list_start_offset: 0,
                used_list_count: 0,
            };

            let mut f = file.try_clone()?;
            f.seek(SeekFrom::Start(0))?;
            let data = header.to_bytes()?;
            f.write_all(&data)?;
            f.flush()?;

            return Ok(vec![]);
        }

        // Read the first u64 to ensure it has the correct value
        let mut identifier_bytes = [0u8; 8];
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut identifier_bytes)?;
        let identifier = u64::from_le_bytes(identifier_bytes);
        if identifier != INDEX_FILE_IDENTIFIER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Index file has invalid identifier",
            ))?;
        }

        // We need to iterate through the free list and load it into memory.
        let header: IndexHeader = Self::read_from_file(&mut f, 0, size_of::<IndexHeader>())?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let entry: FreeListEntry =
                Self::read_from_file(&mut f, current_offset, size_of::<FreeListEntry>())?;
            free_list.push(entry.clone());
            current_offset = entry.next_free_list_offset;
        }

        // Now we need to iterate through the used list and load it into memory.
        let mut used_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.used_list_count {
            let entry: IndexEntry =
                Self::read_from_file(&mut f, current_offset, size_of::<IndexEntry>())?;
            used_list.push(entry.clone());
            current_offset = entry.next_offset;
        }

        Ok(free_list)
    }

    /// Finds a free list entry that is large enough to hold the requested size. If one does not
    /// exist we create a new one at the end of the file.
    async fn get_new_index_entry(&self) {
        todo!()
    }
}

impl Debug for SimpleFileStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SimpleFileStorage")
    }
}

#[async_trait]
impl StorageApi for SimpleFileStorage {
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) {
        todo!()
    }

    async fn get(&self, tenant_id: &str, namespace: &str, cache: &str, key: &str) -> Option<Bytes> {
        todo!()
    }

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Option<Bytes> {
        todo!()
    }

    async fn len(&self) -> usize {
        todo!()
    }

    async fn is_empty(&self) -> bool {
        todo!()
    }
}

unsafe impl Send for SimpleFileStorage {}

impl From<SimpleFileStorage> for Box<dyn StorageApi + Send> {
    fn from(value: SimpleFileStorage) -> Self {
        Box::new(value)
    }
}
