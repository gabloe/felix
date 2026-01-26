use crate::StorageApi;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

// Serialization

use super::data_file::*;
use super::index_file::*;

trait List: Sized {
    fn next() -> Option<Self>;
}

/// The files required to make the back-end work.
struct Files {
    data_file: DataFile,
    index_file: IndexFile,
}

pub struct SimpleFileStorage {
    files: RwLock<Files>,
    path_data_file: PathBuf,
    path_index_file: PathBuf,
}

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
    pub async fn new(path: PathBuf) -> Result<SimpleFileStorage> {
        let path_data_file = path.join("index");
        let path_index_file = path.join("index");

        // Create the files if they do not exist, otherwise we just use them as is.
        let data_file = DataFile::open(path_data_file.clone()).await?;
        let index_file = IndexFile::open(path_index_file.clone()).await?;

        Ok(Self {
            path_data_file,
            path_index_file,
            files: RwLock::new(Files {
                data_file,
                index_file,
            }),
        })
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
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _key: &str,
        _value: Bytes,
        _ttl: Option<Duration>,
    ) {
        todo!()
    }

    async fn get(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _key: &str,
    ) -> Option<Bytes> {
        todo!()
    }

    async fn delete(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _key: &str,
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
