use std::collections::HashMap;
use crate::{EphemeralCache, StorageApi};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

struct Files {
    data_file: File,
    index_file: File,
}

pub struct SimpleFileStorage {
    path_data_file: PathBuf,
    path_index_file: PathBuf,
    files: RwLock<Files>,
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
    pub fn new(path: PathBuf) -> Result<SimpleFileStorage, std::io::Error> {
        let path_data_file = path.join("index");
        let path_index_file = path.join("index");

        let data_file = File::create(path_data_file.clone().to_str().unwrap().to_string())?;
        let index_file = File::create(path_index_file.clone().to_str().unwrap().to_string())?;

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
