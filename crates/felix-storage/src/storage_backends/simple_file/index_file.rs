use crate::derive_serialization;
use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToStream};
use anyhow::{Error, Result};
use base64::prelude::*;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::io::BufReader;
use log::{debug, error, info};
use std::fmt::{Debug, Formatter};
use std::io::SeekFrom;
use std::ops::Deref;
use std::path::PathBuf;
use tokio::fs::File;
// Serialization
use rkyv::{Archive, Deserialize, Serialize, with::Skip};
use tokio_util::compat::TokioAsyncReadCompatExt;
const INDEX_FILE_IDENTIFIER: u64 = 0x49445846494C45; // "IDXFILE" in ASCII

// Using 16 bits because you never know.
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(u16)]
pub(crate) enum IndexVersion {
    V0,
}

/// The identifier size.
#[cfg(debug_assertions)]
const IDENTIFIER_SIZE: usize = 256;
#[cfg(not(debug_assertions))]
const IDENTIFIER_SIZE: usize = 1024;

type Identifier = [u8; IDENTIFIER_SIZE];

const ZERO: Identifier = [0; IDENTIFIER_SIZE];

struct IdentifierWrapper(Identifier);

impl From<Identifier> for IdentifierWrapper {
    fn from(bytes: Identifier) -> Self {
        Self(bytes)
    }
}

impl Deref for IdentifierWrapper {
    type Target = Identifier;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for IdentifierWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let slice = &self[0..16];
        writeln!(f, "{:?}", slice)
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(u8)]
pub(crate) enum IndexState {
    Free = 0,
    Used = 1,
}

/// This will be the header of the index file. It will contain all information about the file.
#[repr(C)]
#[derive(Archive, Serialize, Deserialize, Debug)]
pub(crate) struct IndexHeader {
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

    /// We are currently updating this entry.
    current_update_offset: u64,
}

/// This hold information about the entry in the index file as well as the header for the data
/// in the data file.
#[derive(Archive, Serialize, Deserialize, Clone)]
pub(crate) struct IndexEntry {
    /// The identifier used to look up the data.
    pub identifier: Identifier,

    /// The offset in the data file of this users data.
    pub data_offset: u64,

    /// The state of the data.
    pub state: IndexState,

    /// The previous entry in the list.
    pub previous_offset: u64,

    /// The current so we can find the data in the index file.
    #[rkyv(with = Skip)]
    pub current_offset: u64,

    /// The next index entry
    pub next_offset: u64,
}

impl PartialEq for IdentifierWrapper {
    fn eq(&self, other: &Self) -> bool {
        let a = self.0;
        let b = other.0;
        for (i, e) in a.iter().enumerate() {
            if *e != b[i] {
                debug!("left[{i}] != right[{i}] -> {e} != {b}", e = *e, b = b[i]);
                return false;
            }
        }

        debug!("IdentifierWrapper: Match");
        false
    }
}

impl Debug for IndexEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{{identifier={:?}, data_offset={:?}, state={:?}, previous_offset={:?}, current_offset={:?}, next_offset={:?}}}",
            IdentifierWrapper(self.identifier),
            self.data_offset,
            self.state,
            self.previous_offset,
            self.current_offset,
            self.next_offset
        )
    }
}

pub(crate) struct IndexFile {
    file_path: PathBuf,
    header: IndexHeader,
    file: File,
    free_list: Vec<IndexEntry>,
    used_list: Vec<IndexEntry>,
}

impl IndexFile {
    pub async fn open(file_path: PathBuf) -> Result<Self> {
        let parent = file_path.parent().unwrap();

        let exists = !std::fs::exists(parent)?;
        if !exists {
            info!("Creating parent index directory.");
            std::fs::create_dir_all(parent)?;
        }

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path.clone())
            .await?;

        // If the file is empty, we need to create it.
        let (header, used_list, free_list) = read_header(&file).await?;

        Ok(Self {
            file_path,
            header,
            file,
            free_list,
            used_list,
        })
    }

    pub async fn insert(&mut self, id: Identifier, data_offset: u64) -> Result<IndexEntry> {
        let _encoding = BASE64_STANDARD.encode(id);
        debug!(
            "Inserting new entry: {:?} with data offset {}",
            IdentifierWrapper(id),
            data_offset
        );

        // Create a new entry and put it on the free list.
        if self.free_list.is_empty() {
            self.new_free_entry().await?;
        }

        let before_free_count = self.header.free_list_count;
        let before_used_count = self.header.used_list_count;

        debug_assert_eq!(before_used_count as usize, self.used_list.len());
        debug_assert_eq!(before_free_count as usize, self.free_list.len());

        let result = self.new_used_entry(id, data_offset).await?;

        let after_free_count = self.header.free_list_count;
        let after_used_count = self.header.used_list_count;
        debug_assert_eq!(after_used_count as usize, self.used_list.len());
        debug_assert_eq!(after_free_count as usize, self.free_list.len());

        debug_assert_eq!(before_free_count, after_free_count + 1); // Free has decreased
        debug_assert_eq!(before_used_count, after_used_count - 1); // Used has increased.

        Ok(result)
    }

    pub fn delete(&mut self, id: Identifier) -> Result<bool> {
        let id = IdentifierWrapper(id);

        for (idx, entry) in self.used_list.iter().enumerate() {
            if IdentifierWrapper(entry.identifier) == id {
                self.used_list.remove(idx);
                break;
            }
        }

        Ok(false)
    }

    pub fn get(&mut self, id: Identifier) -> Result<Option<IndexEntry>> {
        debug!("Getting entry with id={:?}", IdentifierWrapper(id));
        debug!(
            "Searching through {count} entries.",
            count = self.used_list.len()
        );
        debug!("Free List Count: {}", self.free_list.len());
        debug!("Used List Count: {}", self.used_list.len());

        let id = IdentifierWrapper(id);

        let found = self
            .used_list
            .iter()
            .filter_map(|x| {
                if IdentifierWrapper(x.identifier) == id {
                    Some(x)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if found.is_empty() {
            Ok(None)
        } else if found.len() != 1 {
            Err(Error::msg(
                "There are more than one matching element.".to_string(),
            ))
        } else {
            let t = found.first().unwrap();
            let t = (**t).clone();
            Ok(Some(t))
        }
    }

    async fn remove_used(&mut self, idx: usize) -> Result<()> {
        if let Some(found) = self.used_list.get_mut(idx) {
            found.state = IndexState::Free;
            super::write_to_file(&self.file, found, found.current_offset).await?;

            // We need to save this in-case of power loss. On resume, we can see that
            // we are updating this file which means that we are deleting it.
            self.header.current_update_offset = found.current_offset;
            super::write_to_file(&self.file, &self.header, 0).await?;

            // Now we update the before and after entries and save those to disk. Once
            // this is done we can modify to unused entry and make it look like its final
            // entry.
            let at_front = found.previous_offset != 0;
            if !at_front {
                let mut previous =
                    super::read_from_file::<IndexEntry>(&self.file, found.previous_offset).await?;
                previous.next_offset = found.next_offset;
                super::write_to_file(&self.file, &previous, previous.current_offset).await?;
            }

            let next_position = found.next_offset;
            if found.next_offset != 0 {
                let mut next =
                    super::read_from_file::<IndexEntry>(&self.file, found.previous_offset).await?;
                next.previous_offset = found.previous_offset;
                super::write_to_file(&self.file, &next, next.current_offset).await?;
            }

            // Finally we update ourselves to be as if we are in the front of the free list.
            found.previous_offset = 0;
            found.next_offset = self.header.free_list_start_offset;
            super::write_to_file(&self.file, found, found.current_offset).await?;

            // We finally update the header
            self.header.used_list_count -= 1;
            if at_front {
                self.header.used_list_count = next_position;
            }
            self.header.free_list_count += 1;
            self.header.free_list_start_offset = found.current_offset;
            self.header.current_update_offset = 0;
            super::write_to_file(&self.file, &self.header, 0).await?;

            Ok(())
        } else {
            Err(Error::msg("Index does not exist.".to_string()))
        }
    }

    /// Allocate a new block in the Index file. You must have the lock before
    /// calling this.
    /// @return Returns the offset in the file including the header for the entry.
    async fn new_free_entry(&mut self) -> Result<()> {
        info!("Creating new free entry");
        debug!("Current header: {:?}", self.header);
        let length = self.file.metadata().await?.len();
        let entry = IndexEntry {
            identifier: ZERO,
            state: IndexState::Free,
            data_offset: 0,
            previous_offset: 0,
            current_offset: length,
            next_offset: self.header.free_list_start_offset,
        };

        // Write the entry at the end.
        debug!("Writing new entry at offset {}", length);
        debug!("entry: {:?}", entry);
        super::write_to_file(&self.file, &entry, length).await?;

        self.header.free_list_start_offset = length;
        self.header.free_list_count += 1;

        debug!("Writing updated header: {:?}", self.header);
        super::write_to_file(&self.file, &self.header, 0).await?;

        self.free_list.push(entry);

        Ok(())
    }

    /// We must ALWAYS use the first entry from the free list.
    async fn new_used_entry(&mut self, id: Identifier, data_offset: u64) -> Result<IndexEntry> {
        // Grab the first free entry and push to the used list.
        let mut result: IndexEntry = self.free_list.pop().unwrap();
        result.identifier = id;
        debug!("Free entry: {:?}", result);

        result.data_offset = data_offset;
        result.next_offset = self.header.used_list_start_offset;

        // Update the header in memory.
        self.header.free_list_start_offset = result.next_offset;
        self.header.free_list_count -= 1;

        self.header.used_list_start_offset = result.current_offset;
        self.header.used_list_count += 1;

        debug!("Writing used entry: {:?}", self.header);
        super::write_to_file(&self.file, &result, result.current_offset).await?;

        debug!("Writing updated header: {:?}", self.header);
        super::write_to_file(&self.file, &self.header, 0).await?;

        // Activate the new entry and save to disk.
        result.state = IndexState::Used;
        debug!("Writing activated entry: {:?}", result);
        super::write_to_file(&self.file, &result, result.current_offset).await?;

        debug!("Inserting into used list: {:?}", result);
        let assert_before = self.used_list.len();
        self.used_list.push(result.clone());
        assert_eq!(assert_before + 1, self.used_list.len());

        Ok(result)
    }
}

async fn read_header(file: &File) -> Result<(IndexHeader, Vec<IndexEntry>, Vec<IndexEntry>)> {
    // If the file is empty, we need to create it.
    if let Ok(metadata) = file.metadata().await
        && metadata.len() == 0
    {
        debug!("Creating a new index file");
        // If the index file is empty we need to create
        // it with an empty used and free list.
        let header = IndexHeader {
            identifier: INDEX_FILE_IDENTIFIER, // "DATAFILE"
            version: IndexVersion::V0,
            free_list_start_offset: 0,
            free_list_count: 0,
            used_list_start_offset: 0,
            used_list_count: 0,
            current_update_offset: 0,
        };

        super::write_to_file(file, &header, 0).await?;

        Ok((header, vec![], vec![]))
    } else {
        debug!("Loading previous file.");
        // Read the first u64 to ensure it has the correct value

        let mut identifier_bytes = [0u8; 8];

        let f = file.try_clone().await?;
        let mut reader = BufReader::new(f.compat());
        reader.seek(SeekFrom::Start(1)).await?;
        reader.read_exact(&mut identifier_bytes[0..7]).await?;
        let identifier = u64::from_le_bytes(identifier_bytes);

        let bytes = INDEX_FILE_IDENTIFIER.to_le_bytes();
        debug_assert_eq!(8, bytes.len());

        for (i, v) in bytes.iter().enumerate() {
            debug!("file[{i}] = {f}, ID[{i}]={v}", f = identifier_bytes[i]);
        }

        if identifier != INDEX_FILE_IDENTIFIER {
            error!(
                "File identifier does not match: expected={:#x} found={:#x}",
                INDEX_FILE_IDENTIFIER, identifier
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Index file has invalid identifier",
            ))?;
        }

        // We need to iterate through the free list and load it into memory.
        let header: IndexHeader = super::read_from_file(file, 0).await?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let mut entry: IndexEntry = super::read_from_file(file, current_offset).await?;
            debug!("Adding free entry: {:?}", entry);
            free_list.push(entry.clone());
            entry.current_offset = current_offset;
            current_offset = entry.next_offset;
        }

        // Now we need to iterate through the used list and load it into memory.
        let mut used_list = vec![];
        let mut current_offset = header.used_list_start_offset;
        for _ in 0..header.used_list_count {
            debug!("Loading used entry from {}", current_offset);
            let entry: IndexEntry = super::read_from_file(file, current_offset).await?;
            debug!("Adding used entry: {:?}", entry);
            used_list.push(entry.clone());
            current_offset = entry.next_offset;
        }

        Ok((header, used_list, free_list))
    }
}

derive_serialization!(IndexEntry, IndexHeader);

#[cfg(test)]
mod test {
    use super::*;
    use env_logger::{Builder, Target};
    use futures::io::BufWriter;
    use log::info;
    use rkyv::from_bytes;
    use rkyv::rancor::Error;
    use std::io::Write;
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            Builder::new()
                .format(|buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
                .target(Target::Stdout)
                .filter(None, log::LevelFilter::Debug)
                .init();
        });
    }
    #[tokio::test]
    async fn create_index_file() {
        initialize();

        let test_output = concat!(env!("CARGO_MANIFEST_DIR"), "/test_output/create_index_file");
        if std::fs::exists(test_output).unwrap() {
            info!("Deleting existing file");
            std::fs::remove_file(test_output).unwrap();
        }

        info! {"Output: {test_output}"}
        let index = IndexFile::open(test_output.into()).await.unwrap();

        assert_eq!(0, index.used_list.len(), "used list len should be 0");
        assert_eq!(
            0, index.header.used_list_start_offset,
            "used list count should be 0"
        );
        assert_eq!(
            0, index.header.used_list_start_offset,
            "used list start offset should be 0"
        );

        assert_eq!(0, index.free_list.len(), "free list len should be 0.");
        assert_eq!(
            0, index.header.free_list_count,
            "free list count should be 0"
        );
        assert_eq!(
            0, index.header.free_list_start_offset,
            "free list starting offset should be 0"
        );

        assert_eq!(
            String::from(test_output),
            index.file_path,
            "output path should be the same."
        );

        let index = IndexFile::open(test_output.into()).await.unwrap();

        assert_eq!(0, index.used_list.len(), "used list len should be 0");
        assert_eq!(
            0, index.header.used_list_start_offset,
            "used list count should be 0"
        );
        assert_eq!(
            0, index.header.used_list_start_offset,
            "used list start offset should be 0"
        );

        assert_eq!(0, index.free_list.len(), "free list len should be 0.");
        assert_eq!(
            0, index.header.free_list_count,
            "free list count should be 0"
        );
        assert_eq!(
            0, index.header.free_list_start_offset,
            "free list starting offset should be 0"
        );

        assert_eq!(
            String::from(test_output),
            index.file_path,
            "output path should be the same."
        );
    }

    #[tokio::test]
    async fn validate_serialization() {
        initialize();
        let entry = IndexEntry {
            identifier: ZERO,
            data_offset: 50,
            previous_offset: 100,
            current_offset: 105,
            next_offset: 1000,
            state: IndexState::Used,
        };

        let bytes = vec![];
        let mut writer = BufWriter::new(bytes);

        entry.write(&mut writer).await.unwrap();
        let new_entry = from_bytes::<IndexEntry, Error>(writer.buffer()).unwrap();

        assert_eq!(entry.identifier, new_entry.identifier);
    }

    #[tokio::test]
    async fn validate_adding_new_entry() {
        initialize();

        let test_output = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_output/validate_adding_new_entry"
        );
        if std::fs::exists(test_output).unwrap() {
            std::fs::remove_file(test_output).unwrap();
        }

        let mut index = IndexFile::open(test_output.into()).await.unwrap();

        let id: Identifier = [1; IDENTIFIER_SIZE];
        let r = index.insert(id, 10).await.unwrap();
        validate_idempotent(&r, id);

        let mut index = IndexFile::open(test_output.into()).await.unwrap();
        let r = index.get(id).unwrap().unwrap();
        validate_idempotent(&r, id);

        fn validate_idempotent(entry: &IndexEntry, id: Identifier) {
            assert_eq!(entry.identifier, id);
            assert_eq!(entry.current_offset, 56);
            assert_eq!(entry.previous_offset, 0);
            assert_eq!(entry.next_offset, 0);
            assert_eq!(entry.data_offset, 10);
        }
    }
}
