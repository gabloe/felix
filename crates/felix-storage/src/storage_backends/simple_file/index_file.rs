use crate::derive_serialization;
use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToBytes};
use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize, with::Skip};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

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

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(u8)]
enum IndexState {
    Free = 0,
    Used = 1,
}

/// This will be the header of the index file. It will contain all information about the file.
#[repr(C)]
#[derive(Archive, Serialize, Deserialize, Debug)]
pub(crate) struct IndexHeader {
    /// Identifies this file as an IndexFile.
    pub identifier: u64,

    /// Indicates the version of the index file.
    pub version: IndexVersion,

    /// The starting offset of the free list.
    pub free_list_start_offset: u64,

    /// The number of free entries.
    pub free_list_count: u64,

    /// The starting offset of the used list.
    pub used_list_start_offset: u64,

    /// The number of used entries
    pub used_list_count: u64,
}

/// This hold information about the entry in the index file as well as the header for the data
/// in the data file.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct IndexEntry {
    /// The identifier used to look up the data.
    pub identifier: Identifier,

    /// The offset in the data file of this users data.
    pub data_offset: u64,

    /// The state of the data.
    pub state: IndexState,

    #[rkyv(with = Skip)]
    pub current_offset: u64,

    /// The next index entry
    pub next_offset: u64,
}

pub(crate) struct IndexFile {
    file_path: PathBuf,
    header: IndexHeader,
    file: File,
    free_list: Vec<IndexEntry>,
    used_list: Vec<IndexEntry>,
}

impl IndexFile {
    pub fn open(file_path: PathBuf) -> Result<Self> {
        let file = File::create(file_path.clone())?;

        // If the file is empty, we need to create it.
        let (header, used_list, free_list) = read_header(&file)?;

        Ok(Self {
            file_path,
            header,
            file,
            free_list,
            used_list,
        })
    }

    fn create_entry(&mut self, id: Identifier, data_offset: u64) -> Result<IndexEntry> {
        // Create a new entry and put it on the free list.
        if self.free_list.is_empty() {
            self.new_free_entry(id)?;
        }

        let result = self.new_used_entry(data_offset)?;
        Ok(result)
    }

    fn remove_entry(&mut self, entry: &mut IndexEntry) -> Result<()> {
        entry.state = IndexState::Free;

        Ok(())
    }

    /// Allocate a new block in the Index file. You must have the lock before
    /// calling this.
    /// @return Returns the offset in the file including the header for the entry.
    fn new_free_entry(&mut self, identifier: Identifier) -> Result<()> {
        let length = self.file.metadata()?.len();
        let entry = IndexEntry {
            identifier,
            state: IndexState::Free,
            data_offset: 0,
            current_offset: length,
            next_offset: self.header.free_list_start_offset,
        };

        // Write the entry at the end.
        super::write_to_file(&self.file, &entry, length)?;

        self.header.free_list_start_offset = length;
        self.header.free_list_count += 1;

        super::write_to_file(&self.file, &self.header, 0)?;

        self.free_list.push(entry);

        Ok(())
    }

    /// We must ALWAYS use the first entry from the free list.
    fn new_used_entry(&mut self, data_offset: u64) -> Result<IndexEntry> {
        // Grab the first free entry and push to the used list.
        let mut result: IndexEntry = self.free_list.pop().unwrap();

        result.data_offset = data_offset;

        // Update the header in memory.
        self.header.free_list_start_offset = result.next_offset;
        self.header.free_list_count -= 1;

        self.header.used_list_start_offset = result.current_offset;
        self.header.used_list_start_offset += 1;

        // Save the entry to disk in the new list.
        result.next_offset = self.header.used_list_start_offset;

        super::write_to_file(&self.file, &result, result.current_offset)?;

        // Write the header to disk.
        super::write_to_file(&self.file, &self.header, 0)?;

        // Activate the new entry and save to disk.
        result.state = IndexState::Used;
        super::write_to_file(&self.file, &result, result.current_offset)?;

        self.used_list.push(result.clone());

        Ok(result)
    }
}

fn read_header(file: &File) -> Result<(IndexHeader, Vec<IndexEntry>, Vec<IndexEntry>)> {
    // If the file is empty, we need to create it.
    if let Ok(metadata) = file.metadata()
        && metadata.len() == 0
    {
        // If the index file is empty we need to create
        // it with an empty used and free list.
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
        File::flush(&mut f)?;

        Ok((header, vec![], vec![]))
    } else {
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
        let header: IndexHeader = super::read_from_file(&mut f, 0)?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let mut entry: IndexEntry = super::read_from_file(&mut f, current_offset)?;
            free_list.push(entry.clone());
            entry.current_offset = current_offset;
            current_offset = entry.next_offset;
        }

        // Now we need to iterate through the used list and load it into memory.
        let mut used_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.used_list_count {
            let entry: IndexEntry = super::read_from_file(&mut f, current_offset)?;
            used_list.push(entry.clone());
            current_offset = entry.next_offset;
        }

        Ok((header, used_list, free_list))
    }
}

derive_serialization!(IndexEntry, IndexHeader);
