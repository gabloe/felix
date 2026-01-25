use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use rkyv::{with::Skip, Archive, Deserialize, Serialize};
use crate::storage_backends::serializers::SerializeToBytes;
use anyhow::Result;

const INDEX_FILE_IDENTIFIER: u64 = 0x49445846494C45; // "IDXFILE" in ASCII

// Using 16 bits because you never know.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(u16)]
pub(crate) enum IndexVersion {
    V0,
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

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct IndexEntry {
    /// The offset in the data file.
    pub data_offset: u64,

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

    fn create_index(&mut self, data_offset: u64) -> Result<IndexEntry> {
        let mut result = IndexEntry{
            data_offset,
            next_offset: 0,
            current_offset: 0,
        };

        if self.free_list.is_empty() {
            let next_index = self.allocate_next_index_unsafe()?;
            if let Some(first) = self.used_list.first_mut() {
                result.next_offset = first.next_offset;
                first.next_offset = next_index;
            }
            Ok(result)
            // Create a new entry with data and add to used list
        } else {
            let mut r = self.free_list.pop().unwrap();
            self.used_list.push(result.clone());

            Ok(result)
        }
    }

    fn update_index(&self, _entry: IndexEntry) -> Result<()> {
        todo!()
    }

    /// Allocate a new block in the Index file. You must have the lock before
    /// calling this.
    /// @return Returns the offset in the file including the header for the entry.
    fn allocate_next_index_unsafe(&self) -> Result<u64> {
        Ok(5)
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

        Ok((
            header,
            vec![],
            vec![],
        ))
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
        let header: IndexHeader = super::read_from_file(&mut f, 0, size_of::<IndexHeader>())?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let mut entry: IndexEntry =
                super::read_from_file(&mut f, current_offset, size_of::<IndexEntry>())?;
            free_list.push(entry.clone());
            entry.current_offset = current_offset;
            current_offset = entry.next_offset;
        }

        // Now we need to iterate through the used list and load it into memory.
        let mut used_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.used_list_count {
            let entry: IndexEntry =
                super::read_from_file(&mut f, current_offset, size_of::<IndexEntry>())?;
            used_list.push(entry.clone());
            current_offset = entry.next_offset;
        }

        Ok((
            header,
            used_list,
            free_list,
        ))
    }
}