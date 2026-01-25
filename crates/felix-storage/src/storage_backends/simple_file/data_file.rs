use crate::storage_backends::serializers::{DeserializeFromBytes, SerializeToBytes};
use crate::{derive_serialization};
use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize, with::Skip};
/// # Summary
/// The data file holds the end-user data. The file is composed of a header and then chunks of data
/// which can be categorized into two types
///     * Free
///     * Used
/// The free list is the previously used space used in the file which we can re-use vs. growing the
/// file. The used list contains existing customer data.
///
/// # Long Term Goals
/// Long term we would want to support the following
///     * File Compaction
///     * Clear data on free
///     * Encryption at rest.
///
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Constants
const DATA_FILE_IDENTIFIER: u64 = 0x4441544146494C45; // "DATAFILE" in ASCII

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(u8)]
enum EntryState {
    Free = 0,
    Used = 1,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
struct DataEntry {
    /// The unique identifier used to find the data. For items in the free list
    /// the values are set to 0.
    identifier: [u8; 1024],

    /// The state of the current entry in the file. When we want to free an entry we set
    /// this to free before all other changes. When we  want to use an entry we mark this
    /// used after setting all other values. This ensures that if there is Power loss we are
    /// not in an inconsistent state.
    state: EntryState,

    /// The current offset in the index file. This is only so we can update the
    /// file when we pull this out of the free list or place it in the free list.
    #[rkyv(with = Skip)]
    current_offset: u64,

    /// The next offset in the index file. A value of 0 indicates end of the list.
    next_offset: u64,
}

// Using 16 bits because you never know.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(u16)]
pub(crate) enum DataVersion {
    V0,
}

/// This is the header of the data file. It is 512 bytes long and contains metadata about the files
/// content.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[repr(C)]
pub(crate) struct DataHeader {
    /// Identifies this file as a DataFile.
    pub identifier: u64,

    /// Indicates the version of the data file.
    pub version: DataVersion,

    pub free_list_count: u64,

    /// Indicates the offset to the first free list is.
    pub free_list_start_offset: u64,
}

derive_serialization!(DataHeader, DataEntry);

pub(crate) struct DataFile {
    pub file_path: PathBuf,
    header: DataHeader,
    file: File,
    free_list: Vec<DataEntry>,
    used_list: Option<Vec<DataEntry>>,
}

impl DataFile {
    pub fn open(file_path: PathBuf) -> Result<Self> {
        let file = File::create(file_path.clone())?;

        // If the file is empty, we need to create it.
        let (header, free_list) = read_header(&file)?;

        Ok(Self {
            file_path,
            header,
            file,
            free_list,
            used_list: None,
        })
    }

    /// Allocate a new block at the end of the data file for use. This assumes we
    /// have already locked the SimpleFileStorage object for writes.
    /// @ return The offset in the file where the data starts (including header).
    fn allocate_next_data_unsafe(&self, size: u64) -> Result<u64> {
        // Make room for the new data + header.
        let current_length = self.file.metadata()?.len();
        self.file
            .set_len(current_length + size + size_of::<DataEntry>() as u64)?;

        Ok(current_length)
    }
}

fn read_header(file: &File) -> Result<(DataHeader, Vec<DataEntry>)> {
    if let Ok(metadata) = file.metadata()
        && metadata.len() == 0
    {
        let header = DataHeader {
            identifier: DATA_FILE_IDENTIFIER, // "DATAFILE"
            version: DataVersion::V0,
            free_list_count: 0,
            free_list_start_offset: 0,
        };

        super::write_to_file(&file, &header, 0)?;
        Ok((header, vec![]))
    } else {
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
        let header: DataHeader = super::read_from_file(&mut f, 0)?;
        let mut free_list = vec![];
        let mut current_offset = header.free_list_start_offset;
        for _ in 0..header.free_list_count {
            let mut entry: DataEntry = super::read_from_file(&mut f, current_offset)?;
            free_list.push(entry.clone());
            entry.current_offset = current_offset;
            current_offset = entry.next_offset;
        }

        Ok((header, free_list))
    }
}
