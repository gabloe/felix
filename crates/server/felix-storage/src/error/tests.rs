use std::error::Error;

use super::*;

#[test]
fn storage_error_display() {
    let err = StorageError::Unsupported("feature");
    assert!(err.to_string().contains("feature"));

    let err = StorageError::InvalidRange;
    assert!(err.to_string().contains("invalid range"));

    let err = StorageError::NotFound;
    assert!(err.to_string().contains("not found"));

    let err = StorageError::Corruption(Corruption::new(CorruptionKind::IndexVersion { found: 9 }));
    assert!(err.to_string().contains("corruption"));
    assert!(err.to_string().contains("unsupported index version 9"));

    let err = StorageError::SyncFailed("disk full".into());
    assert!(err.to_string().contains("disk full"));
}

#[test]
fn storage_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err = StorageError::from(io_err);
    assert!(matches!(storage_err, StorageError::Io(_)));
}

#[test]
fn storage_error_source() {
    let io_err = std::io::Error::other("test");
    let storage_err = StorageError::from(io_err);
    assert!(storage_err.source().is_some());

    let storage_err = StorageError::NotFound;
    assert!(storage_err.source().is_none());
}
