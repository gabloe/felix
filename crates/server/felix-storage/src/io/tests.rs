use std::io::Write;

use tempfile::tempdir;

use super::*;

#[test]
fn read_at_does_not_move_the_cursor() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    std::fs::write(&path, b"0123456789").expect("write");
    let file = File::open(&path).expect("open");

    let mut buf = [0u8; 4];
    assert_eq!(read_at(&file, &mut buf, 2).expect("read"), 4);
    assert_eq!(&buf, b"2345");
    // A second read at the same offset returns the same bytes, which it
    // could not if the first had advanced a shared cursor.
    assert_eq!(read_at(&file, &mut buf, 2).expect("read"), 4);
    assert_eq!(&buf, b"2345");
}

#[test]
fn read_at_is_short_at_end_of_file() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    std::fs::write(&path, b"abc").expect("write");
    let file = File::open(&path).expect("open");

    let mut buf = [0u8; 8];
    assert_eq!(read_at(&file, &mut buf, 1).expect("read"), 2);
    assert_eq!(&buf[..2], b"bc");
    assert_eq!(read_at(&file, &mut buf, 99).expect("read"), 0);
}

#[test]
fn preallocate_leaves_the_logical_length_alone() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    let mut file = File::create(&path).expect("create");
    file.write_all(b"hi").expect("write");

    preallocate(&file, 1024 * 1024).expect("preallocate");
    // Reserving blocks must not make the file look longer, or recovery would
    // read reserved space as a torn record tail.
    assert_eq!(file.metadata().expect("meta").len(), 2);
}

#[test]
fn preallocate_of_zero_is_a_no_op() {
    let dir = tempdir().expect("dir");
    let file = File::create(dir.path().join("f")).expect("create");
    preallocate(&file, 0).expect("preallocate");
}

#[test]
fn sync_data_and_sync_dir_succeed() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    let mut file = File::create(&path).expect("create");
    file.write_all(b"data").expect("write");
    sync_data(&file).expect("sync_data");
    sync_dir(dir.path()).expect("sync_dir");
}
