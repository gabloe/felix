use tempfile::tempdir;

use super::*;

#[test]
fn cursor_serves_overlapping_windows_without_moving_backwards_wrongly() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    std::fs::write(&path, b"0123456789").expect("write");
    let file = File::open(&path).expect("open");
    let mut cursor = SegmentCursor::new(&file);

    assert_eq!(cursor.slice_at(0, 4).expect("read"), b"0123");
    assert_eq!(cursor.slice_at(4, 4).expect("read"), b"4567");
    // Re-reading an earlier position must still be correct.
    assert_eq!(cursor.slice_at(1, 3).expect("read"), b"123");
    // Past the end returns fewer bytes than asked for.
    assert_eq!(cursor.slice_at(8, 8).expect("read"), b"89");
    assert!(cursor.slice_at(50, 4).expect("read").is_empty());
}
