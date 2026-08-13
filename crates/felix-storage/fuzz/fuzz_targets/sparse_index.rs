//! Sparse index loading against arbitrary bytes.
//!
//! The index is an accelerator, so the bar is different from the segment: it is
//! allowed to be wrong, but a wrong index must never produce a seek position
//! that a scan cannot recover from, and loading one must never panic.

#![no_main]

use libfuzzer_sys::fuzz_target;

use felix_storage::segment::SparseIndex;
use felix_storage::segment::format::SEGMENT_HEADER_LEN;

fuzz_target!(|data: &[u8]| {
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let path = dir.path().join("00000000000000000000.index");
    if std::fs::write(&path, data).is_err() {
        return;
    }

    // Property 1: a malformed index loads as `None` rather than panicking. That
    // is what makes "rebuild instead of trust" possible.
    let Some(index) = SparseIndex::load(&path, 0) else {
        return;
    };

    // Property 2: entries are strictly ascending by offset, which the binary
    // search in `seek_position` depends on for correctness.
    for pair in index.entries().windows(2) {
        assert!(pair[0].offset < pair[1].offset);
    }

    // Property 3: every seek lands on a plausible record boundary — at or after
    // the segment header — for any offset at all, including ones the index has
    // never seen.
    for probe in [0u64, 1, 7, u64::MAX / 2, u64::MAX] {
        assert!(index.seek_position(probe) >= SEGMENT_HEADER_LEN);
    }
});
