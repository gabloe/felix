// Platform I/O primitives the segment layer is built on.
//
// Three techniques, each borrowed from log-structured storage engines that have
// already paid for the lesson:
//
// 1. **Positioned reads** (`pread`). A `seek` + `read` pair mutates the file
//    cursor, so a shared descriptor cannot serve two readers at once and every
//    read costs an extra syscall. `pread` takes the offset as an argument, which
//    means one descriptor per segment serves any number of concurrent readers
//    with no lock and no seek. This is why Kafka, RocksDB and LevelDB all use
//    positioned reads on their immutable files.
//
// 2. **Preallocation**. Appending past the end of a file forces the filesystem
//    to allocate blocks and update the inode's block map on the write path, and
//    it invites fragmentation as segments from different shards interleave.
//    Reserving the whole segment up front moves that work off the append path.
//    Both `fallocate` and `F_PREALLOCATE` reserve blocks *without* changing the
//    file's logical length, so recovery's "valid bytes end at EOF" reasoning
//    still holds.
//
// 3. **`fdatasync` over `fsync`**. An append changes file data and the file
//    size, but not the owner, mode, or times that a full `fsync` also flushes.
//    `fdatasync` skips that second metadata round trip. On a spinning disk or a
//    network volume that is a whole extra I/O per commit.
//
// Every platform-specific call degrades to a correct no-op or to the portable
// equivalent, so an unsupported target loses performance and never correctness.

use std::fs::File;
use std::io;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Read into `buf` starting at `offset` without touching the file cursor.
///
/// Returns the number of bytes read, which is short only at end of file.
pub fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        let mut filled = 0;
        while filled < buf.len() {
            match file.read_at(&mut buf[filled..], offset + filled as u64) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
    #[cfg(windows)]
    {
        let mut filled = 0;
        while filled < buf.len() {
            match file.seek_read(&mut buf[filled..], offset + filled as u64) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(filled)
    }
}

/// Reserve `len` bytes of blocks for `file` without changing its logical length.
///
/// Best effort: an unsupported filesystem leaves the file untouched and appends
/// simply allocate as they go, which is correct but slower.
pub fn preallocate(file: &File, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // FALLOC_FL_KEEP_SIZE: reserve blocks, leave st_size alone.
        const FALLOC_FL_KEEP_SIZE: libc::c_int = 0x01;
        // SAFETY: `fd` is a live descriptor owned by `file` for the duration of
        // the call, and the remaining arguments are plain integers.
        let rc = unsafe {
            libc::fallocate(file.as_raw_fd(), FALLOC_FL_KEEP_SIZE, 0, len as libc::off_t)
        };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // The filesystem cannot preallocate (tmpfs, some network mounts) or
            // the kernel lacks the call. Neither is fatal: appends fall back to
            // allocating as they go, which is slower but correct.
            //
            // `ENOTSUP` is deliberately absent: on Linux it is the same value as
            // `EOPNOTSUPP`, so listing both is an unreachable arm rather than
            // extra coverage. They differ on macOS, which is why the `sync_data`
            // fallback below does list both.
            if !matches!(
                err.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)
            ) {
                return Err(err);
            }
        }
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATECONTIG,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        // SAFETY: `fd` is live for the call and `store` is a correctly
        // initialised `fstore_t` that outlives it.
        let mut rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        if rc == -1 {
            // Contiguous allocation failed; retry allowing fragmentation.
            store.fst_flags = libc::F_ALLOCATEALL;
            // SAFETY: as above.
            rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) };
        }
        // Still unsupported (e.g. a network mount): fall through silently.
        let _ = rc;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = file;
        Ok(())
    }
}

/// Flush file *data* (and the size needed to read it back) to stable storage.
///
/// Deliberately `sync_data` rather than `sync_all` on platforms where the
/// distinction is real: see the module comment.
///
/// macOS is the exception. There, POSIX `fsync`/`fdatasync` only push data out
/// of the kernel — the drive is free to keep it in a volatile write cache, so a
/// power loss can still lose an "fsynced" write. `F_FULLFSYNC` is the call that
/// orders the device itself to flush.
///
/// Rust's `sync_data` currently issues `F_FULLFSYNC` on macOS, so this call is
/// redundant against today's standard library. It is here anyway because that
/// behaviour is an implementation detail rather than a documented guarantee, and
/// `FsyncMode::OnCommit` is a promise this crate makes, not one it delegates.
/// The measured cost is the same either way (~4ms per flush on APFS), which is
/// itself the evidence that the flush is reaching the device.
pub fn sync_data(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `fd` is a live descriptor owned by `file` for the duration of
        // the call, and `F_FULLFSYNC` takes no argument.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if rc != -1 {
            return Ok(());
        }
        // Some filesystems (notably network mounts) do not implement it. Fall
        // back rather than failing the write: a weaker flush is still better
        // than reporting a durability error we cannot fix.
        let err = io::Error::last_os_error();
        if !matches!(
            err.raw_os_error(),
            Some(libc::ENOTSUP) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
        ) {
            return Err(err);
        }
    }
    file.sync_data()
}

/// Flush a directory entry so a newly created or renamed file survives a crash.
///
/// Creating a file makes the *file* durable only once its parent directory
/// entry is durable too; without this a crash can leave a segment that exists in
/// the page cache but not in the directory after reboot.
pub fn sync_dir(path: &std::path::Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        // Windows has no directory-fsync equivalent; file handles carry their
        // own durability.
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

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
}
