use super::*;

#[tokio::test]
async fn truncate_drops_the_suffix_and_survives_reopen() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..20 {
            log.append(&records(&[&format!("v{i:03}")]))
                .await
                .expect("append");
        }
        log.truncate(6).await.expect("truncate");
        assert_eq!(log.tail_offset().await.expect("tail"), 6);
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 0).await.len(), 6);
    log.append(&records(&["resumed"])).await.expect("append");
    assert_eq!(read_all(&log, 6).await, vec!["resumed"]);
}

/// A rebuild is a reset, not a truncation: everything goes, the base moves
/// to wherever the leader's oldest record is, in either direction, and the
/// generation history goes with the records it described.
#[tokio::test]
async fn reset_to_discards_everything_and_rebases_in_either_direction() {
    let dir = tempdir().expect("dir");
    {
        let log = open(&dir, FsyncMode::OnCommit);
        for i in 0..10 {
            log.append(&records(&[&format!("v{i:03}")]))
                .await
                .expect("append");
        }
        log.record_generation(3, 0).expect("generation");

        log.reset_to(100).await.expect("reset up");
        assert_eq!(log.base_offset(), 100);
        assert_eq!(log.tail_offset().await.expect("tail"), 100);
        assert_eq!(log.durable_offset(), 100);
        assert!(log.generations().is_empty(), "history outlived its records");
        assert!(read_all(&log, 100).await.is_empty());

        log.append(&records(&["fresh"])).await.expect("append");
        assert_eq!(read_all(&log, 100).await, vec!["fresh"]);

        log.reset_to(5).await.expect("reset down");
        assert_eq!(log.base_offset(), 5);
        assert_eq!(log.tail_offset().await.expect("tail"), 5);
        log.append(&records(&["after"])).await.expect("append");
        log.shutdown().await.expect("shutdown");
    }

    let log = open(&dir, FsyncMode::OnCommit);
    assert_eq!(log.base_offset(), 5);
    assert_eq!(log.tail_offset().await.expect("tail"), 6);
    assert_eq!(read_all(&log, 5).await, vec!["after"]);
}

#[tokio::test]
async fn on_commit_reflushes_offsets_reused_after_truncation() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::OnCommit);

    log.append(&records(&["zero", "one", "two"]))
        .await
        .expect("initial append");
    assert_eq!(log.durable_offset(), 3);

    log.truncate(1).await.expect("truncate");
    assert_eq!(log.durable_offset(), 1);

    let replacement = log
        .append(&records(&["replacement"]))
        .await
        .expect("replacement append");
    assert_eq!(replacement.first_offset, 1);
    assert_eq!(log.durable_offset(), 2);
    assert_eq!(log.unsynced_bytes(), 0);
}
