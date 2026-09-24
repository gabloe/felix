use super::*;

#[tokio::test]
async fn the_provider_returns_one_log_per_shard() {
    let root = tempdir().expect("dir");
    let provider = DiskLogProvider::new(root.path(), config(FsyncMode::None)).expect("provider");

    let shard = ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 0,
    };
    let other = ShardKey {
        shard: 1,
        ..shard.clone()
    };

    let first = provider.open(&shard).await.expect("open");
    let again = provider.open(&shard).await.expect("open again");
    let separate = provider.open(&other).await.expect("open other");

    // The same shard must resolve to the same log; two writers over one
    // directory would interleave offsets.
    first.append(&records(&["one"])).await.expect("append");
    again.append(&records(&["two"])).await.expect("append");
    assert_eq!(read_all(&again, 0).await, vec!["one", "two"]);

    // A different shard is genuinely separate.
    separate.append(&records(&["other"])).await.expect("append");
    assert_eq!(read_all(&separate, 0).await, vec!["other"]);
    assert_eq!(provider.open_shards().len(), 2);

    provider.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn provider_logs_reopen_with_their_data() {
    let root = tempdir().expect("dir");
    let shard = ShardKey {
        tenant: "acme".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard: 3,
    };
    {
        let provider =
            DiskLogProvider::new(root.path(), config(FsyncMode::OnCommit)).expect("provider");
        let log = provider.open(&shard).await.expect("open");
        log.append(&records(&["persisted"])).await.expect("append");
        provider.shutdown().await.expect("shutdown");
    }

    let provider =
        DiskLogProvider::new(root.path(), config(FsyncMode::OnCommit)).expect("provider");
    let log = provider.open(&shard).await.expect("open");
    assert_eq!(read_all(&log, 0).await, vec!["persisted"]);
}
