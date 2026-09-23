// Tests cover error tolerance (skipping on fetch errors), deletion propagation, and cursor advancement.
use super::*;
use crate::test_support::{build_test_client, spawn_axum_with_shutdown, wait_for_listen};
use axum::{Json, Router, http::StatusCode, routing::get};
use felix_storage::EphemeralCache;
use std::net::SocketAddr;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

async fn serve_router(
    router: Router,
) -> Result<(
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, handle) = spawn_axum_with_shutdown(listener, router);
    wait_for_listen(addr).await?;
    Ok((addr, shutdown_tx, handle))
}

fn error_router() -> Router {
    Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
}

#[tokio::test]
async fn sync_once_logs_and_skips_on_errors() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let (addr, shutdown_tx, handle) = serve_router(error_router()).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 0);
        assert_eq!(state.next_namespace_seq, 0);
        assert_eq!(state.next_cache_seq, 0);
        assert_eq!(state.next_stream_seq, 0);
        assert!(!broker.namespace_exists("t1", "ns").await);
        assert!(!broker.stream_exists("t1", "ns", "s1").await);
        assert!(!broker.cache_exists("t1", "ns", "c1").await);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn sync_once_handles_tenant_deletion() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t1".to_string(),
                        }],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![TenantChange {
                            op: TenantChangeOp::Deleted,
                            tenant_id: "t1".to_string(),
                            tenant: None,
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 2);
        let err = broker.register_namespace("t1", "ns").await;
        assert!(err.is_err());

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn sync_once_handles_namespace_deletion() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![Tenant {
                            tenant_id: "t1".to_string(),
                        }],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![Namespace {
                            tenant_id: "t1".to_string(),
                            namespace: "ns".to_string(),
                        }],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![NamespaceChange {
                            op: NamespaceChangeOp::Deleted,
                            key: NamespaceKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns".to_string(),
                            },
                            namespace: None,
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_namespace_seq, 2);
        assert!(!broker.namespace_exists("t1", "ns").await);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn sync_once_registers_created_and_updated_changes() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(|| async {
                    Json(TenantChangesResponse {
                        items: vec![TenantChange {
                            op: TenantChangeOp::Created,
                            tenant_id: "t1".to_string(),
                            tenant: Some(Tenant {
                                tenant_id: "t1".to_string(),
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![NamespaceChange {
                            op: NamespaceChangeOp::Created,
                            key: NamespaceKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            },
                            namespace: Some(Namespace {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![CacheChange {
                            op: CacheChangeOp::Updated,
                            key: CacheKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                cache: "c1".to_string(),
                            },
                            cache: Some(Cache {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                cache: "c1".to_string(),
                                consistency: None,
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![StreamChange {
                            op: StreamChangeOp::Created,
                            key: StreamKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                stream: "orders".to_string(),
                            },
                            stream: Some(Stream {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                                stream: "orders".to_string(),
                                shards: 2,
                                durable: true,
                                consistency: None,
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let state = sync_once(&broker, &client, &base_url, None, SyncState::new()).await?;
        assert_eq!(state.next_tenant_seq, 2);
        assert_eq!(state.next_namespace_seq, 2);
        assert_eq!(state.next_cache_seq, 2);
        assert_eq!(state.next_stream_seq, 2);
        assert!(broker.namespace_exists("t1", "ns1").await);
        assert!(broker.cache_exists("t1", "ns1", "c1").await);
        // `orders` is marked durable and this broker has no durable storage,
        // so the sync skips it and keeps going rather than registering a
        // stream it could not persist. Everything else still applied.
        assert!(!broker.stream_exists("t1", "ns1", "orders").await);

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn fetch_cache_snapshot_rejects_non_200() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new().route(
            "/v1/caches/snapshot",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let result = fetch_cache_snapshot(&client, &base_url, None).await;
        assert!(result.is_err());

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn apply_namespace_create_registers_missing_tenant() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_namespace_create(&broker, "t1".to_string(), "ns1".to_string()).await?;
    assert!(broker.namespace_exists("t1", "ns1").await);
    Ok(())
}

#[tokio::test]
async fn apply_cache_upsert_recovers_from_missing_parents() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_cache_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "c1".to_string(),
        CacheMetadata::default(),
    )
    .await?;
    assert!(broker.cache_exists("t1", "ns1", "c1").await);

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2".to_string()).await?;
    apply_cache_upsert(
        &broker,
        "t2".to_string(),
        "ns2".to_string(),
        "c2".to_string(),
        CacheMetadata::default(),
    )
    .await?;
    assert!(broker.cache_exists("t2", "ns2", "c2").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_recovers_from_missing_parents() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: false,
            shards: 2,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "orders").await);

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t2".to_string()).await?;
    apply_stream_upsert(
        &broker,
        "t2".to_string(),
        "ns2".to_string(),
        "events".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t2", "ns2", "events").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_skips_durable_streams_without_storage() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));

    // A durable stream on a broker with no durable storage must not be
    // registered: a registered stream accepts publishes, and those would be
    // acknowledged against a guarantee that does not exist.
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(!broker.stream_exists("t1", "ns1", "orders").await);

    // The sync must keep going, so a later non-durable stream still applies.
    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "telemetry".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "telemetry").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_registers_durable_streams_when_storage_exists() -> Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));

    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    assert!(broker.stream_exists("t1", "ns1", "orders").await);
    Ok(())
}

#[test]
fn a_partially_seeded_state_is_not_seeded() {
    // Every cursor must be non-zero. A snapshot that failed leaves its
    // cursor at 0 and `sync_once` still returns Ok, so this is the only
    // thing standing between a failed cold start and a ready broker.
    let mut state = SyncState::new();
    assert!(!state.is_seeded(), "a fresh state is not seeded");

    state.seeded.tenants = true;
    state.seeded.namespaces = true;
    state.seeded.caches = true;
    assert!(!state.is_seeded(), "streams never arrived");

    state.seeded.streams = true;
    assert!(state.is_seeded());

    // A cursor advancing on its own proves nothing: a failed snapshot
    // leaves the cursor at 0 and the change feed in the same iteration
    // polls from zero, which can carry it straight to the global tail.
    let mut cursors_only = SyncState::new();
    cursors_only.next_tenant_seq = 9_000;
    cursors_only.next_namespace_seq = 9_000;
    cursors_only.next_cache_seq = 9_000;
    cursors_only.next_stream_seq = 9_000;
    assert!(
        !cursors_only.is_seeded(),
        "advanced cursors must not stand in for an applied snapshot"
    );
}

#[tokio::test]
async fn an_empty_catalog_still_becomes_seeded() -> Result<()> {
    // A successful snapshot of an empty catalog returns next_seq == 0. That
    // is a valid cursor, not a failure, and such a deployment has to be
    // able to reach ready.
    let router = Router::new()
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        );
    let (addr, shutdown_tx, handle) = serve_router(router).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let client = build_test_client()?;

    let state = sync_once(
        &broker,
        &client,
        &format!("http://{addr}"),
        None,
        SyncState::new(),
    )
    .await?;
    assert!(
        state.is_seeded(),
        "an empty catalog that fetched cleanly is seeded"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    Ok(())
}

#[tokio::test]
async fn a_failed_snapshot_never_reports_the_catalog_as_seeded() -> Result<()> {
    // A control plane that answers every snapshot with 500. `sync_once`
    // logs and returns Ok, so without the seeded check the broker would
    // report ready having restored nothing.
    let router = Router::new()
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let (addr, shutdown_tx, handle) = serve_router(router).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let client = build_test_client()?;

    let state = sync_once(
        &broker,
        &client,
        &format!("http://{addr}"),
        None,
        SyncState::new(),
    )
    .await?;
    assert!(
        !state.is_seeded(),
        "a sync where every snapshot failed must not count as seeded"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_propagates_storage_failures() -> Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));

    // Corrupt the shard directory so opening the log fails. A file where
    // the segment directory belongs is the simplest way to make recovery
    // return an I/O error rather than a clean empty log.
    let shard_dir = felix_storage::disk_log::layout::shard_dir(
        dir.path(),
        &felix_storage::log::ShardKey {
            tenant: "t1".into(),
            namespace: "ns1".into(),
            stream: "orders".into(),
            shard: 0,
        },
    );
    std::fs::write(&shard_dir, b"not a directory").expect("block the shard dir");

    let err = apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await
    .expect_err("storage failure must not be skipped");

    // The sync fails rather than advancing its cursor past a durable stream
    // it could not create. Skipping here would leave the broker ready with
    // the stream permanently missing.
    assert!(err.to_string().contains("durable storage error"), "{err}");
    assert!(!broker.stream_exists("t1", "ns1", "orders").await);
    Ok(())
}

#[tokio::test]
async fn apply_stream_upsert_rejects_live_durability_changes() -> Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));

    apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: false,
            shards: 1,
            ..Default::default()
        },
    )
    .await?;
    let err = apply_stream_upsert(
        &broker,
        "t1".to_string(),
        "ns1".to_string(),
        "orders".to_string(),
        StreamMetadata {
            durable: true,
            shards: 1,
            ..Default::default()
        },
    )
    .await
    .expect_err("live durability change");

    assert!(
        err.to_string().contains("requires removal and recreation"),
        "{err}"
    );
    assert!(broker.stream_exists("t1", "ns1", "orders").await);
    assert_eq!(
        std::fs::read_dir(dir.path())
            .expect("read storage root")
            .filter_map(|entry| entry.ok())
            .count(),
        0,
        "a rejected control-plane update must not create durable state"
    );
    Ok(())
}

#[tokio::test]
async fn fetch_stream_changes_rejects_invalid_json() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let router = Router::new().route(
            "/v1/streams/changes",
            get(|| async { (StatusCode::OK, "not-json") }),
        );
        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let client = build_test_client()?;

        let result = fetch_changes(&client, &base_url, None, 0).await;
        assert!(result.is_err());

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

#[tokio::test]
async fn start_sync_retries_after_transient_parent_ordering_error() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let namespace_polls = StdArc::new(AtomicUsize::new(0));
        let namespace_polls_clone = StdArc::clone(&namespace_polls);
        let router = Router::new()
            .route(
                "/v1/tenants/snapshot",
                get(|| async {
                    Json(TenantSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/namespaces/snapshot",
                get(|| async {
                    Json(NamespaceSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/caches/snapshot",
                get(|| async {
                    Json(CacheSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/streams/snapshot",
                get(|| async {
                    Json(StreamSnapshotResponse {
                        items: vec![],
                        next_seq: 1,
                    })
                }),
            )
            .route(
                "/v1/tenants/changes",
                get(move || {
                    let namespace_polls = StdArc::clone(&namespace_polls_clone);
                    async move {
                        let poll = namespace_polls.fetch_add(1, Ordering::SeqCst);
                        let items = if poll == 0 {
                            vec![]
                        } else {
                            vec![TenantChange {
                                op: TenantChangeOp::Created,
                                tenant_id: "t1".to_string(),
                                tenant: Some(Tenant {
                                    tenant_id: "t1".to_string(),
                                }),
                            }]
                        };
                        Json(TenantChangesResponse { items, next_seq: 2 })
                    }
                }),
            )
            .route(
                "/v1/namespaces/changes",
                get(|| async {
                    Json(NamespaceChangesResponse {
                        items: vec![NamespaceChange {
                            op: NamespaceChangeOp::Created,
                            key: NamespaceKey {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            },
                            namespace: Some(Namespace {
                                tenant_id: "t1".to_string(),
                                namespace: "ns1".to_string(),
                            }),
                        }],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/caches/changes",
                get(|| async {
                    Json(CacheChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            )
            .route(
                "/v1/streams/changes",
                get(|| async {
                    Json(StreamChangesResponse {
                        items: vec![],
                        next_seq: 2,
                    })
                }),
            );

        let (addr, shutdown_tx, handle) = serve_router(router).await?;
        let base_url = format!("http://{}", addr);
        let sync_task = tokio::spawn(start_sync_with_signal(
            Arc::clone(&broker),
            base_url,
            Duration::from_millis(10),
            None,
            None,
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if broker.namespace_exists("t1", "ns1").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(broker.namespace_exists("t1", "ns1").await);

        sync_task.abort();
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server shutdown");
        Ok(())
    })
    .await
    .expect("test timeout")
}

/// What the control plane sends for `consistency`, and what this broker
/// makes of it.
mod consistency {
    use super::*;

    /// A control plane that predates replication sends no level at all, and
    /// every stream written before this existed behaves as it did.
    #[test]
    fn an_absent_level_is_leader() {
        assert_eq!(
            read_consistency(None).expect("read"),
            ConsistencyLevel::Leader
        );
    }

    /// The exact strings the control plane serializes. Pinned on this side
    /// too, because the two enums are compiled separately and nothing else
    /// would catch them drifting apart.
    #[test]
    fn the_levels_are_read_by_their_wire_names() {
        assert_eq!(
            read_consistency(Some("Leader")).expect("read"),
            ConsistencyLevel::Leader,
        );
        assert_eq!(
            read_consistency(Some("Quorum")).expect("read"),
            ConsistencyLevel::Quorum,
        );
    }

    /// **An unrecognised level is refused, never defaulted.** Falling back
    /// to `Leader` would serve a stream the operator asked to be
    /// quorum-replicated at the weaker guarantee, and the acknowledgement
    /// would keep its meaning on paper while losing it in fact.
    #[test]
    fn an_unknown_level_is_refused_rather_than_downgraded() {
        let err = read_consistency(Some("Everywhere")).expect_err("should refuse");
        assert!(err.to_string().contains("Everywhere"), "{err}");
    }

    /// Case matters: the wire form is what the control plane serializes, and
    /// guessing at near-misses is how a downgrade slips through.
    #[test]
    fn a_near_miss_is_not_guessed_at() {
        assert!(read_consistency(Some("quorum")).is_err());
        assert!(read_consistency(Some("QUORUM")).is_err());
        assert!(read_consistency(Some("")).is_err());
    }
}
