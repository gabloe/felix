use super::*;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::{Duration, timeout};

use crate::wire::read_frame_into;

#[tokio::test]
async fn in_process_publish_and_subscribe() {
    // Smoke-test the in-process path without any network transport.
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await
        .expect("register");
    let client = InProcessClient::new(broker);
    let mut receiver = client
        .subscribe("t1", "default", "updates")
        .await
        .expect("subscribe");
    client
        .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
        .await
        .expect("publish");
    let msg = receiver.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"payload"));
}

#[tokio::test]
async fn clients_share_broker_state() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "shared", Default::default())
        .await
        .expect("register");
    let publisher = InProcessClient::new(broker.clone());
    let subscriber = InProcessClient::new(broker);
    let mut receiver = subscriber
        .subscribe("t1", "default", "shared")
        .await
        .expect("subscribe");
    publisher
        .publish(
            "t1",
            "default",
            "shared",
            Bytes::from_static(b"from-publisher"),
        )
        .await
        .expect("publish");
    let msg = receiver.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"from-publisher"));
}

#[tokio::test]
#[serial_test::serial]
async fn cache_worker_exits_on_stream_error() -> Result<()> {
    struct EnvGuard;

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                std::env::remove_var("FELIX_PUB_CONN_POOL");
                std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
                std::env::remove_var("FELIX_CACHE_CONN_POOL");
                std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
                std::env::remove_var("FELIX_EVENT_CONN_POOL");
            }
        }
    }

    let _env_guard = EnvGuard;
    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "1");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "1");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_EVENT_CONN_POOL", "1");
    }

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..3 {
            let connection = server.accept().await?;
            connections.push(connection);
        }

        let mut closed = false;
        for connection in connections {
            let result = timeout(Duration::from_secs(1), connection.accept_bi()).await;
            if let Ok(Ok((mut send, mut recv))) = result {
                let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
                let _ = read_frame_into(&mut recv, &mut frame_scratch, false).await;
                let _ = send.finish();
                closed = true;
            }
        }

        if !closed {
            return Err(anyhow::anyhow!("server did not accept cache stream"));
        }

        Result::<()>::Ok(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config(cert)?,
        TransportConfig::default(),
    )
    .await?;

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache should fail");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("cache response closed")
            || err_msg.contains("cache worker closed")
            || err_msg.contains("connection lost"),
        "unexpected cache error: {err_msg}"
    );

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache worker should be closed");
    assert!(err.to_string().contains("cache worker closed"));

    server_task.await.context("server task join")??;
    Ok(())
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}

// ===== Config tests =====

#[allow(deprecated)]
#[test]
fn config_optimized_defaults() {
    use crate::config::*;
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);

    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(
        config.publish_streams_per_conn,
        DEFAULT_PUB_STREAMS_PER_CONN
    );
    assert_eq!(config.publish_chunk_bytes, DEFAULT_PUBLISH_CHUNK_BYTES);
    assert_eq!(config.cache_conn_pool, DEFAULT_CACHE_CONN_POOL);
    assert_eq!(
        config.cache_streams_per_conn,
        DEFAULT_CACHE_STREAMS_PER_CONN
    );
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
    assert_eq!(
        config.event_conn_recv_window,
        DEFAULT_EVENT_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.event_stream_recv_window,
        DEFAULT_EVENT_STREAM_RECV_WINDOW
    );
    assert_eq!(config.event_send_window, DEFAULT_EVENT_SEND_WINDOW);
    assert_eq!(
        config.cache_conn_recv_window,
        DEFAULT_CACHE_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.cache_stream_recv_window,
        DEFAULT_CACHE_STREAM_RECV_WINDOW
    );
    assert_eq!(config.cache_send_window, DEFAULT_CACHE_SEND_WINDOW);
    assert_eq!(
        config.event_router_max_pending,
        DEFAULT_EVENT_ROUTER_MAX_PENDING
    );
    assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    assert!(!config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_from_env_variables() {
    use crate::config::*;

    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "2");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "3");
        std::env::set_var("FELIX_PUBLISH_CHUNK_BYTES", "32768");
        std::env::set_var("FELIX_PUBLISH_SHARDING", "rr");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "4");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "5");
        std::env::set_var("FELIX_EVENT_CONN_POOL", "6");
        std::env::set_var("FELIX_EVENT_CONN_RECV_WINDOW", "1024");
        std::env::set_var("FELIX_EVENT_STREAM_RECV_WINDOW", "2048");
        std::env::set_var("FELIX_EVENT_SEND_WINDOW", "4096");
        std::env::set_var("FELIX_CACHE_CONN_RECV_WINDOW", "8192");
        std::env::set_var("FELIX_CACHE_STREAM_RECV_WINDOW", "16384");
        std::env::set_var("FELIX_CACHE_SEND_WINDOW", "32768");
        std::env::set_var("FELIX_EVENT_ROUTER_MAX_PENDING", "1000");
        std::env::set_var("FELIX_MAX_FRAME_BYTES", "8388608");
        std::env::set_var("FELIX_BENCH_EMBED_TS", "true");
    }

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");

    assert_eq!(config.publish_conn_pool, 2);
    assert_eq!(config.publish_streams_per_conn, 3);
    assert_eq!(config.publish_chunk_bytes, 32768);
    assert_eq!(config.cache_conn_pool, 4);
    assert_eq!(config.cache_streams_per_conn, 5);
    assert_eq!(config.event_conn_pool, 6);
    assert_eq!(config.event_conn_recv_window, 1024);
    assert_eq!(config.event_stream_recv_window, 2048);
    assert_eq!(config.event_send_window, 4096);
    assert_eq!(config.cache_conn_recv_window, 8192);
    assert_eq!(config.cache_stream_recv_window, 16384);
    assert_eq!(config.cache_send_window, 32768);
    assert_eq!(config.event_router_max_pending, 1000);
    assert_eq!(config.max_frame_bytes, 8388608);
    assert!(config.bench_embed_ts);

    // Clean up
    unsafe {
        std::env::remove_var("FELIX_PUB_CONN_POOL");
        std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_PUBLISH_CHUNK_BYTES");
        std::env::remove_var("FELIX_PUBLISH_SHARDING");
        std::env::remove_var("FELIX_CACHE_CONN_POOL");
        std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_EVENT_CONN_POOL");
        std::env::remove_var("FELIX_EVENT_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_SEND_WINDOW");
        std::env::remove_var("FELIX_CACHE_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_SEND_WINDOW");
        std::env::remove_var("FELIX_EVENT_ROUTER_MAX_PENDING");
        std::env::remove_var("FELIX_MAX_FRAME_BYTES");
        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
fn config_from_yaml_file() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let yaml = r#"
publish_conn_pool: 10
publish_streams_per_conn: 20
publish_chunk_bytes: 65536
publish_sharding: "hash_stream"
cache_conn_pool: 30
cache_streams_per_conn: 40
event_conn_pool: 50
event_conn_recv_window: 10240
event_stream_recv_window: 20480
event_send_window: 40960
cache_conn_recv_window: 81920
cache_stream_recv_window: 163840
cache_send_window: 327680
event_router_max_pending: 2000
max_frame_bytes: 16777216
bench_embed_ts: true
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    assert_eq!(config.publish_conn_pool, 10);
    assert_eq!(config.publish_streams_per_conn, 20);
    assert_eq!(config.publish_chunk_bytes, 65536);
    assert_eq!(config.cache_conn_pool, 30);
    assert_eq!(config.cache_streams_per_conn, 40);
    assert_eq!(config.event_conn_pool, 50);
    assert_eq!(config.event_conn_recv_window, 10240);
    assert_eq!(config.event_stream_recv_window, 20480);
    assert_eq!(config.event_send_window, 40960);
    assert_eq!(config.cache_conn_recv_window, 81920);
    assert_eq!(config.cache_stream_recv_window, 163840);
    assert_eq!(config.cache_send_window, 327680);
    assert_eq!(config.event_router_max_pending, 2000);
    assert_eq!(config.max_frame_bytes, 16777216);
    assert!(config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_yaml_overrides_ignore_zero_values() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let yaml = r#"
publish_conn_pool: 0
cache_conn_pool: 5
event_conn_pool: 0
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    // Zero values should be ignored, defaults used
    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(config.cache_conn_pool, 5);
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
}

#[allow(deprecated)]
#[test]
fn config_invalid_yaml_file_returns_error() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let invalid_yaml = r#"
publish_conn_pool: [invalid
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(invalid_yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some(path));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_nonexistent_file_returns_error() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some("/nonexistent/path/config.yaml"));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_transport_configs() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.event_conn_recv_window = 12345;
    config.event_stream_recv_window = 23456;
    config.event_send_window = 34567;
    config.cache_conn_recv_window = 45678;
    config.cache_stream_recv_window = 56789;
    config.cache_send_window = 67890;

    let base_transport = TransportConfig::default();

    let event_transport = event_transport_config(base_transport.clone(), &config);
    assert_eq!(event_transport.receive_window, 12345);
    assert_eq!(event_transport.stream_receive_window, 23456);
    assert_eq!(event_transport.send_window, 34567);

    let cache_transport = cache_transport_config(base_transport, &config);
    assert_eq!(cache_transport.receive_window, 45678);
    assert_eq!(cache_transport.stream_receive_window, 56789);
    assert_eq!(cache_transport.send_window, 67890);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_bool_parsing() {
    unsafe {
        // Test various true values
        for val in &["1", "true", "TRUE", "yes", "YES"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(config.bench_embed_ts, "Failed for value: {}", val);
        }

        // Test false values
        for val in &["0", "false", "FALSE", "no", "NO", "random"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(!config.bench_embed_ts, "Failed for value: {}", val);
        }

        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_sharding_variants() {
    use crate::client::sharding::PublishSharding;

    unsafe {
        // Test round robin
        std::env::set_var("FELIX_PUB_SHARDING", "rr");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::RoundRobin
        ));

        // Test hash_stream
        std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        // Test invalid value (should default to HashStream)
        std::env::set_var("FELIX_PUB_SHARDING", "invalid");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        std::env::remove_var("FELIX_PUB_SHARDING");
    }
}
