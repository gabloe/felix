//! The Kafka listener against a real Kafka client: `kcat` (librdkafka 1.8).
//!
//! kcat runs from the `edenhill/kcat:1.7.1` image, so these tests need Docker.
//! Without it each test says so and returns; CI has Docker. On Linux the
//! container shares the host network; elsewhere (Docker Desktop) it reaches
//! the host as `host.docker.internal`, which is also what the listener
//! advertises in Metadata.
//!
//! Run with `cargo test -p felix-broker-service --test kafka_kcat`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Output;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::KafkaListenerConfig;
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_broker_service::serving::kafka::{BrokerCluster, KafkaListener, tls_config};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use jsonwebtoken::Algorithm;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
const TENANT: &str = "t1";
const TOPIC: &str = "orders.created";
const SHARDS: u32 = 3;
/// Generous: the image may run under emulation, and each kcat is a container.
const KCAT_BOUND: Duration = Duration::from_secs(60);

struct Felix {
    broker: Arc<Broker>,
    token: String,
    /// `host:port` a kcat container dials.
    plain: String,
    tls: String,
    anonymous: String,
    certs: TempDir,
    _data: TempDir,
    shutdown: CancellationToken,
}

impl Drop for Felix {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Felix {
    async fn publish(&self, shard: u32, values: &[&str]) {
        let payloads: Vec<Bytes> = values
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_bytes()))
            .collect();
        self.broker
            .publish_batch(TENANT, "orders", "created", shard, &payloads)
            .await
            .expect("publish");
    }

    /// kcat against the SASL/PLAIN listener, with this tenant's token.
    async fn kcat(&self, args: &[&str]) -> Output {
        let mut all = vec![
            "-b",
            self.plain.as_str(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
        ];
        let password = format!("sasl.password={}", self.token);
        all.extend(["-X", password.as_str()]);
        all.extend_from_slice(args);
        kcat(&all, None).await
    }
}

fn docker_host() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1"
    } else {
        "host.docker.internal"
    }
}

/// Whether Docker can run the kcat image. Pulls it if missing.
async fn kcat_available() -> bool {
    let ok = Command::new("docker")
        .args(["run", "--rm", KCAT_IMAGE, "-V"])
        .output()
        .await
        .is_ok_and(|out| out.status.success());
    if !ok {
        eprintln!("skipping: docker cannot run {KCAT_IMAGE}");
    }
    ok
}

async fn kcat(args: &[&str], certs: Option<&Path>) -> Output {
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "-i"]);
    if cfg!(target_os = "linux") {
        command.args(["--network", "host"]);
    }
    if let Some(certs) = certs {
        command.args(["-v", &format!("{}:/certs:ro", certs.display())]);
    }
    command.arg(KCAT_IMAGE).args(args).kill_on_drop(true);
    tokio::time::timeout(KCAT_BOUND, command.output())
        .await
        .unwrap_or_else(|_| panic!("kcat {args:?} did not finish within {KCAT_BOUND:?}"))
        .expect("run docker")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("0.0.0.0:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn start() -> Felix {
    let data = tempfile::tempdir().expect("dir");
    let storage = DurableStorage::open(
        data.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, "orders")
        .await
        .expect("ns");
    broker
        .register_stream(
            TENANT,
            "orders",
            "created",
            StreamMetadata {
                durable: true,
                shards: SHARDS,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    let (auth, token) = auth_and_token();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let certs = tempfile::tempdir().expect("certs");
    std::fs::write(certs.path().join("ca.pem"), cert.cert.pem()).expect("write cert");
    let tls = tls_config(
        cert.cert.der().clone(),
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())),
    )
    .expect("tls");

    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    let mut addrs = Vec::new();
    for (tls_on, anonymous) in [(false, None), (true, None), (false, Some(TENANT))] {
        let port = free_port();
        let advertise = format!("{}:{port}", docker_host());
        let config = KafkaListenerConfig {
            listen: SocketAddr::from(([0, 0, 0, 0], port)),
            advertise: advertise.clone(),
            tls: tls_on,
            anonymous_tenant: anonymous.map(str::to_string),
            default_namespace: None,
            max_connections: 64,
        };
        let cluster = BrokerCluster::new(Arc::clone(&auth), None, None, "felix", &advertise)
            .expect("cluster");
        let listener = KafkaListener::bind(
            &config,
            Some(Arc::clone(&tls)),
            Arc::clone(&broker),
            Arc::new(cluster),
            "felix-test".to_string(),
        )
        .await
        .expect("bind");
        tokio::spawn(listener.serve(shutdown.clone(), connections.clone()));
        addrs.push(advertise);
    }
    let anonymous = addrs.pop().expect("anonymous");
    let tls = addrs.pop().expect("tls");
    let plain = addrs.pop().expect("plain");
    Felix {
        broker,
        token,
        plain,
        tls,
        anonymous,
        certs,
        _data: data,
        shutdown,
    }
}

/// A broker auth that knows one tenant key, and a token signed by it that may
/// read the tenant's streams.
fn auth_and_token() -> (Arc<BrokerAuth>, String) {
    let private_key = [9u8; 32];
    let public_key = SigningKey::from_bytes(&private_key)
        .verifying_key()
        .to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(TENANT), jwks.clone());
    let material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key,
        public_key,
        jwks,
    };
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(HashMap::from([(TENANT.to_string(), material)])),
    );
    let token = issuer
        .mint(
            &TenantId::new(TENANT),
            "p:kafka-test",
            vec!["stream.subscribe:stream:t1/orders/*".to_string()],
        )
        .expect("mint");
    (Arc::new(BrokerAuth::with_key_store(key_store)), token)
}

/// `partition:offset:value` lines, sorted.
fn records(output: &Output) -> Vec<String> {
    let mut lines: Vec<String> = stdout(output)
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

#[tokio::test]
async fn kcat_lists_the_topic_with_a_partition_per_shard() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let output = felix.kcat(&["-L", "-t", TOPIC]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(
        text.contains(&format!("topic \"{TOPIC}\" with {SHARDS} partitions")),
        "{text}"
    );
    assert!(
        text.contains(&felix.plain),
        "the broker's advertised address: {text}"
    );
}

#[tokio::test]
async fn kcat_consumes_every_partition_from_the_beginning_and_from_an_offset() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(0, &["a0", "a1"]).await;
    felix.publish(1, &["b0", "b1", "b2"]).await;
    felix.publish(2, &["c0"]).await;

    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        records(&output),
        ["0:0:a0", "0:1:a1", "1:0:b0", "1:1:b1", "1:2:b2", "2:0:c0"]
    );

    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-p",
            "1",
            "-o",
            "1",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["1:1:b1", "1:2:b2"]);

    // -o end with -e: nothing new, so nothing printed, and kcat exits.
    let output = felix
        .kcat(&[
            "-C",
            "-t",
            TOPIC,
            "-o",
            "end",
            "-e",
            "-q",
            "-f",
            "%p:%o:%s\\n",
        ])
        .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(records(&output).is_empty(), "{:?}", records(&output));
}

#[tokio::test]
async fn kcat_queries_offsets() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(1, &["b0", "b1", "b2"]).await;
    let latest = format!("{TOPIC}:1:-1");
    let output = felix.kcat(&["-Q", "-t", latest.as_str()]).await;
    let text = stdout(&output);
    assert!(output.status.success(), "{text}\n{}", stderr(&output));
    assert!(text.contains(&format!("{TOPIC} [1] offset 3")), "{text}");
    let earliest = format!("{TOPIC}:1:-2");
    let output = felix.kcat(&["-Q", "-t", earliest.as_str()]).await;
    assert!(
        stdout(&output).contains(&format!("{TOPIC} [1] offset 0")),
        "{}",
        stdout(&output)
    );
}

#[tokio::test]
async fn kcat_receives_a_record_published_while_its_fetch_waits() {
    if !kcat_available().await {
        return;
    }
    let felix = Arc::new(start().await);
    felix.publish(0, &["old"]).await;
    let consumer = {
        let felix = Arc::clone(&felix);
        tokio::spawn(async move {
            // No -e: kcat keeps fetching until it has one record (-c 1).
            felix
                .kcat(&[
                    "-C", "-t", TOPIC, "-p", "0", "-o", "1", "-c", "1", "-q", "-f", "%o:%s\\n",
                ])
                .await
        })
    };
    // Enough for the container to start and park a fetch at the tail.
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(!consumer.is_finished(), "kcat is waiting for a record");
    let published = Instant::now();
    felix.publish(0, &["new"]).await;
    let output = consumer.await.expect("task");
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["1:new"]);
    assert!(published.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn kcat_reads_over_sasl_ssl_and_anonymously_when_allowed() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    felix.publish(2, &["c0"]).await;
    let password = format!("sasl.password={}", felix.token);
    let output = kcat(
        &[
            "-b",
            felix.tls.as_str(),
            "-X",
            "security.protocol=SASL_SSL",
            "-X",
            "ssl.ca.location=/certs/ca.pem",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
            "-X",
            password.as_str(),
            "-C",
            "-t",
            TOPIC,
            "-p",
            "2",
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%o:%s\\n",
        ],
        Some(felix.certs.path()),
    )
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["0:c0"]);

    let output = kcat(
        &[
            "-b",
            felix.anonymous.as_str(),
            "-C",
            "-t",
            TOPIC,
            "-p",
            "2",
            "-o",
            "beginning",
            "-e",
            "-q",
            "-f",
            "%o:%s\\n",
        ],
        None,
    )
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), ["0:c0"]);
}

#[tokio::test]
async fn kcat_with_a_bad_token_fails_with_the_reason() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let output = kcat(
        &[
            "-b",
            felix.plain.as_str(),
            "-X",
            "security.protocol=SASL_PLAINTEXT",
            "-X",
            "sasl.mechanisms=PLAIN",
            "-X",
            "sasl.username=t1",
            "-X",
            "sasl.password=not-a-token",
            "-L",
            "-m",
            "10",
        ],
        None,
    )
    .await;
    let text = stderr(&output);
    assert!(!output.status.success());
    assert!(text.contains("token rejected"), "{text}");
}

#[tokio::test]
async fn kcat_joining_a_group_exits_with_a_readable_error() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let started = Instant::now();
    let output = felix.kcat(&["-G", "kcat-group", TOPIC]).await;
    let text = format!("{}{}", stdout(&output), stderr(&output));
    eprintln!("kcat -G said:\n{text}");
    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("Felix has no Kafka consumer groups"),
        "{text}"
    );
    assert!(started.elapsed() < KCAT_BOUND);
}

#[tokio::test]
async fn kcat_producing_is_refused_with_a_reason() {
    if !kcat_available().await {
        return;
    }
    let felix = start().await;
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "-i"]);
    if cfg!(target_os = "linux") {
        command.args(["--network", "host"]);
    }
    let output = {
        use tokio::io::AsyncWriteExt;
        let mut child = command
            .args([
                KCAT_IMAGE,
                "-b",
                felix.anonymous.as_str(),
                "-P",
                "-t",
                TOPIC,
                "-p",
                "0",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn");
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(b"hello\n").await.expect("write");
        drop(stdin);
        tokio::time::timeout(KCAT_BOUND, child.wait_with_output())
            .await
            .expect("kcat -P finished")
            .expect("output")
    };
    let text = stderr(&output);
    assert!(!output.status.success(), "{text}");
    assert!(text.contains("Policy violation"), "{text}");
}
