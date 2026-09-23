//! The real binary on the Raft backend: a single-member group serving the
//! HTTP API with no external database, and keeping its metadata across a
//! restart — the property the data directory exists to provide.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use felix_controlplane_service::store::command::{MetaCommand, encode_command};
use felix_controlplane_service::store::memory::{InMemoryStore, export_state_from};
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&[u8]>,
) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(bearer) = bearer {
        request.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
    }
    let mut payload = Vec::new();
    match body {
        Some(body) => {
            request.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            ));
            payload.extend_from_slice(body);
        }
        None => request.push_str("\r\n"),
    }
    stream.write_all(request.as_bytes()).ok()?;
    stream.write_all(&payload).ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, text))
}

fn spawn(addr: SocketAddr, data_dir: &std::path::Path) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_felix-controlplane"))
        .env("FELIX_CONTROLPLANE_BIND", addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        // The three raft variables select the backend on their own, the same
        // way a postgres URL selects postgres.
        .env("FELIX_RAFT_NODE_ID", "1")
        .env("FELIX_RAFT_DATA_DIR", data_dir)
        .env("FELIX_RAFT_PEERS", format!("1={addr}"))
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn controlplane")
}

fn wait_ready(child: &mut std::process::Child, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((200, _)) = http(addr, "GET", "/v1/system/ready", None, None) {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("controlplane exited during startup: {status}");
        }
        assert!(Instant::now() < deadline, "never became ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn stop(child: &mut std::process::Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().expect("try_wait").is_none() {
        assert!(Instant::now() < deadline, "did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn the_binary_serves_and_survives_a_restart_with_no_database() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("addr");
    let data_dir = tempfile::tempdir().expect("tempdir");

    let mut first = spawn(addr, data_dir.path());
    wait_ready(&mut first, addr);

    // The catalog API needs an operator credential, and a fresh binary has no
    // tenant to mint one from. Seed one through the import path, the same
    // way a migration would, then mint against the keys it was given.
    let bearer = seed_operator(addr);

    let (status, _) = http(
        addr,
        "POST",
        "/v1/tenants",
        Some(&bearer),
        Some(br#"{"tenant_id": "t1", "display_name": "Tenant One"}"#),
    )
    .expect("create tenant");
    assert_eq!(status, 201, "create through the HTTP API on raft");

    stop(&mut first);

    // Same data directory: the metadata must come back from the Raft log
    // and snapshot — there is no database holding it.
    let mut second = spawn(addr, data_dir.path());
    wait_ready(&mut second, addr);
    let (status, body) =
        http(addr, "GET", "/v1/tenants", Some(&bearer), None).expect("list tenants");
    assert_eq!(status, 200);
    assert!(
        body.contains("\"t1\""),
        "metadata survived the restart without a database: {body}"
    );

    stop(&mut second);
}

/// Import an `ops` tenant with known keys and return an operator bearer.
fn seed_operator(addr: SocketAddr) -> String {
    let private = [3u8; 32];
    let signing = ed25519_dalek::SigningKey::from_bytes(&private);
    let keys = felix_controlplane_service::auth::felix_token::TenantSigningKeys {
        current: felix_controlplane_service::auth::felix_token::SigningKey {
            kid: "runtime-kid".to_string(),
            alg: jsonwebtoken::Algorithm::EdDSA,
            private_key: private,
            public_key: signing.verifying_key().to_bytes(),
        },
        previous: Vec::new(),
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let exported = runtime.block_on(async {
        let seed = InMemoryStore::new(StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        });
        seed.create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: "ops".to_string(),
            display_name: "Operators".to_string(),
        })
        .await
        .expect("tenant");
        seed.set_tenant_signing_keys("ops", keys.clone())
            .await
            .expect("keys");
        export_state_from(&seed).await.expect("export")
    });
    let import = encode_command(&MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: false,
    });
    let (status, _) =
        http(addr, "POST", "/internal/raft/propose", None, Some(&import)).expect("propose import");
    assert_eq!(status, 200, "import the operator tenant");

    felix_controlplane_service::auth::felix_token::mint_token(
        &keys,
        "ops",
        "p:operator",
        vec!["tenant.manage:cluster:*".to_string()],
        Duration::from_secs(3_600),
    )
    .expect("token")
}
