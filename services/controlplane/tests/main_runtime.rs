use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct RunningControlplane {
    child: std::process::Child,
    main_addr: SocketAddr,
    bootstrap_addr: Option<SocketAddr>,
}

fn available_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve test port")
        .local_addr()
        .expect("read test port")
}

fn spawn_controlplane(bootstrap_enabled: bool) -> RunningControlplane {
    let main_addr = available_addr();
    let bootstrap_addr = available_addr();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"));
    cmd.env("FELIX_CONTROLPLANE_BIND", main_addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        .env("FELIX_CONTROLPLANE_STORAGE_BACKEND", "memory")
        .env(
            "FELIX_BOOTSTRAP_ENABLED",
            if bootstrap_enabled { "true" } else { "false" },
        )
        .env("FELIX_BOOTSTRAP_BIND_ADDR", bootstrap_addr.to_string())
        .env("FELIX_BOOTSTRAP_TOKEN", "bootstrap-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    RunningControlplane {
        child: cmd.spawn().expect("spawn controlplane"),
        main_addr,
        bootstrap_addr: bootstrap_enabled.then_some(bootstrap_addr),
    }
}

fn wait_for_listener(child: &mut std::process::Child, addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("check controlplane status") {
            panic!("controlplane exited before listening on {addr}: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "controlplane did not listen on {addr} within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn wait_until_ready(controlplane: &mut RunningControlplane) {
    let timeout = Duration::from_secs(5);
    wait_for_listener(&mut controlplane.child, controlplane.main_addr, timeout);
    if let Some(addr) = controlplane.bootstrap_addr {
        wait_for_listener(&mut controlplane.child, addr, timeout);
    }
}

fn stop_with_sigint(child: &mut std::process::Child) {
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid)
        .status()
        .expect("send SIGINT");
    assert!(status.success());
}

fn wait_for_exit(child: &mut std::process::Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill on timeout");
            return child.wait().expect("wait after kill");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn binary_starts_and_stops_on_sigint_without_bootstrap() {
    let mut controlplane = spawn_controlplane(false);
    wait_until_ready(&mut controlplane);
    stop_with_sigint(&mut controlplane.child);
    let status = wait_for_exit(&mut controlplane.child, Duration::from_secs(5));
    assert!(status.success(), "controlplane exited with {status}");
}

#[test]
fn binary_starts_and_stops_on_sigint_with_bootstrap() {
    let mut controlplane = spawn_controlplane(true);
    wait_until_ready(&mut controlplane);
    stop_with_sigint(&mut controlplane.child);
    let status = wait_for_exit(&mut controlplane.child, Duration::from_secs(5));
    assert!(status.success(), "controlplane exited with {status}");
}
