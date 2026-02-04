use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn spawn_controlplane(bootstrap_enabled: bool) -> std::process::Child {
    let bin = std::env::var("CARGO_BIN_EXE_felix-controlplane").unwrap_or_else(|_| {
        let current = std::env::current_exe().expect("current exe");
        let debug_dir = current
            .parent()
            .and_then(|p| p.parent())
            .expect("target debug dir");
        debug_dir
            .join("felix-controlplane")
            .to_string_lossy()
            .to_string()
    });
    let mut cmd = Command::new(bin);
    cmd.env("FELIX_CONTROLPLANE_BIND", "127.0.0.1:0")
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        .env("FELIX_CONTROLPLANE_STORAGE_BACKEND", "memory")
        .env(
            "FELIX_BOOTSTRAP_ENABLED",
            if bootstrap_enabled { "true" } else { "false" },
        )
        .env("FELIX_BOOTSTRAP_BIND_ADDR", "127.0.0.1:0")
        .env("FELIX_BOOTSTRAP_TOKEN", "bootstrap-token")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("spawn controlplane")
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
    let mut child = spawn_controlplane(false);
    std::thread::sleep(Duration::from_millis(250));
    stop_with_sigint(&mut child);
    let status = wait_for_exit(&mut child, Duration::from_secs(3));
    assert!(status.success());
}

#[test]
fn binary_starts_and_stops_on_sigint_with_bootstrap() {
    let mut child = spawn_controlplane(true);
    std::thread::sleep(Duration::from_millis(300));
    stop_with_sigint(&mut child);
    let status = wait_for_exit(&mut child, Duration::from_secs(3));
    assert!(status.success());
}
