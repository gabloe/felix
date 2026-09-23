use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct RunningControlplane {
    child: std::process::Child,
    main_addr: SocketAddr,
    bootstrap_addr: Option<SocketAddr>,
}

/// Two free ports, both held while they are chosen.
///
/// Binding port 0 and reading the address back is the only way to find a free
/// port, and it is a reservation the process gives up the moment the listener
/// drops. Holding both at once makes the two distinct by construction rather
/// than by the OS happening not to repeat itself.
///
/// **This does not make the port safe.** Between the drop here and the child's
/// bind, any other process — another test binary in the same cargo run — can
/// take it, and the child exits. Closing that would mean the child choosing its
/// own port and reporting it back, which is a bigger change than this harness
/// justifies. What matters is that when it does happen, the failure now says
/// so: see `child_stderr`.
fn reserve_two_addrs() -> (SocketAddr, SocketAddr) {
    let first = TcpListener::bind("127.0.0.1:0").expect("reserve test port");
    let second = TcpListener::bind("127.0.0.1:0").expect("reserve test port");
    let addrs = (
        first.local_addr().expect("read test port"),
        second.local_addr().expect("read test port"),
    );
    assert_ne!(addrs.0, addrs.1, "two reservations returned one port");
    addrs
}

fn spawn_controlplane(bootstrap_enabled: bool) -> RunningControlplane {
    // Nothing routes to these instances, so the load-balancer hold-off is pure
    // waiting. Tests that are about the hold-off ask for one explicitly.
    spawn_controlplane_with_predrain(bootstrap_enabled, 0)
}

fn spawn_controlplane_with_predrain(
    bootstrap_enabled: bool,
    predrain_ms: u64,
) -> RunningControlplane {
    let (main_addr, bootstrap_addr) = reserve_two_addrs();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_felix-controlplane"));
    cmd.env("FELIX_CONTROLPLANE_BIND", main_addr.to_string())
        .env("FELIX_CONTROLPLANE_METRICS_BIND", "127.0.0.1:0")
        .env("FELIX_CONTROLPLANE_STORAGE_BACKEND", "memory")
        .env("FELIX_SHUTDOWN_PREDRAIN_MS", predrain_ms.to_string())
        .env(
            "FELIX_BOOTSTRAP_ENABLED",
            if bootstrap_enabled { "true" } else { "false" },
        )
        .env("FELIX_BOOTSTRAP_BIND_ADDR", bootstrap_addr.to_string())
        .env("FELIX_BOOTSTRAP_TOKEN", "bootstrap-token")
        .stdout(Stdio::null())
        // Kept, not discarded. When the child exits before listening, this is
        // the only thing that says why — and "it exited" with no reason is how
        // a bind collision reads as an unexplained flake.
        .stderr(Stdio::piped());
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
            panic!(
                "controlplane exited before listening on {addr}: {status}\n{}",
                child_stderr(child),
            );
        }
        assert!(
            Instant::now() < deadline,
            "controlplane did not listen on {addr} within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Whatever the child wrote to stderr before it died.
///
/// Read only on the failure path, because taking the pipe closes it for
/// anything after — and on the failure path there is nothing after.
fn child_stderr(child: &mut std::process::Child) -> String {
    use std::io::Read;
    let Some(mut stderr) = child.stderr.take() else {
        return "(stderr already taken)".to_string();
    };
    let mut buffer = String::new();
    match stderr.read_to_string(&mut buffer) {
        Ok(_) if buffer.trim().is_empty() => "(the child wrote nothing to stderr)".to_string(),
        Ok(_) => buffer,
        Err(err) => format!("(could not read stderr: {err})"),
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

/// Wait until the API actually answers, not merely until the port accepts.
///
/// `wait_for_listener` returns as soon as a connection is accepted, which
/// happens before the router is answering. That gap is invisible on a fast
/// machine and several hundred milliseconds wide under coverage
/// instrumentation, where it made the readiness test fail on its very first
/// assertion.
fn wait_until_serving(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if get(addr, "/v1/system/ready") == Some(200) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "controlplane did not answer /v1/system/ready with 200 on {addr} within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Send SIGTERM, which is what Kubernetes, systemd and `docker stop` use. The
/// tests above use SIGINT; the drain path is the same, but a rolling deploy
/// sends this one and it is worth exercising the signal that will actually
/// arrive.
fn signal_terminate(child: &std::process::Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
}

/// A one-shot HTTP/1.1 GET returning just the status code, or `None` if the
/// connection could not be made. Hand-rolled because the distinction this file
/// needs is exactly "refused the connection" versus "answered with a status",
/// and a client that retries or pools would blur it.
fn get(addr: SocketAddr, path: &str) -> Option<u16> {
    use std::io::{Read, Write};

    // Generous, because this also runs under coverage instrumentation where the
    // process is several times slower. A timeout here reads as "refused", which
    // is the one answer this file must not get wrong.
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .ok()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let head = String::from_utf8_lossy(&response);
    head.split_whitespace().nth(1)?.parse().ok()
}

/// **The ordering the whole drain rests on.** Readiness must fail *while the
/// listener is still accepting*, so a load balancer takes this instance out
/// before anything is refused at the socket.
///
/// The assertion is deliberately two-sided: 503 alone would also be produced by
/// a process that had already stopped answering, and that is the failure this
/// is meant to rule out. Liveness answering 200 on the same connection proves
/// the server is still there and chose to report itself unready.
#[test]
fn readiness_fails_before_the_listener_closes() {
    // Long enough that the observation is not a race, short enough that a
    // regression fails the test rather than hanging it.
    let mut controlplane = spawn_controlplane_with_predrain(false, 3_000);
    wait_until_ready(&mut controlplane);
    let addr = controlplane.main_addr;

    // The premise of everything below: it is serving, and reporting itself
    // ready, before the signal arrives.
    wait_until_serving(addr, Duration::from_secs(20));

    signal_terminate(&controlplane.child);

    // Poll rather than sleep: the flip is the first thing the shutdown path
    // does, but "first" is still asynchronous.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut ready_status = None;
    while Instant::now() < deadline {
        match get(addr, "/v1/system/ready") {
            Some(status) if status != 200 => {
                ready_status = Some(status);
                break;
            }
            // Connection refused: the listener closed before we ever observed
            // the flip, which is the bug this test exists for.
            None => break,
            _ => std::thread::sleep(Duration::from_millis(10)),
        }
    }

    assert_eq!(
        ready_status,
        Some(503),
        "readiness never reported draining while the listener was still open",
    );
    assert_eq!(
        get(addr, "/v1/system/live"),
        Some(200),
        "liveness should still answer during a drain: restarting a draining \
         process is the one thing a liveness probe must not do",
    );

    let status = wait_for_exit(&mut controlplane.child, Duration::from_secs(30));
    assert!(status.success(), "controlplane exited with {status}");
}

/// Shutdown finishes well inside the configured grace period rather than
/// running to the deadline and being cut off.
#[test]
fn shutdown_completes_inside_the_drain_budget() {
    let mut controlplane = spawn_controlplane(false);
    wait_until_ready(&mut controlplane);

    let started = Instant::now();
    signal_terminate(&controlplane.child);
    let status = wait_for_exit(&mut controlplane.child, Duration::from_secs(30));

    assert!(status.success(), "controlplane exited with {status}");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "an idle instance took {:?} to drain, against a 25s budget — it is \
         waiting out the deadline rather than finishing",
        started.elapsed(),
    );
}

/// A second signal means "stop waiting". Without it, an operator who wants the
/// process gone now has to wait out a hold-off sized for a load balancer that,
/// in a manual restart, is not watching.
#[test]
fn a_second_signal_cuts_the_hold_off_short() {
    // A hold-off far longer than the assertion below, so finishing quickly can
    // only be the escape hatch and not the timer elapsing.
    let mut controlplane = spawn_controlplane_with_predrain(false, 60_000);
    wait_until_ready(&mut controlplane);
    wait_until_serving(controlplane.main_addr, Duration::from_secs(20));

    signal_terminate(&controlplane.child);
    // The first signal must be observed before the second arrives, or there is
    // no hold-off in progress for it to cut short.
    let deadline = Instant::now() + Duration::from_secs(5);
    while get(controlplane.main_addr, "/v1/system/ready") == Some(200) {
        assert!(Instant::now() < deadline, "never began draining");
        std::thread::sleep(Duration::from_millis(10));
    }

    let started = Instant::now();
    signal_terminate(&controlplane.child);
    let status = wait_for_exit(&mut controlplane.child, Duration::from_secs(15));

    assert!(status.success(), "controlplane exited with {status}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "second signal did not cut the 60s hold-off short: exit took {:?}",
        started.elapsed(),
    );
}
