"""A real cluster for the tests to talk to.

Deliberately not a mock. The claim this binding makes is that Python gets the
*same* client behaviour Rust does — reconnection, redirect-following, offset
accounting — and a mock would only validate the mock. So these tests start the
real thing: `felix-cluster up` runs a control plane and brokers, writes their
addresses and a working token to a session file, and the tests connect over
real QUIC with a real credential.

Skipped rather than failed when the binaries are not built, so a checkout
without a Rust toolchain still collects.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SESSION_WAIT_SECONDS = 90


def _require_binaries() -> None:
    for binary in ("felix-broker", "felix-cluster"):
        if not (REPO_ROOT / "target" / "debug" / binary).exists():
            pytest.skip(
                f"no {binary} binary — build with "
                "`cargo build -p broker --bin felix-broker && "
                "cargo build -p felix-cluster --bin felix-cluster`"
            )


@pytest.fixture(scope="session")
def cluster(tmp_path_factory) -> dict:
    """Start a one-node cluster, yield its session, tear it down afterwards."""
    _require_binaries()
    work = tmp_path_factory.mktemp("felix-cluster")
    session_path = work / "session.json"
    ca_file = work / "broker-ca.pem"

    env = {
        **os.environ,
        # Every broker the harness starts exports the certificate it generated,
        # so the Python client can trust it by name rather than skipping
        # verification. One node, so one certificate.
        "FELIX_TLS_CERT_EXPORT": str(ca_file),
        "RUST_LOG": "warn",
    }

    process = subprocess.Popen(
        [
            str(REPO_ROOT / "target" / "debug" / "felix-cluster"),
            "up",
            "--nodes",
            "1",
            "--session",
            str(session_path),
        ],
        env=env,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    deadline = time.monotonic() + SESSION_WAIT_SECONDS
    session = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            pytest.fail(f"cluster exited during startup:\n{output}")
        if session_path.exists() and ca_file.exists():
            try:
                session = json.loads(session_path.read_text())
            except json.JSONDecodeError:
                # Being written right now; look again.
                time.sleep(0.2)
                continue
            if session.get("nodes"):
                break
        time.sleep(0.2)

    if session is None or not session.get("nodes"):
        process.terminate()
        pytest.fail(f"cluster did not become usable within {SESSION_WAIT_SECONDS}s")

    yield {
        "addrs": [node["client_addr"] for node in session["nodes"]],
        "ca_file": str(ca_file),
        "tenant_id": session["tenant_id"],
        "namespace": session["namespace"],
        "token": session["client_token"],
    }

    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()


@pytest.fixture
def client(cluster):
    """A connected synchronous client, closed after the test."""
    import felix

    with felix.Client(
        cluster["addrs"],
        tenant_id=cluster["tenant_id"],
        token=cluster["token"],
        ca_file=cluster["ca_file"],
    ) as connected:
        yield connected


@pytest.fixture
def stream_name(request) -> str:
    """A stream name unique to the test, so tests cannot read each other's records."""
    return f"py-{request.node.name.replace('[', '-').replace(']', '')}"
