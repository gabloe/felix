"""Cluster semantics: redirects, discovery, and surviving a broker.

These are the scenarios a second client approximates rather than implements,
because none of them can be observed against a single broker on a happy path.
They need a shard whose owner is not the broker the client reached, and a
broker that goes away while the client is using it.
"""

from __future__ import annotations

import subprocess
import time

import pytest

import felix

TIMEOUT = 25.0


def drain(subscription, count, timeout=TIMEOUT):
    events = []
    while len(events) < count:
        event = subscription.next_event(timeout=timeout)
        if event is None:
            break
        events.append(event)
    return events


def connect_to(fixture, addr, token=None):
    return felix.Client(
        addr,
        tenant_id=fixture["tenant_id"],
        token=token or fixture["token"],
        ca_file=fixture["ca_file"],
    )


@pytest.mark.scenario("reconnect.discovers_brokers_from_one_seed")
def test_one_seed_is_enough(fixture):
    """A client given one address learns the rest of the cluster.

    Configured with a single broker and still knowing about the others is the
    difference between a cluster client and a single point of failure wearing
    a cluster's clothes.
    """
    if len(fixture["addrs"]) < 2:
        pytest.skip("needs a multi-broker fixture")

    with connect_to(fixture, fixture["addrs"][0]) as client:
        # Discovery happens during connect; the endpoints it would now try
        # include brokers it was never told about.
        endpoints = client.endpoints()

    assert len(endpoints) > 1, (
        f"the client knows only {endpoints}; it was given one seed and never "
        "learned the cluster, so losing that seed would end it"
    )


@pytest.mark.scenario(
    "redirect.follows_to_the_shard_owner",
    "redirect.is_not_reported_as_a_failure",
)
def test_a_subscription_through_any_broker_receives(fixture, key):
    """Every broker must serve a subscription, owner or not.

    The stream has several shards spread over the cluster, so for at least one
    broker this is a subscription to a shard it does not own. A client that
    surfaced the redirect would fail here for that broker and pass for the
    others — which is exactly the shape of bug that gets blamed on placement.
    """
    stream = fixture["durable_stream"]
    payload = f"redirect-{key}".encode()

    for addr in fixture["addrs"]:
        with connect_to(fixture, addr) as client:
            with client.subscribe(
                fixture["tenant_id"], fixture["namespace"], stream
            ) as events:
                client.publish(
                    fixture["tenant_id"], fixture["namespace"], stream, payload
                )
                received = drain(events, 1, timeout=15.0)
            assert received, f"no record arrived through broker {addr}"


@pytest.mark.scenario("redirect.carries_the_start_offset_through_every_hop")
def test_a_redirected_subscription_keeps_its_start_offset(fixture, key):
    """The subtle one: a redirect must not silently reset the start position.

    A client that rebuilds the subscribe request on redirect, instead of
    carrying the original, drops the offset and begins at the live tail. The
    call succeeds, nothing errors, and every record between the requested
    offset and now is simply absent — which is why this is asserted against
    records published *before* the subscription exists.
    """
    stream = fixture["durable_stream"]
    history = [f"{key}-h{index}".encode() for index in range(6)]

    # Publish the history first and learn where it starts.
    with connect_to(fixture, fixture["addrs"][0]) as writer:
        with writer.subscribe(
            fixture["tenant_id"], fixture["namespace"], stream
        ) as events:
            for payload in history:
                writer.publish(
                    fixture["tenant_id"], fixture["namespace"], stream, payload
                )
            observed = drain(events, len(history))

    mine = [event for event in observed if event.payload in history]
    assert len(mine) == len(history), "could not observe the history to resume into"
    first_offset = mine[0].offset
    assert first_offset is not None

    # Now resume from that offset through *every* broker. For the ones that do
    # not own the shard the request is redirected, and the offset has to
    # survive the hop.
    for addr in fixture["addrs"]:
        with connect_to(fixture, addr) as client:
            with client.subscribe(
                fixture["tenant_id"],
                fixture["namespace"],
                stream,
                start=first_offset,
            ) as resumed:
                replayed = drain(resumed, len(history), timeout=15.0)
        payloads = [event.payload for event in replayed]
        assert history[0] in payloads, (
            f"through broker {addr}, a subscription asked to start at offset "
            f"{first_offset} did not receive the record at that offset — the "
            "start position was lost on the way to the shard's owner"
        )


@pytest.mark.scenario("reconnect.survives_broker_loss")
def test_a_client_outlives_the_broker_it_was_using(disposable_fixture, key):
    """Kill the broker in use; the next call must still succeed.

    Deliberately kills the broker the client actually connected to rather than
    an arbitrary one — killing an unused broker proves nothing about
    reconnection.
    """
    fixture = disposable_fixture
    if len(fixture["addrs"]) < 2:
        pytest.skip("needs a multi-broker fixture")

    stream = fixture["durable_stream"]
    seed = fixture["addrs"][0]

    with connect_to(fixture, fixture["addrs"]) as client:
        client.publish(
            fixture["tenant_id"], fixture["namespace"], stream, f"{key}-before".encode()
        )

        killed = _kill_broker_on(seed)
        if not killed:
            pytest.skip(f"could not identify the broker process listening on {seed}")

        # Generous, because two things have to happen and only one is the
        # client's: it has to notice the broker is gone, and the cluster has to
        # give the dead broker's shards a new leader. The scenario is about the
        # client recovering at all, not about how fast the cluster reassigns.
        last_error = None
        for _ in range(60):
            try:
                client.publish(
                    fixture["tenant_id"],
                    fixture["namespace"],
                    stream,
                    f"{key}-after".encode(),
                )
                return
            except felix.FelixError as error:
                last_error = error
                time.sleep(1.0)

    pytest.fail(
        f"the client never recovered from losing {seed}: {last_error}. A client "
        "that cannot outlive one broker makes every caller handle failover."
    )


def _kill_broker_on(addr: str) -> bool:
    """Kill the felix-broker process listening on `addr`. True if one died."""
    port = addr.rsplit(":", 1)[1]
    try:
        listing = subprocess.run(
            ["lsof", "-nP", f"-iUDP:{port}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False

    for line in listing.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 2:
            continue
        try:
            pid = int(fields[1])
        except ValueError:
            continue
        subprocess.run(["kill", "-9", str(pid)], check=False)
        return True
    return False
