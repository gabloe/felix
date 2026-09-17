"""Error classification and lifecycle.

The scenarios here are about what an application can *decide* from a failure.
A client that reports everything as one error type forces callers to match on
message text, which breaks the first time a message is reworded — so these
assert on exception identity, never on wording.
"""

from __future__ import annotations

import concurrent.futures

import pytest

import felix


@pytest.mark.scenario("error.unknown_stream_is_typed")
def test_publishing_to_an_unknown_stream_is_distinguishable(client, fixture):
    with pytest.raises(felix.FelixError) as caught:
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["missing_stream"],
            b"nowhere",
        )

    # Distinguishable from a transport failure, which is the decision this
    # supports: an unknown stream will not start existing because you retried.
    assert not isinstance(caught.value, felix.ConnectionError), (
        "an unknown stream was reported as a connection failure, which tells "
        "an application to retry something that cannot succeed"
    )


@pytest.mark.scenario(
    "error.unauthorized_is_typed",
    "retry.terminal_failures_are_not_retried",
)
def test_an_unauthorized_publish_is_distinguishable_and_not_retried(fixture):
    """A token without publish permission fails as auth, promptly.

    Promptly is half the assertion: a client that retried this would take its
    full backoff budget to arrive at the same answer, so the elapsed time is
    evidence that it did not.
    """
    import time

    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["unauthorized_token"],
        ca_file=fixture["ca_file"],
    ) as restricted:
        started = time.monotonic()
        with pytest.raises(felix.FelixError) as caught:
            restricted.publish(
                fixture["tenant_id"],
                fixture["namespace"],
                fixture["durable_stream"],
                b"not allowed",
            )
        elapsed = time.monotonic() - started

    assert isinstance(caught.value, felix.AuthError), (
        f"an authorization failure surfaced as {type(caught.value).__name__}; "
        "an application cannot tell it from a retryable fault"
    )
    assert elapsed < 5.0, (
        f"the refusal took {elapsed:.2f}s — it was retried, and no amount of "
        "retrying grants a permission"
    )


@pytest.mark.scenario("client.close_is_idempotent")
def test_closing_twice_is_safe(client, fixture):
    """Cleanup runs on paths that may already have cleaned up."""
    subscription = client.subscribe(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    )
    subscription.close()
    assert subscription.closed
    subscription.close()  # must not raise
    assert subscription.closed

    # And through the context manager, over an already-closed subscription.
    with subscription:
        pass

    # A client's own exit is idempotent for the same reason.
    with client:
        pass


@pytest.mark.scenario("client.concurrent_calls")
def test_one_client_serves_concurrent_callers(client, fixture, key):
    """Threads share a client; every publish must land, none may corrupt.

    The binding blocks with the GIL released, so this is genuine concurrency
    rather than interleaved bytecode — which is exactly the condition under
    which shared mutable state would show.
    """
    stream = fixture["durable_stream"]
    payloads = [f"{key}-{index}".encode() for index in range(24)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        errors = list(
            pool.map(
                lambda payload: _publish_capturing(client, fixture, stream, payload),
                payloads,
            )
        )
    assert [error for error in errors if error] == []

    with client.subscribe(
        fixture["tenant_id"], fixture["namespace"], stream, start="earliest"
    ) as events:
        seen = set()
        while True:
            event = events.next_event(timeout=5.0)
            if event is None:
                break
            seen.add(event.payload)

    missing = [payload for payload in payloads if payload not in seen]
    assert not missing, f"{len(missing)} concurrent publishes were lost"


def _publish_capturing(client, fixture, stream, payload):
    try:
        client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        return None
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"{payload!r}: {error}"
