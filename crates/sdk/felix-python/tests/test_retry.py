"""What the client will and will not re-send.

`retry.ambiguous_outcomes_are_not_silently_retried` is the scenario worth the
most care here, because getting it wrong changes the delivery guarantee without
anyone choosing to. It is asserted by construction rather than by fault
injection: the two calls differ only in `at_least_once`, and the binding's
default must be the one that does not duplicate.
"""

from __future__ import annotations

import inspect

import pytest

import felix


@pytest.mark.scenario("retry.ambiguous_outcomes_are_not_silently_retried")
def test_re_sending_a_publish_is_opt_in(client, fixture, key):
    """The default must not re-send; duplication has to be asked for.

    A publish that fails after the broker may already have written the record
    cannot be retried safely — nothing downstream can tell the copies apart.
    So the default is to report the failure, and re-sending is a separate,
    named choice.
    """
    import felix._felix as native

    signature = inspect.signature(native.Client.publish)
    parameter = signature.parameters.get("at_least_once")
    assert parameter is not None, (
        "there is no way to ask for re-sending, which means either it never "
        "happens or it always does; neither is a choice the caller made"
    )
    assert parameter.default is False, (
        "re-sending is the default: a publish that may already have been "
        "applied would be duplicated without the caller choosing at-least-once"
    )

    # And the opt-in path must actually work, or the choice is theoretical.
    client.publish(
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["durable_stream"],
        f"{key}-at-least-once".encode(),
        at_least_once=True,
    )


@pytest.mark.scenario("retry.transport_failures_are_retried_against_another_broker")
def test_a_dead_seed_does_not_stop_a_client_that_has_others(fixture, key):
    """An unreachable address among reachable ones must not fail the client.

    Retrying the one endpoint known not to answer is how a client spends its
    budget on the least promising option.
    """
    if len(fixture["addrs"]) < 2:
        pytest.skip("needs a multi-broker fixture")

    # A port nothing listens on, offered first.
    addresses = ["127.0.0.1:1", *fixture["addrs"]]
    with felix.Client(
        addresses,
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    ) as client:
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["durable_stream"],
            f"{key}-past-a-dead-seed".encode(),
        )
