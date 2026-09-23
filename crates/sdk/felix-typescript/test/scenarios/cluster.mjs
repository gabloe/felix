// Cluster semantics: redirects, discovery, and surviving a broker.
//
// These are the scenarios a second client approximates rather than implements,
// because none of them can be observed against a single broker on a happy path.
// They need a shard whose owner is not the broker the client reached, and a
// broker that goes away while the client is using it.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { it } from "node:test";

import { scenario, sleep, startFixture, take, unique, withClient } from "../harness.mjs";

const TIMEOUT = 25_000;

export default function register(ctx) {
  it("one seed address is enough", () =>
    scenario("reconnect.discovers_brokers_from_one_seed", async () => {
      // Configured with a single broker and still knowing about the others is
      // the difference between a cluster client and a single point of failure
      // wearing a cluster's clothes.
      const { fixture } = ctx;
      assert.ok(fixture.addrs.length > 1, "needs a multi-broker fixture");

      const endpoints = await withClient(fixture, { addrs: fixture.addrs[0] }, (client) =>
        // Discovery happens during connect; the endpoints it would now try
        // include brokers it was never told about.
        client.endpoints(),
      );

      assert.ok(
        endpoints.length > 1,
        `the client knows only ${endpoints}; it was given one seed and never ` +
          "learned the cluster, so losing that seed would end it",
      );
    }));

  it("a subscription through any broker receives, redirect or not", () =>
    scenario(
      ["redirect.follows_to_the_shard_owner", "redirect.is_not_reported_as_a_failure"],
      async () => {
        // The stream has several shards spread over the cluster, so for at
        // least one broker this is a subscription to a shard it does not own. A
        // client that surfaced the redirect would fail here for that broker and
        // pass for the others — exactly the shape of bug that gets blamed on
        // placement.
        const { fixture } = ctx;
        const stream = fixture.durable_stream;

        for (const addr of fixture.addrs) {
          const payload = unique("redirect");
          await withClient(fixture, { addrs: addr }, async (client) => {
            const events = await client.subscribe(fixture.tenant_id, fixture.namespace, stream);
            try {
              await client.publish(
                fixture.tenant_id,
                fixture.namespace,
                stream,
                Buffer.from(payload),
              );
              const received = await take(events, 1, 15_000);
              assert.ok(received.length > 0, `no record arrived through broker ${addr}`);
            } finally {
              await events.close();
            }
          });
        }
      },
    ));

  it("a redirected subscription keeps its requested start offset", () =>
    scenario("redirect.carries_the_start_offset_through_every_hop", async () => {
      // The subtle one. A client that rebuilds the subscribe request on
      // redirect, instead of carrying the original, drops the offset and begins
      // at the live tail. The call succeeds, nothing errors, and every record
      // between the requested offset and now is simply absent — which is why
      // this is asserted against records published *before* the subscription.
      const { fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("hop");
      const history = Array.from({ length: 6 }, (_, i) => `${prefix}-h${i}`);

      // Publish the history first and learn where it starts.
      const observed = await withClient(fixture, { addrs: fixture.addrs[0] }, async (writer) => {
        const events = await writer.subscribe(fixture.tenant_id, fixture.namespace, stream);
        try {
          for (const payload of history) {
            await writer.publish(
              fixture.tenant_id,
              fixture.namespace,
              stream,
              Buffer.from(payload),
            );
          }
          return await take(events, history.length, TIMEOUT);
        } finally {
          await events.close();
        }
      });

      const mine = observed.filter((event) => history.includes(event.payload.toString()));
      assert.equal(mine.length, history.length, "could not observe the history to resume into");
      const firstOffset = mine[0].offset;
      assert.equal(typeof firstOffset, "bigint");

      // Now resume from that offset through *every* broker. For the ones that
      // do not own the shard the request is redirected, and the offset has to
      // survive the hop.
      for (const addr of fixture.addrs) {
        const replayed = await withClient(fixture, { addrs: addr }, async (client) => {
          const resumed = await client.subscribe(
            fixture.tenant_id,
            fixture.namespace,
            stream,
            firstOffset,
          );
          try {
            return await take(resumed, history.length, 15_000);
          } finally {
            await resumed.close();
          }
        });
        assert.ok(
          replayed.map((event) => event.payload.toString()).includes(history[0]),
          `through broker ${addr}, a subscription asked to start at offset ` +
            `${firstOffset} did not receive the record at that offset — the ` +
            "start position was lost on the way to the shard's owner",
        );
      }
    }));

  it("a client outlives the broker it was using", () =>
    scenario("reconnect.survives_broker_loss", async () => {
      // Kill the broker the client actually connected to; the next call must
      // still succeed. Killing an unused broker would prove nothing about
      // reconnection.
      //
      // Its own cluster, because killing a broker is a legitimate thing to
      // assert against and a ruinous thing to do to the shared fixture: every
      // test after it would fail against a cluster someone else damaged, and
      // the failures would look like client bugs.
      const own = await startFixture();
      try {
        const fixture = own.fixture;
        assert.ok(fixture.addrs.length > 1, "needs a multi-broker fixture");
        const stream = fixture.durable_stream;
        const seed = fixture.addrs[0];
        const prefix = unique("survives");

        await withClient(fixture, {}, async (client) => {
          await client.publish(
            fixture.tenant_id,
            fixture.namespace,
            stream,
            Buffer.from(`${prefix}-before`),
          );

          assert.ok(killBrokerOn(seed), `could not identify the broker process listening on ${seed}`);

          // Generous, because two things have to happen and only one is the
          // client's: it has to notice the broker is gone, and the cluster has
          // to give the dead broker's shards a new leader. The scenario is
          // about the client recovering at all, not about how fast the cluster
          // reassigns — and on a loaded CI runner the reassignment is the
          // slower half by a wide margin.
          let last = null;
          const deadline = Date.now() + 180_000;
          while (Date.now() < deadline) {
            try {
              await client.publish(
                fixture.tenant_id,
                fixture.namespace,
                stream,
                Buffer.from(`${prefix}-after`),
              );
              return;
            } catch (err) {
              last = err;
              await sleep(1_000);
            }
          }
          assert.fail(
            `the client never recovered from losing ${seed} within 180s: ${last}. ` +
              "A client that cannot outlive one broker makes every caller handle " +
              "failover.",
          );
        });
      } finally {
        await own.stop();
      }
    }));

  it("a dead seed does not stop a client that has others", () =>
    scenario("retry.transport_failures_are_retried_against_another_broker", async () => {
      // Retrying the one endpoint known not to answer is how a client spends
      // its budget on the least promising option.
      const { fixture } = ctx;
      assert.ok(fixture.addrs.length > 1, "needs a multi-broker fixture");

      // A port nothing listens on, offered first.
      const addrs = ["127.0.0.1:1", ...fixture.addrs];
      await withClient(fixture, { addrs }, (client) =>
        client.publish(
          fixture.tenant_id,
          fixture.namespace,
          fixture.durable_stream,
          Buffer.from(unique("past-a-dead-seed")),
        ),
      );
    }));
}

/** Kill the felix-broker process listening on `addr`. True if one died. */
function killBrokerOn(addr) {
  const port = addr.slice(addr.lastIndexOf(":") + 1);
  let listing;
  try {
    listing = execFileSync("lsof", ["-nP", `-iUDP:${port}`], {
      encoding: "utf8",
      timeout: 15_000,
    });
  } catch (err) {
    // lsof exits non-zero when nothing matches, and may be absent entirely.
    listing = err?.stdout ?? "";
  }

  for (const line of listing.split("\n").slice(1)) {
    const fields = line.split(/\s+/);
    if (fields.length < 2) continue;
    const pid = Number.parseInt(fields[1], 10);
    if (!Number.isInteger(pid)) continue;
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // Already gone is the outcome we wanted.
    }
    return true;
  }
  return false;
}
