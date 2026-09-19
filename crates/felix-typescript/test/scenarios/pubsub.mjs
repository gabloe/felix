// Publish and subscribe, against a real cluster.
//
// Each test names the conformance scenario it demonstrates. The names are what
// turn this file into evidence rather than assertion: a scenario with no
// passing test here is reported missing by `felix-conformance verify`.

import assert from "node:assert/strict";
import { it } from "node:test";

import { scenario, take, unique } from "../harness.mjs";

const TIMEOUT = 20_000;

/** The payloads of `events`, as strings, for readable assertions. */
const texts = (events) => events.map((event) => event.payload.toString());

export default function register(ctx) {
  it("a published record reaches a subscriber", () =>
    scenario("pubsub.roundtrip", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const payload = Buffer.from(unique("roundtrip"));

      const events = await client.subscribe(fixture.tenant_id, fixture.namespace, stream);
      try {
        await client.publish(fixture.tenant_id, fixture.namespace, stream, payload);
        const received = await take(events, 1, TIMEOUT);
        assert.ok(received.length > 0, "nothing arrived");
        // Byte for byte: a client that coerces to text or wraps in JSON fails
        // here, which is the point of asserting on bytes rather than on a
        // decoded value.
        assert.ok(Buffer.isBuffer(received[0].payload));
        assert.deepEqual(received[0].payload, payload);
      } finally {
        await events.close();
      }
    }));

  it("records arrive in publish order", () =>
    scenario("pubsub.order", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("order");
      const payloads = Array.from({ length: 20 }, (_, i) => `${prefix}-${i}`);

      const events = await client.subscribe(fixture.tenant_id, fixture.namespace, stream);
      try {
        for (const payload of payloads) {
          await client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from(payload));
        }
        const received = await take(events, payloads.length, TIMEOUT);
        const mine = texts(received).filter((text) => text.startsWith(prefix));
        assert.deepEqual(mine, payloads, "records were reordered");
      } finally {
        await events.close();
      }
    }));

  it("delivered offsets increase by one", () =>
    scenario("pubsub.offsets_are_contiguous", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("offsets");
      const payloads = Array.from({ length: 10 }, (_, i) => `${prefix}-${i}`);

      const events = await client.subscribe(fixture.tenant_id, fixture.namespace, stream);
      let received;
      try {
        for (const payload of payloads) {
          await client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from(payload));
        }
        received = await take(events, payloads.length, TIMEOUT);
      } finally {
        await events.close();
      }

      const offsets = received.map((event) => event.offset);
      assert.ok(
        offsets.length > 1 && offsets.every((offset) => typeof offset === "bigint"),
        "a durable stream must deliver offsets; without them an application " +
          "cannot detect a drop or resume after one",
      );
      for (let i = 1; i < offsets.length; i++) {
        assert.equal(
          offsets[i],
          offsets[i - 1] + 1n,
          `offsets jumped: ${offsets[i - 1]} then ${offsets[i]}`,
        );
      }
    }));

  it("a subscription resumes at the next offset, with no gap and no duplicate", () =>
    scenario(
      ["pubsub.resume_from_offset", "reconnect.subscription_resumes_at_the_next_offset"],
      async () => {
        // The single most expensive client bug is off-by-one here — one
        // direction loses records silently, the other duplicates them — so
        // this asserts both directions rather than only that something arrived.
        const { client, fixture } = ctx;
        const stream = fixture.durable_stream;
        const prefix = unique("resume");
        const first = Array.from({ length: 5 }, (_, i) => `${prefix}-a${i}`);
        const second = Array.from({ length: 5 }, (_, i) => `${prefix}-b${i}`);

        const events = await client.subscribe(fixture.tenant_id, fixture.namespace, stream);
        let received;
        try {
          for (const payload of first) {
            await client.publish(
              fixture.tenant_id,
              fixture.namespace,
              stream,
              Buffer.from(payload),
            );
          }
          received = await take(events, first.length, TIMEOUT);
        } finally {
          await events.close();
        }

        const mine = received.filter((event) => first.includes(event.payload.toString()));
        assert.ok(mine.length > 0, "nothing to resume from");
        const lastHandled = mine.at(-1).offset;
        assert.equal(typeof lastHandled, "bigint");

        // Published while nothing is subscribed: these exist only in the log,
        // so a resume that silently started at the tail would miss all of them.
        for (const payload of second) {
          await client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from(payload));
        }

        const resumed = await client.subscribe(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          lastHandled + 1n,
        );
        let after;
        try {
          after = await take(resumed, second.length, TIMEOUT);
        } finally {
          await resumed.close();
        }

        const payloads = texts(after);
        for (const payload of payloads) {
          assert.ok(!first.includes(payload), `a record already handled was delivered again: ${payload}`);
        }
        for (const payload of second) {
          assert.ok(payloads.includes(payload), `resume skipped ${payload}`);
        }
      },
    ));

  it("a subscription can start at the oldest retained record", () =>
    scenario("pubsub.start_earliest", async () => {
      // `earliest` is "as far back as you can", not offset 0. A trimmed log has
      // no offset 0 to ask for, and asking is an error — which is why this is a
      // separate start position rather than a number a caller could have
      // written themselves.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const payload = unique("earliest");
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(payload),
        undefined,
        "per_message",
      );

      const events = await client.subscribe(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        "earliest",
      );
      let received;
      try {
        received = await take(events, 500, 5_000);
      } finally {
        await events.close();
      }

      const payloads = texts(received);
      assert.ok(
        payloads.includes(payload),
        "a subscription starting at the earliest retained record did not " +
          "replay history, so it started somewhere else",
      );
      // History, not just the live tail: the record was published before the
      // subscription existed.
      assert.ok(payloads.length > 1);
    }));

  it("an acknowledged publish returns only after the broker accepted it", () =>
    scenario("ack.per_message_means_the_broker_accepted_it", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const payload = unique("acked");

      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(payload),
        undefined,
        "per_message",
      );

      // The acknowledgement is the claim that the broker has the record, so it
      // must be readable from the log immediately afterwards — without any
      // further wait on the test's part.
      const events = await client.subscribe(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        "earliest",
      );
      let received;
      try {
        received = await take(events, 500, 5_000);
      } finally {
        await events.close();
      }
      assert.ok(
        texts(received).includes(payload),
        "a record the broker acknowledged was not in the log",
      );
    }));

  it("an unacknowledged publish does not wait for the broker", () =>
    scenario("ack.none_does_not_wait_for_the_broker", async () => {
      // Timing is the only observable difference, so this asserts a bound loose
      // enough not to flake on a loaded machine and tight enough to catch a
      // client that waits for an acknowledgement it was told not to want.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("unacked");

      // Warm the connection so the measurement is not dominated by setup.
      await client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from("warm"));

      const started = process.hrtime.bigint();
      for (let i = 0; i < 20; i++) {
        await client.publish(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          Buffer.from(`${prefix}-${i}`),
          undefined,
          "none",
        );
      }
      const elapsed = Number(process.hrtime.bigint() - started) / 1e9;

      assert.ok(
        elapsed < 2.0,
        `20 unacknowledged publishes took ${elapsed.toFixed(2)}s; a client that ` +
          "waits for the broker has turned the cheapest ack mode into the dearest",
      );
    }));
}
