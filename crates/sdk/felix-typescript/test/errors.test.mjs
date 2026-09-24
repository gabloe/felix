// How a native error becomes a typed one. No addon and no cluster: the
// messages below are the ones `src/errors.rs` writes, and its own tests pin
// that side of the format.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const errors = require("../errors.js");

function native(message) {
  return new Error(message);
}

test("a broker code picks the class and rides along", () => {
  const err = errors.typed(
    native(
      'FELIX_SHARD_UNAVAILABLE {"code":"shard_unavailable","detail":{"reason":"fenced"},"retry":"retry"}\n' +
        "publish failed: shard is fenced",
    ),
  );
  assert.ok(err instanceof errors.ShardUnavailableError);
  assert.ok(err instanceof errors.FelixError);
  assert.equal(err.code, "FELIX_SHARD_UNAVAILABLE");
  assert.equal(err.brokerCode, "shard_unavailable");
  assert.equal(err.retry, "retry");
  assert.deepEqual(err.detail, { reason: "fenced" });
  assert.equal(err.retryable, true);
  assert.equal(err.message, "publish failed: shard is fenced");
});

test("an outcome-unknown error is not retryable", () => {
  const err = errors.typed(
    native('FELIX_OUTCOME_UNKNOWN {"code":"quorum_timeout","retry":"outcome_unknown"}\nquorum'),
  );
  assert.ok(err instanceof errors.OutcomeUnknownError);
  assert.equal(err.brokerCode, "quorum_timeout");
  assert.equal(err.retryable, false);
});

test("the broker's retry class decides retryable over the class default", () => {
  // `not_found` is sent as retry_after: a promoted broker may not know the
  // stream yet.
  const err = errors.typed(
    native('FELIX_NOT_FOUND {"code":"not_found","retry":"retry_after"}\nunknown stream'),
  );
  assert.ok(err instanceof errors.NotFoundError);
  assert.equal(err.retryable, true);
});

test("without a code the old prefix still types it", () => {
  const err = errors.typed(native("FELIX_AUTH: publish failed: forbidden: a: b"));
  assert.ok(err instanceof errors.AuthError);
  assert.equal(err.code, "FELIX_AUTH");
  assert.equal(err.brokerCode, null);
  assert.equal(err.retry, null);
  assert.equal(err.detail, null);
  assert.equal(err.retryable, false);
  assert.equal(err.message, "publish failed: forbidden: a: b");

  const lost = errors.typed(native("FELIX_CONNECTION: connection lost"));
  assert.ok(lost instanceof errors.ConnectionError);
  assert.equal(lost.retryable, true);
});

test("anything else is left alone", () => {
  const plain = native("something else: entirely");
  assert.equal(errors.typed(plain), plain);
  const unknownKind = native("FELIX_FROM_THE_FUTURE: text");
  assert.equal(errors.typed(unknownKind), unknownKind);
  assert.equal(errors.typed(undefined), undefined);
});
