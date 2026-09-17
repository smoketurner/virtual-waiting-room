// forgetPosition() across a closed → running transition: the shipped
// waiting.js clears knownPosition on a "closed" tick and asks no
// /v1/queue_num that tick (the closed branch calls forgetPosition() then
// `return schedule()`), so the first running tick after a reopen re-asks —
// the behaviour the Rust harness mirrors in
// crates/harness/src/visitor.rs's `run()` and its test
// `backoff_forgets_position_across_close_so_re_asks_after_reopen`.
//
// Tests against the shipped waiting.js, loaded under node:vm (see
// vm-client.js for why — a file split that could be unit-tested directly
// cannot reach the bug these tests exist to catch, because the bug lives in
// this file's own closure state).
//
// Run with `node --test infra/modules/edge/tests`.

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { loadClient, jsonResponse } = require("./vm-client");

const POLICY = { floor_ms: 5000, ceiling_ms: 30000, divisor: 10 };

/** A `/v1/status` body for an open, active event. `participant_count: 0`
 *  zeroes scheduleFirstAsk's spread so /v1/queue_num is asked on the first
 *  eligible running tick rather than nondeterministically skipped by
 *  Math.random() — this test is about forgetPosition, not the arrival
 *  spread. */
function runningStatus() {
  return {
    event_id: "evt-1",
    phase: "active",
    serving_state: "running",
    serving_position: 90,
    participant_count: 0,
    target_rate: 100,
    poll_policy: POLICY,
  };
}

/** A `/v1/status` body for a closed event — an operator reset mid-session.
 *  `phase: "active"` (not "pre_queue") so the closed branch hits `return
 *  schedule()` rather than `return join().then(schedule)`, keeping the
 *  closed tick to one /v1/status and no /v1/queue_num. */
function closedStatus() {
  return {
    event_id: "evt-1",
    phase: "active",
    serving_state: "closed",
    serving_position: 90,
    poll_policy: POLICY,
  };
}

function queueNumCount(client) {
  return client.calls.filter((c) => c.url.includes("/v1/queue_num")).length;
}

test("a closed tick calls forgetPosition and asks no /v1/queue_num, so the first running tick after a reopen re-asks", async () => {
  let servingState = "running";
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/v1/status")) {
        return jsonResponse(
          200,
          servingState === "closed" ? closedStatus() : runningStatus()
        );
      }
      if (url.startsWith("/v1/queue_num")) {
        return jsonResponse(200, { position: 100, live_join: true });
      }
      return jsonResponse(200, {}); // /v1/join
    },
  });

  // Tick 1 — running: knownPosition is null and firstAskAt has elapsed (the
  // zero spread puts firstAskAt at now), so the chain asks /v1/queue_num and
  // caches the position.
  await client.flush();
  assert.equal(
    queueNumCount(client),
    1,
    "the first running tick asks /v1/queue_num once and caches the position"
  );

  // Tick 2 — closed: forgetPosition() clears knownPosition and the closed
  // branch returns schedule() without asking /v1/queue_num.
  servingState = "closed";
  client.clock.now += client.lastTimer().ms;
  client.fireLastTimer();
  await client.flush();
  assert.equal(
    queueNumCount(client),
    1,
    "the closed tick asks no /v1/queue_num (forgetPosition + return schedule)"
  );

  // Tick 3 — running again: knownPosition was forgotten, so the cache-hit
  // guard (knownPosition !== null) falls through and the chain re-asks
  // /v1/queue_num — exactly the re-ask the harness must mirror.
  servingState = "running";
  client.clock.now += client.lastTimer().ms;
  client.fireLastTimer();
  await client.flush();
  assert.equal(
    queueNumCount(client),
    2,
    "the first running tick after the reopen re-asks /v1/queue_num"
  );
});
