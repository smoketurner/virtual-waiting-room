// Adaptive poll interval (#69, ADR-0023): tests against the shipped
// waiting.js, loaded under node:vm (see vm-client.js for why — a file split
// that could be unit-tested directly cannot reach the bug these tests exist
// to catch, because the bug lives in this file's own closure state).
//
// Run with `node --test infra/modules/edge/tests`.

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { loadClient, jsonResponse } = require("./vm-client");

const FLOOR = 5000;
const CEILING = 30000;
const DIVISOR = 10;
const POLICY = { floor_ms: FLOOR, ceiling_ms: CEILING, divisor: DIVISOR };

/**
 * A `/v1/status` body for an open, active event. `participant_count: 0`
 * keeps `scheduleFirstAsk`'s spread window at exactly zero, so `/v1/queue_num`
 * is always asked on the first eligible tick rather than nondeterministically
 * skipped by `Math.random()` — these tests are about the poll interval, not
 * the arrival spread, which is covered separately in `crates/harness`.
 */
function activeStatus(overrides) {
  return Object.assign(
    {
      event_id: "evt-1",
      phase: "active",
      serving_state: "running",
      serving_position: 90,
      participant_count: 0,
      target_rate: 100,
    },
    overrides
  );
}

/**
 * Routes `/v1/status` to `statusBody` (or `statusBody()` if given a
 * function, so a test can vary it call to call), answers `/v1/queue_num`
 * with `position`, and accepts `/v1/join` unconditionally.
 */
function route(statusBody, position) {
  return (url) => {
    if (url.startsWith("/v1/status")) {
      return jsonResponse(200, typeof statusBody === "function" ? statusBody() : statusBody);
    }
    if (url.startsWith("/v1/queue_num")) {
      return jsonResponse(200, { position, live_join: true });
    }
    return jsonResponse(200, {}); // /v1/join
  };
}

test("a first-poll rejection, before any success, schedules at the floor", async () => {
  const client = loadClient({ route: () => Promise.reject(new Error("origin down")) });
  await client.flush();

  const live = client.liveTimers();
  assert.equal(live.length, 1, "the catch handler must schedule exactly one retry");
  assert.ok(Number.isFinite(live[0].ms), `retry delay must be a number, got ${live[0].ms}`);
  assert.ok(live[0].ms >= FLOOR, `retry delay must be >= the floor, got ${live[0].ms}`);
});

test("the client claims no place while /status keeps failing", async () => {
  const client = loadClient({ route: () => Promise.reject(new Error("origin down")) });
  await client.flush();
  assert.equal(client.calls.filter((c) => c.url.includes("/v1/join")).length, 0);
});

test("criterion 3: a near-front visitor with a policy still polls at the floor", async () => {
  // ahead=10 at rate=100/s is a 0.1s wait — intervalFor's clamp keeps it at
  // the floor, not the near-zero value the raw formula would give.
  const client = loadClient({
    route: route(activeStatus({ poll_policy: POLICY }), 100),
  });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected a floor-ish interval, got ${ms}`);
});

test("criterion 3: a deep-queue visitor with a policy is capped at the ceiling", async () => {
  // ahead=999,910 at rate=100/s is a ~2.8-hour wait — intervalFor's clamp
  // keeps it at the ceiling, not the far larger raw value.
  const client = loadClient({
    route: route(activeStatus({ poll_policy: POLICY }), 1_000_000),
  });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(
    ms >= CEILING && ms <= CEILING * 1.3,
    `expected a ceiling-ish interval, got ${ms}`
  );
});

const invalidPolicies = [
  ["absent", undefined],
  ["null", null],
  ["an array", []],
  ["a string", "5000,30000,10"],
  ["a number", 5000],
  ["missing a field", { floor_ms: FLOOR, ceiling_ms: CEILING }],
  ["a non-numeric field", { floor_ms: "5000", ceiling_ms: CEILING, divisor: DIVISOR }],
  ["a NaN field", { floor_ms: Number.NaN, ceiling_ms: CEILING, divisor: DIVISOR }],
  ["a negative field", { floor_ms: -1, ceiling_ms: CEILING, divisor: DIVISOR }],
  ["a zero divisor", { floor_ms: FLOOR, ceiling_ms: CEILING, divisor: 0 }],
  ["an inverted clamp", { floor_ms: CEILING, ceiling_ms: FLOOR, divisor: DIVISOR }],
];

for (const [label, policy] of invalidPolicies) {
  test(`boundary: a policy that is ${label} falls back to today's fixed interval`, async () => {
    // A deep-queue ahead: if this policy were wrongly adopted, the interval
    // would land near the ceiling instead of the fixed default.
    const status = activeStatus();
    if (policy !== undefined) {
      status.poll_policy = policy;
    }
    const client = loadClient({ route: route(status, 1_000_000) });
    await client.flush();
    const ms = client.lastTimer().ms;
    assert.ok(
      ms >= 5000 && ms <= 6500,
      `expected today's fixed ~5000-6500ms interval, got ${ms}`
    );
  });
}

test("paused, no known position yet: interval is the ceiling", async () => {
  const client = loadClient({
    route: route(activeStatus({ serving_state: "paused", poll_policy: POLICY }), 0),
  });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= CEILING && ms <= CEILING * 1.3, `expected the ceiling, got ${ms}`);
});

test("paused, near-front known position: interval stays at the floor", async () => {
  // Without this, EVERY paused state jumped to the ceiling regardless of
  // position, regressing a near-front visitor to a 30s wait on a pause.
  let phase = "active";
  const client = loadClient({
    route: route(() => activeStatus({ serving_state: phase, poll_policy: POLICY }), 100),
  });
  await client.flush(); // tick 1: active, learns the position (ahead = 10)
  phase = "paused";
  await client.fireLastTimer();
  await client.flush(); // tick 2: paused, position already known

  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected a floor-ish interval, got ${ms}`);
});

test("paused, deep-queue known position: interval is the ceiling", async () => {
  let phase = "active";
  const client = loadClient({
    route: route(() => activeStatus({ serving_state: phase, poll_policy: POLICY }), 1_000_000),
  });
  await client.flush();
  phase = "paused";
  await client.fireLastTimer();
  await client.flush();

  const ms = client.lastTimer().ms;
  assert.ok(ms >= CEILING && ms <= CEILING * 1.3, `expected the ceiling, got ${ms}`);
});

test("a policy already adopted survives a later withdrawal", async () => {
  let sendPolicy = true;
  const client = loadClient({
    route: route(
      () => activeStatus(sendPolicy ? { poll_policy: POLICY } : {}),
      1_000_000
    ),
  });
  await client.flush(); // adopts the real policy; deep ahead lands near the ceiling
  assert.ok(client.lastTimer().ms >= CEILING);

  sendPolicy = false; // /status now omits it entirely
  await client.fireLastTimer();
  await client.flush();

  // Reverting to the fixed default here would step every already-adaptive
  // client back to a 5s poll mid-event on one malformed response.
  assert.ok(
    client.lastTimer().ms >= CEILING,
    "the last good policy should still govern the interval"
  );
});

test("returning to a visible tab before the floor has elapsed defers rather than polling", async () => {
  const client = loadClient({ route: route(activeStatus(), 200) });
  await client.flush(); // establishes lastPollAt

  const callsBefore = client.calls.length;
  client.clock.now += 2000; // well under the 5s floor
  client.fireVisibilityChange();

  assert.equal(
    client.calls.length,
    callsBefore,
    "a poll under the floor since the last one must not fire immediately"
  );
  const scheduled = client.lastTimer();
  assert.equal(scheduled.ms, FLOOR - 2000);

  client.fireLastTimer();
  await client.flush();
  assert.equal(client.calls.length, callsBefore + 1, "the deferred poll must still run");
});

test("returning to a visible tab after the floor has elapsed polls immediately", async () => {
  const client = loadClient({ route: route(activeStatus(), 200) });
  await client.flush();

  const callsBefore = client.calls.length;
  client.clock.now += 6000; // past the 5s floor
  client.fireVisibilityChange();

  assert.equal(client.calls.length, callsBefore + 1, "the floor has elapsed; poll now");
});

test("a visibility-triggered poll cannot overlap a chain already in flight", async () => {
  let resolveStatus;
  const pending = new Promise((resolve) => {
    resolveStatus = resolve;
  });
  let statusCalls = 0;
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/v1/status")) {
        statusCalls += 1;
        return statusCalls === 1 ? pending : jsonResponse(200, activeStatus());
      }
      if (url.startsWith("/v1/queue_num")) {
        return jsonResponse(200, { position: 200, live_join: true });
      }
      return jsonResponse(200, {}); // /v1/join
    },
  });
  // Not awaited: the first /status call is still pending when visibility
  // fires below, exactly the concurrency race the inFlight guard exists for.
  const settle = client.flush();

  client.clock.now += 6000;
  client.fireVisibilityChange();
  await client.flush();

  assert.equal(statusCalls, 1, "inFlight must block the second chain's own /status call");

  resolveStatus(jsonResponse(200, activeStatus()));
  await settle;
  await client.flush();
});

test("a dropped visibility poll is caught up once the in-flight chain settles", async () => {
  // Without catchUpWanted, the visibility handler's tick() above is silently
  // swallowed by inFlight and the visitor waits out the running chain's own
  // (here, ceiling-length) interval instead — burning most of the admission
  // grace on a visitor who was trying to check back in.
  let resolveStatus;
  const pending = new Promise((resolve) => {
    resolveStatus = resolve;
  });
  let statusCalls = 0;
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/v1/status")) {
        statusCalls += 1;
        // Deep ahead, so the settling chain would otherwise schedule a
        // ceiling-length interval and hide whether the catch-up won.
        return statusCalls === 1 ? pending : jsonResponse(200, activeStatus({ poll_policy: POLICY }));
      }
      if (url.startsWith("/v1/queue_num")) {
        return jsonResponse(200, { position: 1_000_000, live_join: true });
      }
      return jsonResponse(200, {}); // /v1/join
    },
  });
  const settle = client.flush();

  client.clock.now += 6000; // already past the 5s floor by the time this settles
  client.fireVisibilityChange(); // dropped: inFlight, sets catchUpWanted

  resolveStatus(jsonResponse(200, activeStatus({ poll_policy: POLICY })));
  await settle;
  await client.flush();

  // lastPollAt is the in-flight poll's own start (t=0); by settle time the
  // clock has already moved 6000ms past it, so the caught-up poll fires at
  // once (remainder = max(0, floor - 6000) = 0) rather than at the ceiling
  // the deep-queue position would otherwise schedule.
  assert.equal(
    client.lastTimer().ms,
    0,
    "the dropped poll must be honoured immediately once the floor has already elapsed"
  );
});

test("measuredRate becomes non-null once a ceiling-length interval has elapsed", async () => {
  // RATE_MIN_SPAN_MS (30s) is only just cleared by a 30s ceiling interval
  // pre-jitter; with two retained samples the span equals one interval, so
  // this is the case where the rate is closest to staying stuck at null.
  let serving = 90;
  const client = loadClient({
    route: route(() => activeStatus({ poll_policy: POLICY, serving_position: serving }), 1_000_000),
  });
  await client.flush(); // tick 1: deep ahead -> ceiling interval; first cursor sample
  const scheduledMs = client.lastTimer().ms; // always >= CEILING (jitter only adds)

  client.clock.now += scheduledMs;
  serving += 50; // the cursor moved during the interval
  client.fireLastTimer();
  await client.flush(); // tick 2: second sample, span === scheduledMs >= RATE_MIN_SPAN_MS

  assert.notEqual(
    client.elements.eta.textContent,
    "—",
    "measuredRate should be non-null once a ceiling-length span has elapsed"
  );
});

test("target_rate absent, position known and ahead > 0: interval is the floor", async () => {
  const status = activeStatus({ poll_policy: POLICY });
  delete status.target_rate;
  const client = loadClient({ route: route(status, 1_000_000) });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected the floor with no rate, got ${ms}`);
});

test("target_rate is zero, position known and ahead > 0: interval is the floor", async () => {
  const client = loadClient({
    route: route(activeStatus({ poll_policy: POLICY, target_rate: 0 }), 1_000_000),
  });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected the floor with rate zero, got ${ms}`);
});

// --- redemption navigation (ADR-0021 §8: next= replaces the bounce guard) --

/**
 * Routes an admitted visitor through to `/v1/generate_token`: `serving_position`
 * ahead of `position` is what makes `redeem()` fire (`position < serving_position`).
 */
function admittedRoute() {
  return (url) => {
    if (url.startsWith("/v1/status")) {
      return jsonResponse(200, activeStatus({ serving_position: 100 }));
    }
    if (url.startsWith("/v1/queue_num")) {
      return jsonResponse(200, { position: 50, live_join: true });
    }
    if (url.startsWith("/v1/generate_token")) {
      return jsonResponse(200, { admitted: true });
    }
    return jsonResponse(200, {}); // /v1/join
  };
}

test("a successful redemption navigates to the gate's next= destination", async () => {
  const client = loadClient({
    route: admittedRoute(),
    locationSearch: "?r=none&next=%2Fcheckout%3Fid%3D1",
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, "/checkout?id=1");
});

test("a successful redemption falls back to / when next is absent", async () => {
  const client = loadClient({ route: admittedRoute() });
  await client.flush();
  assert.equal(client.win.location.replacedTo, "/");
});

test("a successful redemption rejects a next= that would navigate off-site", async () => {
  const client = loadClient({
    route: admittedRoute(),
    locationSearch: "?next=" + encodeURIComponent("//evil.example/phish"),
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, "/");
});
