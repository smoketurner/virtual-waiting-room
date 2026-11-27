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
 * A `/_wr/v1/status` body for an open, active event. `participant_count: 0`
 * keeps `scheduleFirstAsk`'s spread window at exactly zero, so `/_wr/v1/queue_num`
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
      origin_concurrency: 100,
      session_seconds: 1,
    },
    overrides
  );
}

/**
 * Routes `/_wr/v1/status` to `statusBody` (or `statusBody()` if given a
 * function, so a test can vary it call to call), answers `/_wr/v1/queue_num`
 * with `position`, and accepts `/_wr/v1/join` unconditionally.
 */
function route(statusBody, position) {
  return (url) => {
    if (url.startsWith("/_wr/v1/status")) {
      return jsonResponse(200, typeof statusBody === "function" ? statusBody() : statusBody);
    }
    if (url.startsWith("/_wr/v1/queue_num")) {
      return jsonResponse(200, { position, live_join: true });
    }
    return jsonResponse(200, {}); // /_wr/v1/join
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
  assert.equal(client.calls.filter((c) => c.url.includes("/_wr/v1/join")).length, 0);
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
      if (url.startsWith("/_wr/v1/status")) {
        statusCalls += 1;
        return statusCalls === 1 ? pending : jsonResponse(200, activeStatus());
      }
      if (url.startsWith("/_wr/v1/queue_num")) {
        return jsonResponse(200, { position: 200, live_join: true });
      }
      return jsonResponse(200, {}); // /_wr/v1/join
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
      if (url.startsWith("/_wr/v1/status")) {
        statusCalls += 1;
        // Deep ahead, so the settling chain would otherwise schedule a
        // ceiling-length interval and hide whether the catch-up won.
        return statusCalls === 1 ? pending : jsonResponse(200, activeStatus({ poll_policy: POLICY }));
      }
      if (url.startsWith("/_wr/v1/queue_num")) {
        return jsonResponse(200, { position: 1_000_000, live_join: true });
      }
      return jsonResponse(200, {}); // /_wr/v1/join
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

test("origin_concurrency absent, position known and ahead > 0: interval is the floor", async () => {
  const status = activeStatus({ poll_policy: POLICY });
  delete status.origin_concurrency;
  const client = loadClient({ route: route(status, 1_000_000) });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected the floor with no rate, got ${ms}`);
});

test("origin_concurrency is zero, position known and ahead > 0: interval is the floor", async () => {
  const client = loadClient({
    route: route(activeStatus({ poll_policy: POLICY, origin_concurrency: 0 }), 1_000_000),
  });
  await client.flush();
  const ms = client.lastTimer().ms;
  assert.ok(ms >= FLOOR && ms <= FLOOR * 1.3, `expected the floor with rate zero, got ${ms}`);
});

// --- redemption navigation (ADR-0021 §8: next= replaces the bounce guard) --

/**
 * Routes an admitted visitor through to `/_wr/v1/generate_token`: `serving_position`
 * ahead of `position` is what makes `redeem()` fire (`position < serving_position`).
 */
function admittedRoute() {
  return (url) => {
    if (url.startsWith("/_wr/v1/status")) {
      return jsonResponse(200, activeStatus({ serving_position: 100 }));
    }
    if (url.startsWith("/_wr/v1/queue_num")) {
      return jsonResponse(200, { position: 50, live_join: true });
    }
    if (url.startsWith("/_wr/v1/generate_token")) {
      return jsonResponse(200, { admitted: true });
    }
    return jsonResponse(200, {}); // /_wr/v1/join
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

test("a successful redemption rejects a backslash next= that browsers treat as //", async () => {
  const client = loadClient({
    route: admittedRoute(),
    locationSearch: "?next=" + encodeURIComponent("/\\evil.example/phish"),
  });
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

// Issue #244: browsers drop tabs and newlines from a URL, so "/\t/host" is
// "//host" by the time it navigates.
for (const ws of ["\t", "\n", "\r"]) {
  test(`a successful redemption rejects a next= hiding // behind ${JSON.stringify(ws)}`, async () => {
    const client = loadClient({
      route: admittedRoute(),
      locationSearch: "?next=" + encodeURIComponent("/" + ws + "/evil.example/phish"),
    });
    await client.flush();
    assert.equal(client.win.location.replacedTo, "/");
  });
}

// --- issue #62: the request id is not enough on its own ------------------

const nodeCrypto = require("node:crypto");
const b64url = (buf) => Buffer.from(buf).toString("base64url");

test("the join carries SHA-256 of the secret, and only redemption carries the secret", async () => {
  const client = loadClient({ route: admittedRoute() });
  await client.flush();

  const join = client.calls.find((c) => c.url.startsWith("/_wr/v1/join"));
  const redeem = client.calls.find((c) => c.url.startsWith("/_wr/v1/generate_token"));
  const joinBody = JSON.parse(join.opts.body);
  const redeemBody = JSON.parse(redeem.opts.body);

  assert.match(redeemBody.secret, /^[A-Za-z0-9_-]{43}$/);
  assert.equal(joinBody.secret, undefined, "the secret never goes to the join");
  assert.equal(
    joinBody.h,
    b64url(nodeCrypto.createHash("sha256").update(redeemBody.secret).digest()),
    "h is base64url(SHA-256(secret)), as wr_common::PossessionSecret::digest computes it"
  );
  assert.equal(joinBody.request_id, redeemBody.request_id);
  for (const call of client.calls) {
    assert.ok(!call.url.includes(redeemBody.secret), `secret in a URL: ${call.url}`);
    if (!call.url.startsWith("/_wr/v1/generate_token") && call.opts && call.opts.body) {
      assert.ok(!call.opts.body.includes(redeemBody.secret), `secret sent to ${call.url}`);
    }
  }
});

test("the id and its secret are stored together", async () => {
  const client = loadClient({ route: admittedRoute() });
  await client.flush();

  const stored = client.win.localStorage.getItem("vwr_identity");
  assert.match(stored, /^[0-9a-f-]{36}\.[A-Za-z0-9_-]{43}$/);
  const join = JSON.parse(client.calls.find((c) => c.url.startsWith("/_wr/v1/join")).opts.body);
  assert.equal(join.request_id, stored.split(".")[0]);
});

test("the identity cookie is scoped to the waiting room, not sent to the origin", async () => {
  const client = loadClient({ route: admittedRoute(), storageFails: ["local"] });
  await client.flush();
  const write = client.cookieWrites.find((w) => w.startsWith("vwr_identity="));
  assert.ok(write, "the cookie tier holds the identity when localStorage fails");
  assert.match(write, /;path=\/_wr\/;/);
});

test("a redemption refused as not the holder stops, rather than polling forever", async () => {
  const admitted = admittedRoute();
  const client = loadClient({
    route: (url) =>
      url.startsWith("/_wr/v1/generate_token")
        ? jsonResponse(403, { admitted: false, error: "not the holder of this place in line" })
        : admitted(url),
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, undefined);
  assert.equal(client.lastTimer(), undefined, "nothing left scheduled");
  assert.match(client.elements.headline.textContent, /couldn't confirm/);
});

// Issue #237: a place whose session has run out cannot be redeemed again.
test("a redemption answered 'gone' drops the place and stops", async () => {
  const admitted = admittedRoute();
  const client = loadClient({
    route: (url) =>
      url.startsWith("/_wr/v1/generate_token")
        ? jsonResponse(410, { admitted: false, error: "the admission for this place in line has run out" })
        : admitted(url),
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, undefined);
  assert.equal(client.lastTimer(), undefined, "nothing left scheduled");
  assert.match(client.elements.headline.textContent, /visit has ended/);
  assert.equal(client.win.localStorage.getItem("vwr_identity"), null);
});

// A turn that passed before the page took it: the spot went to the next
// place in line, so the page takes a new place at the back and says why.
test("a redemption answered 'turn passed' takes a new place and keeps waiting", async () => {
  const admitted = admittedRoute();
  let missed = true;
  let draw = 0;
  const client = loadClient({
    // A fresh identity each time one is minted, so the new place is told apart.
    randomValues: (a) => a.fill((draw += 1)),
    route: (url) => {
      if (url.startsWith("/_wr/v1/generate_token") && missed) {
        missed = false;
        return jsonResponse(410, {
          admitted: false,
          error: "the turn for position 50 has passed",
          reason: "turn_missed",
        });
      }
      return admitted(url);
    },
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, undefined);
  assert.notEqual(client.lastTimer(), undefined, "still waiting");
  assert.match(client.elements.headline.textContent, /turn passed/);
  const identity = client.win.localStorage.getItem("vwr_identity");
  assert.ok(identity, "a new identity is stored");

  // The next poll joins again, under the new identity.
  const joinsBefore = client.calls.filter((c) => c.url.startsWith("/_wr/v1/join")).length;
  client.clock.now += client.lastTimer().ms;
  client.fireLastTimer();
  await client.flush();
  const joins = client.calls.filter((c) => c.url.startsWith("/_wr/v1/join"));
  assert.equal(joins.length, joinsBefore + 1, "joined again");
  const rejoined = JSON.parse(joins[joins.length - 1].opts.body).request_id;
  assert.equal(rejoined, identity.split(".")[0]);
  assert.notEqual(rejoined, JSON.parse(joins[0].opts.body).request_id);
});

// ADR-0039: the visitor's turn has come but the admission budget is spent.
test("a redemption answered 'at capacity' keeps polling and is admitted on a later try", async () => {
  const admitted = admittedRoute();
  let full = true;
  const client = loadClient({
    route: (url) =>
      url.startsWith("/_wr/v1/generate_token") && full
        ? jsonResponse(425, { admitted: false, error: "at capacity; position 50 is next" })
        : admitted(url),
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, undefined);
  const retry = client.lastTimer();
  assert.ok(retry, "a poll is still scheduled");

  full = false;
  retry.fn();
  await client.flush();
  assert.equal(client.win.location.replacedTo, "/");
});

for (const status of [429, 502]) {
  test(`a redemption answered ${status} backs off like 'at capacity'`, async () => {
    const client = loadClient({
      route: (url) => {
        if (url.startsWith("/_wr/v1/status")) {
          return jsonResponse(200, activeStatus({ serving_position: 100, poll_policy: POLICY }));
        }
        if (url.startsWith("/_wr/v1/queue_num")) {
          return jsonResponse(200, { position: 50, live_join: true });
        }
        if (url.startsWith("/_wr/v1/generate_token")) {
          return jsonResponse(status, { message: "busy" });
        }
        return jsonResponse(200, {});
      },
    });
    const delays = [];
    for (let i = 0; i < 3; i += 1) {
      await client.flush();
      const t = client.lastTimer();
      delays.push(t.ms);
      t.cancelled = true;
      t.fn();
    }
    assert.ok(delays[1] > delays[0] * 1.2, JSON.stringify(delays));
  });
}

test("repeated 'at capacity' answers are asked again at the floor", async () => {
  // A 425 is a slot not yet free, and the people asking are only those
  // released for the next slots: backing off would leave the slot empty.
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/_wr/v1/status")) {
        return jsonResponse(200, activeStatus({ serving_position: 100, poll_policy: POLICY }));
      }
      if (url.startsWith("/_wr/v1/queue_num")) {
        return jsonResponse(200, { position: 50, live_join: true });
      }
      if (url.startsWith("/_wr/v1/generate_token")) {
        return jsonResponse(425, { admitted: false, error: "at capacity; position 50 is next" });
      }
      return jsonResponse(200, {});
    },
  });
  for (let i = 0; i < 6; i += 1) {
    await client.flush();
    const t = client.lastTimer();
    assert.ok(t.ms <= POLICY.floor_ms * 1.3, `try ${i} waits ${t.ms} ms`);
    t.cancelled = true;
    t.fn();
  }
});

test("a visitor within one origin's worth of the front polls at the floor", async () => {
  // Sessions ending release them without the cursor's measured speed saying
  // so in advance. Further back, the interval follows the wait.
  const near = loadClient({
    route: route(activeStatus({ poll_policy: POLICY, origin_concurrency: 500, session_seconds: 900 }), 400),
  });
  await near.flush();
  assert.ok(near.lastTimer().ms <= POLICY.floor_ms * 1.3, `near waits ${near.lastTimer().ms} ms`);

  const far = loadClient({
    route: route(activeStatus({ poll_policy: POLICY, origin_concurrency: 500, session_seconds: 900 }), 100_000),
  });
  await far.flush();
  assert.ok(far.lastTimer().ms >= POLICY.ceiling_ms, `far waits ${far.lastTimer().ms} ms`);
});

// The whole crowd arriving at once: the gateway answers most joins 429.
test("a throttled join is retried for as long as it takes, and no number is asked for meanwhile", async () => {
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/_wr/v1/status")) {
        return jsonResponse(200, activeStatus({ poll_policy: POLICY }));
      }
      if (url.startsWith("/_wr/v1/join")) {
        return jsonResponse(429, {});
      }
      return jsonResponse(200, { position: 50, live_join: true });
    },
  });
  // Far more rounds than MAX_JOIN_ATTEMPTS, each past any backoff.
  for (let i = 0; i < 30; i += 1) {
    await client.flush();
    client.clock.now += 10 * 60 * 1000;
    const t = client.lastTimer();
    assert.ok(t, "still polling after " + i + " throttled joins");
    t.cancelled = true;
    t.fn();
  }
  await client.flush();
  const joins = client.calls.filter((c) => c.url.startsWith("/_wr/v1/join")).length;
  assert.ok(joins > 20, "kept trying: " + joins);
  assert.equal(
    client.calls.filter((c) => c.url.startsWith("/_wr/v1/queue_num")).length,
    0,
    "no position is asked for before a join has been accepted"
  );
});

// A registration throttled in the countdown's last seconds has to land before
// the open counts the cohort, or the visitor is numbered behind all of it.
test("a throttled registration during the countdown is retried at the floor, without growing", async () => {
  let accept = false;
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/_wr/v1/status")) {
        return jsonResponse(200, {
          event_id: "e",
          phase: "pre_queue",
          serving_state: "closed",
          starts_at: Math.floor(Date.now() / 1000) + 3600,
          poll_policy: POLICY,
        });
      }
      if (url.startsWith("/_wr/v1/join")) {
        return jsonResponse(accept ? 200 : 429, {});
      }
      return jsonResponse(404, {});
    },
  });
  const joins = () => client.calls.filter((c) => c.url.startsWith("/_wr/v1/join")).length;
  for (let i = 0; i < 10; i += 1) {
    await client.flush();
    const t = client.lastTimer();
    assert.ok(t.ms <= FLOOR * 1.3, `poll ${i} waits ${t.ms} ms`);
    client.clock.now += t.ms;
    t.cancelled = true;
    t.fn();
  }
  await client.flush();
  // Each retry waits half to one and a half floors, so at least every other
  // poll at the floor asks again.
  assert.ok(joins() >= 5, "retried: " + joins());

  accept = true;
  client.clock.now += 2 * FLOOR;
  client.fireLastTimer();
  await client.flush();
  const settled = joins();
  client.fireLastTimer();
  await client.flush();
  assert.equal(joins(), settled, "an accepted registration is not repeated");
  assert.ok(client.lastTimer().ms >= CEILING, "and the countdown polls at the ceiling again");
});

// --- issue #61: re-minting a session refused for another network ----------

test("a visitor sent back for another network is re-admitted straight through", async () => {
  const client = loadClient({
    route: admittedRoute(),
    locationSearch: "?r=ip&next=%2Fcheckout",
  });
  await client.flush();
  assert.equal(client.win.location.replacedTo, "/checkout");
});

test("a second ip refusal within a minute stops instead of looping", async () => {
  const first = loadClient({ route: admittedRoute(), locationSearch: "?r=ip&next=%2Fcheckout" });
  await first.flush();
  const stamp = first.win.localStorage.getItem("vwr_ip_rebind");
  assert.ok(stamp, "the re-mint is recorded");

  const second = loadClient({
    route: admittedRoute(),
    locationSearch: "?r=ip&next=%2Fcheckout",
    now: first.clock.now + 5_000,
  });
  second.win.localStorage.setItem("vwr_ip_rebind", stamp);
  second.win.localStorage.setItem("vwr_identity", first.win.localStorage.getItem("vwr_identity"));
  await second.flush();
  assert.equal(second.win.location.replacedTo, undefined, "no second bounce");
  assert.match(second.elements.headline.textContent, /keeps changing/);
});

// --- issue #247: the gate turning a fresh session back ----------------------

/** Loads the page as the gate's redirect would, carrying storage over from `prev`. */
function bounceBack(prev, reason, afterMs) {
  // Seeded before load: the page counts a bounce once, as it starts.
  const stored = {};
  for (const key of ["vwr_identity", "vwr_redeemed", "vwr_joined"]) {
    const value = prev.win.localStorage.getItem(key);
    if (value !== null) {
      stored[key] = value;
    }
  }
  return loadClient({
    route: admittedRoute(),
    locationSearch: `?r=${reason}&next=%2Fcheckout`,
    now: prev.clock.now + afterMs,
    stored,
  });
}

function redemptions(client) {
  return client.calls.filter((c) => c.url.startsWith("/_wr/v1/generate_token")).length;
}

test("a session the gate keeps refusing stops after two bounces instead of looping", async () => {
  const first = loadClient({ route: admittedRoute(), locationSearch: "?r=none&next=%2Fcheckout" });
  await first.flush();
  assert.equal(first.win.location.replacedTo, "/checkout");

  // Turned back once: could be a race with a rotating key, so try once more.
  const second = bounceBack(first, "signature", 800);
  await second.flush();
  assert.equal(second.win.location.replacedTo, "/checkout", "one bounce is retried");

  // Turned back again: stop, without redeeming a third time.
  const third = bounceBack(second, "signature", 800);
  await third.flush();
  assert.equal(third.win.location.replacedTo, undefined, "no third navigation");
  assert.equal(redemptions(third), 0, "and no third redemption");
  assert.match(third.elements.headline.textContent, /turned you back/);
  assert.match(third.elements.subhead.textContent, /reload in a minute/);
  assert.equal(third.liveTimers().length, 0, "polling has stopped");

  // Reloading inside the window changes nothing; after it, the visitor is let through again.
  const early = bounceBack(second, "signature", 30_000);
  await early.flush();
  assert.equal(early.win.location.replacedTo, undefined);
  const later = bounceBack(second, "signature", 61_000);
  await later.flush();
  assert.equal(later.win.location.replacedTo, "/checkout");
});

test("the bounce guard covers every refusal reason, blocked cookies included", async () => {
  for (const reason of ["none", "event", "expired", "error"]) {
    const first = loadClient({ route: admittedRoute() });
    await first.flush();
    const second = bounceBack(first, reason, 500);
    await second.flush();
    const third = bounceBack(second, reason, 500);
    await third.flush();
    assert.equal(third.win.location.replacedTo, undefined, `r=${reason} must stop`);
  }
});

test("a refusal long after the last redemption is not a bounce", async () => {
  // An expired session an hour on is the ordinary way back into the queue.
  const first = loadClient({ route: admittedRoute() });
  await first.flush();
  let prev = first;
  for (let i = 0; i < 4; i++) {
    const next = bounceBack(prev, "expired", 3_600_000);
    await next.flush();
    assert.equal(next.win.location.replacedTo, "/checkout", `visit ${i}`);
    prev = next;
  }
});

test("a recent redemption without a gate refusal is not a bounce", async () => {
  // Reopening the waiting page by hand carries no r=.
  let prev = loadClient({ route: admittedRoute() });
  await prev.flush();
  for (let i = 0; i < 4; i++) {
    const next = loadClient({
      route: admittedRoute(),
      now: prev.clock.now + 500,
      stored: { vwr_redeemed: prev.win.localStorage.getItem("vwr_redeemed") },
    });
    await next.flush();
    assert.equal(next.win.location.replacedTo, "/", `visit ${i}`);
    prev = next;
  }
});

// --- issue #251: back off while the API is failing --------------------------

test("consecutive /status failures back off to the ceiling and reset on success", async () => {
  let failing = false;
  const healthy = route(() => activeStatus({ poll_policy: POLICY, serving_position: 90 }), 95);
  const client = loadClient({
    route: (url) => (failing ? jsonResponse(503, {}) : healthy(url)),
  });
  await client.flush();
  assert.ok(client.lastTimer().ms <= FLOOR * 1.3, "healthy and near the front: the floor");

  failing = true;
  const seen = [];
  for (let i = 0; i < 5; i++) {
    client.fireLastTimer();
    await client.flush();
    seen.push(client.lastTimer().ms);
  }
  [10000, 20000, 30000, 30000, 30000].forEach((base, i) => {
    assert.ok(
      seen[i] >= base && seen[i] <= base * 1.3,
      `failure ${i + 1}: expected ${base}ms plus jitter, got ${seen[i]}`
    );
  });

  failing = false;
  client.fireLastTimer();
  await client.flush();
  assert.ok(client.lastTimer().ms <= FLOOR * 1.3, "the first success returns to the floor");

  // And the next outage starts doubling from the floor again, not the ceiling.
  failing = true;
  client.fireLastTimer();
  await client.flush();
  assert.ok(client.lastTimer().ms <= 10000 * 1.3, `got ${client.lastTimer().ms}`);
});

test("a network failure and a 429 back off the same way", async () => {
  for (const fail of [() => Promise.reject(new TypeError("Failed to fetch")), () => jsonResponse(429, {})]) {
    let failing = false;
    const healthy = route(() => activeStatus({ poll_policy: POLICY }), 95);
    const client = loadClient({ route: (url) => (failing ? fail() : healthy(url)) });
    await client.flush();
    failing = true;
    client.fireLastTimer();
    await client.flush();
    client.fireLastTimer();
    await client.flush();
    assert.ok(client.lastTimer().ms >= 20000, `second failure: got ${client.lastTimer().ms}`);
  }
});

test("a redemption that fails in flight is retried, not abandoned", async () => {
  // A network error, or an error page that is not JSON, rejects the
  // generate_token call. Left set, the in-flight flag turned every later
  // redeem() into a no-op that scheduled nothing, and the visitor sat on
  // "Letting you through" with no timer running.
  let attempts = 0;
  const admitted = admittedRoute();
  const client = loadClient({
    route: (url) => {
      if (url.startsWith("/_wr/v1/generate_token") && attempts++ === 0) {
        return Promise.reject(new TypeError("Failed to fetch"));
      }
      return admitted(url);
    },
  });
  await client.flush();
  assert.equal(attempts, 1);
  assert.equal(client.win.location.replacedTo, undefined);

  for (let i = 0; i < 3 && client.win.location.replacedTo === undefined; i++) {
    assert.ok(client.lastTimer(), "a retry is scheduled after the failed redemption");
    client.fireLastTimer();
    await client.flush();
  }
  assert.equal(attempts, 2, "the redemption is attempted again");
  assert.equal(client.win.location.replacedTo, "/");
});

test("the status timestamp is stamped from the clock once a position is known", async () => {
  const client = loadClient({
    route: route(() => activeStatus({ serving_position: 10 }), 500),
  });
  await client.flush();

  assert.equal(
    client.elements.updated.hidden,
    false,
    "the freshness line appears alongside the bar"
  );
  assert.equal(
    client.elements["updated-at"].textContent,
    new Date(client.clock.now).toLocaleTimeString(),
    "stamped from the injected clock, not the wall clock"
  );

  // It answers "is this page still live", so it has to move on a later poll
  // even when nothing else on the page changes.
  const first = client.elements["updated-at"].textContent;
  client.clock.now += 60_000;
  client.fireLastTimer();
  await client.flush();
  assert.notEqual(client.elements["updated-at"].textContent, first);
});

test("the progress baseline survives a reload", async () => {
  // A long wait outlives a tab. Without persistence the bar resets to empty on
  // every reopen, telling a visitor who has waited an hour that they are at the
  // start.
  const first = loadClient({ route: route(() => activeStatus({ serving_position: 100 }), 1000) });
  await first.flush();
  const started = first.win.localStorage.getItem("vwr_ahead_at_start");
  assert.notEqual(started, null, "the starting distance is stored");

  const second = loadClient({ route: route(() => activeStatus({ serving_position: 600 }), 1000) });
  second.win.localStorage.setItem("vwr_ahead_at_start", started);
  second.win.localStorage.setItem("vwr_identity", first.win.localStorage.getItem("vwr_identity"));
  await second.flush();

  const width = parseFloat(second.elements.fill.style.width);
  assert.ok(width > 0, `progress carried over, got ${second.elements.fill.style.width}`);
});

test("the freshness stamp updates on every status poll, number or not", async () => {
  // It reports that the page is talking to the room. Before a position lands
  // every other value is a dash, which is precisely when a visitor doubts it.
  const client = loadClient({
    route: (url) =>
      url.includes("/_wr/v1/status")
        ? jsonResponse(200, { event_id: "e", phase: "pre_queue", serving_state: "closed" })
        : jsonResponse(404, {}),
  });
  await client.flush();

  assert.equal(client.elements.updated.hidden, false, "shown on a closed event");
  const first = client.elements["updated-at"].textContent;
  assert.notEqual(first, "—");

  client.clock.now += 60_000;
  client.fireLastTimer();
  await client.flush();
  assert.notEqual(client.elements["updated-at"].textContent, first, "moves on the next poll");
});
