// Pre-queue countdown (issue #128): tests against the shipped waiting.js,
// loaded under node:vm (see vm-client.js for why).
//
// The countdown is the visitor half of making the start time an operator
// control. It renders only during pre_queue, only from an absolute epoch
// published on /status, and must fall back to the original text whenever that
// epoch is absent or unusable — a page that renders "Opens in NaN" to a
// million waiting people is worse than one that says nothing.
//
// Run with `node --test infra/modules/edge/tests`.

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { loadClient, jsonResponse } = require("./vm-client");

const NOW_MS = 1_700_000_000_000;
const NOW_SECS = NOW_MS / 1000;

/** A `/v1/status` body for a scheduled event that has not opened yet. */
function preQueueStatus(overrides) {
  return Object.assign(
    {
      event_id: "evt-1",
      phase: "pre_queue",
      serving_state: "closed",
      serving_position: 0,
    },
    overrides
  );
}

function route(statusBody) {
  return (url) => {
    if (url.startsWith("/v1/status")) {
      return jsonResponse(200, statusBody);
    }
    return jsonResponse(200, {}); // /v1/join
  };
}

async function headlineFor(statusBody) {
  const client = loadClient({ route: route(statusBody), now: NOW_MS });
  await client.flush();
  return client.elements.headline.textContent;
}

test("a scheduled event counts down to its start", async () => {
  const headline = await headlineFor(
    preQueueStatus({ starts_at: NOW_SECS + 15 * 60 })
  );
  assert.match(headline, /^Opens in /);
  assert.match(headline, /15 min/);
});

test("an unscheduled event keeps the original text", async () => {
  // The regression this guards: before #128 every closed event said this, and
  // an event with no start time still must.
  const headline = await headlineFor(preQueueStatus());
  assert.equal(headline, "The event isn't open yet");
});

test("a start time that has passed says so instead of counting down further", async () => {
  // The seal fires from EventBridge, not from this page, so there is a window
  // where the start has passed and the phase has not flipped. A negative
  // duration must never be rendered.
  const headline = await headlineFor(
    preQueueStatus({ starts_at: NOW_SECS - 30 })
  );
  assert.equal(headline, "Opening now");
  assert.doesNotMatch(headline, /-/);
});

test("a start time exactly now says opening, not a zero countdown", async () => {
  const headline = await headlineFor(preQueueStatus({ starts_at: NOW_SECS }));
  assert.equal(headline, "Opening now");
});

const unusable = [
  ["a string", "1700000900"],
  ["null", null],
  ["an object", { epoch: 1_700_000_900 }],
  ["an array", [1_700_000_900]],
  ["NaN", Number.NaN],
  ["Infinity", Number.POSITIVE_INFINITY],
  ["a boolean", true],
];

for (const [label, starts_at] of unusable) {
  test(`a start time that is ${label} falls back rather than rendering NaN`, async () => {
    const headline = await headlineFor(preQueueStatus({ starts_at }));
    assert.equal(headline, "The event isn't open yet");
    assert.doesNotMatch(headline, /NaN|Infinity|undefined/);
  });
}

test("the countdown does not suppress pre-queue registration", async () => {
  // The countdown shares its branch with the join() call that registers a
  // visitor into the raffle. Rendering a countdown must not cost them their
  // place in it.
  const client = loadClient({
    route: route(preQueueStatus({ starts_at: NOW_SECS + 600 })),
    now: NOW_MS,
  });
  await client.flush();
  assert.equal(
    client.calls.filter((c) => c.url.includes("/v1/join")).length,
    1,
    "a counting-down visitor must still register"
  );
});

test("the countdown adds no timer of its own", async () => {
  // It redraws on the existing poll rather than ticking locally: a second
  // timer would be a second thing to get wrong, and the coarse wording does
  // not need one.
  const client = loadClient({
    route: route(preQueueStatus({ starts_at: NOW_SECS + 600 })),
    now: NOW_MS,
  });
  await client.flush();
  assert.equal(
    client.liveTimers().length,
    1,
    "exactly the poll timer, nothing else"
  );
});
