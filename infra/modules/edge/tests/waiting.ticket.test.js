// Entry tickets on the client (issue #59): ingress, precedence, the derived
// request_id, the conditional fragment strip, and the join circuit breaker.
// Tested against the shipped waiting.js under node:vm — see vm-client.js.
//
// Run with `node --test infra/modules/edge/tests`.

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  loadClient,
  jsonResponse,
  deriveRequestId,
  ticket,
} = require("./vm-client");

const EVENT = "evt-1";
const SUB = "c3ViamVjdC1vbmUtMjItY2hhcnMtbG9uZw";
const NOW_SECS = 1_700_000_000;
const NOW_MS = NOW_SECS * 1000;
const FUTURE = NOW_SECS + 3600;

function statusBody() {
  return {
    phase: "active",
    event_id: EVENT,
    serving_position: 0,
    target_rate: 100,
  };
}

/** Routes /status and /queue_num sanely; records every join body. */
function routerWith(joins) {
  return (url, opts) => {
    if (url.startsWith("/v1/status")) {
      return jsonResponse(200, statusBody());
    }
    if (url.startsWith("/v1/join")) {
      joins.push(JSON.parse(opts.body));
      return jsonResponse(200, {});
    }
    if (url.startsWith("/v1/queue_num")) {
      return jsonResponse(200, { position: 42, live_join: true });
    }
    return jsonResponse(200, {});
  };
}

async function settle(client) {
  await client.flush();
  await client.flush();
  await client.flush();
}

test("a ticket in the fragment derives the request_id the server will derive", async () => {
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: `#wrt=${ticket({ exp: FUTURE })}`,
  });
  await settle(client);

  assert.equal(joins.length, 1, "the visitor joined exactly once");
  assert.equal(
    joins[0].request_id,
    deriveRequestId(EVENT, SUB),
    "client and server must derive the same id or every record is dropped"
  );
  assert.ok(joins[0].ticket, "the ticket is forwarded for verification");
});

test("the same identity yields the same request_id across separate visits", async () => {
  // No shared storage between these two clients: the id is a pure function of
  // the ticket, which is what makes one identity hold one position.
  const first = [];
  const second = [];
  const a = loadClient({
    route: routerWith(first),
    now: NOW_MS,
    locationHash: `#wrt=${ticket({ exp: FUTURE })}`,
  });
  await settle(a);
  const b = loadClient({
    route: routerWith(second),
    now: NOW_MS,
    // A different ticket for the same subject — a re-issue, as a second device
    // would receive.
    locationHash: `#wrt=${ticket({ exp: FUTURE + 60 })}`,
  });
  await settle(b);

  assert.equal(first[0].request_id, second[0].request_id);
});

test("with no ticket the client still joins, on a generated id", async () => {
  const joins = [];
  const client = loadClient({ route: routerWith(joins), now: NOW_MS });
  await settle(client);

  assert.equal(joins.length, 1);
  assert.match(
    joins[0].request_id,
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    "an unticketed deployment must keep working"
  );
  assert.equal(joins[0].ticket, undefined, "no ticket field when none is held");
});

test("an expired ticket is ignored rather than presented", async () => {
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: `#wrt=${ticket({ exp: NOW_SECS - 1 })}`,
  });
  await settle(client);

  assert.equal(joins[0].ticket, undefined);
  assert.notEqual(joins[0].request_id, deriveRequestId(EVENT, SUB));
});

test("a fresher cookie beats a stale bookmarked fragment", async () => {
  // The conditional strip deliberately leaves a fragment in place for a
  // storage-denied visitor, so they can bookmark it and come back later with a
  // newer ticket in the cookie. Ordering by source would pick the stale one and
  // have it rejected as expired while a valid ticket sat one tier down.
  const stale = ticket({ exp: NOW_SECS + 10, sub: SUB });
  const fresh = ticket({ exp: NOW_SECS + 9999, sub: SUB });
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: `#wrt=${stale}`,
    cookie: `vwr_ticket=${fresh}`,
  });
  await settle(client);

  assert.equal(joins[0].ticket, fresh, "the later exp must win, not the source");
});

test("the fragment is stripped once the ticket is stored durably", async () => {
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: `#wrt=${ticket({ exp: FUTURE })}`,
  });
  await settle(client);

  assert.ok(
    client.win.history.replacedWith !== undefined,
    "a stored ticket should not linger in the address bar"
  );
  assert.ok(!String(client.win.history.replacedWith).includes("wrt="));
});

test("the fragment is KEPT when every durable tier fails", async () => {
  // The memory tier always succeeds and never survives a reload, so treating
  // it as stored would strip the fragment and strand this visitor on refresh —
  // the exact population the storage chain exists to protect.
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: `#wrt=${ticket({ exp: FUTURE })}`,
    storageFails: ["local", "session", "cookie"],
  });
  await settle(client);

  assert.equal(
    client.win.history.replacedWith,
    undefined,
    "stripping here loses the ticket on reload"
  );
  assert.equal(
    joins[0].request_id,
    deriveRequestId(EVENT, SUB),
    "the derived id still works with no storage at all"
  );
});

test("a storage-denied visitor joins once, not once per poll", async () => {
  // Before the in-memory joined mirror, readStored(JOINED_KEY) returned null on
  // every poll, so this visitor re-POSTed /v1/join for the whole wait.
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    storageFails: ["local", "session", "cookie"],
  });
  await settle(client);
  for (let i = 0; i < 5; i++) {
    client.clock.now += 60_000;
    await client.fireLastTimer();
    await settle(client);
  }

  assert.equal(joins.length, 1, `joined ${joins.length} times across 6 polls`);
});

test("a join that always fails stops instead of retrying forever", async () => {
  // Backoff bounds the rate but never stops; MAX_JOIN_ATTEMPTS is what bounds
  // the total. Without a terminal state this is ~8k POST/s at a million
  // waiters.
  let attempts = 0;
  const client = loadClient({
    now: NOW_MS,
    route: (url) => {
      if (url.startsWith("/v1/status")) {
        return jsonResponse(200, statusBody());
      }
      if (url.startsWith("/v1/join")) {
        attempts += 1;
        return jsonResponse(500, {});
      }
      return jsonResponse(404, {});
    },
  });
  await settle(client);

  for (let i = 0; i < 40; i++) {
    const timer = client.lastTimer();
    if (!timer) {
      break;
    }
    client.clock.now += 600_000;
    await client.fireLastTimer();
    await settle(client);
  }

  // The ceiling is MAX_JOIN_ATTEMPTS x (MAX_REJOIN_CYCLES + 1) = 8 x 4: a
  // re-join cycle deliberately resets the attempt counter, because it is a
  // fresh attempt at a fresh place. Both counters terminate, so the product is
  // the bound — what matters is that it is finite and that polling stops.
  assert.ok(attempts >= 2, "it should retry a few times before giving up");
  assert.ok(attempts <= 32, `gave up after ${attempts} attempts, expected <= 32`);
  assert.equal(
    client.liveTimers().length,
    0,
    "polling must stop once the join is hopeless"
  );
});

test("a malformed ticket is discarded, not forwarded", async () => {
  const joins = [];
  const client = loadClient({
    route: routerWith(joins),
    now: NOW_MS,
    locationHash: "#wrt=not-a-jws",
  });
  await settle(client);

  assert.equal(joins.length, 1, "a bad ticket must not prevent joining");
  assert.equal(joins[0].ticket, undefined);
});
