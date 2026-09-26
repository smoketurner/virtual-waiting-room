// The edge web ACL (ADR-0038) as the shipped waiting.js meets it.
//
// WAF answers an API call without a valid token with a 202 carrying
// x-amzn-waf-action: challenge, or past a per-IP limit a 405 carrying
// x-amzn-waf-action: captcha. The 202 is the dangerous one: it is a 2xx, so a
// client that reads status alone records a join that never reached the queue
// and polls for a place nothing will write. These tests pin that the page
// treats either answer as "renew the token on the verify page", keeps the
// gate's next= across the detour, and stops rather than loops when the token
// will not stick. The verify page's own return trip is tested at the bottom.
//
// Run with `node --test infra/modules/edge/tests`.

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { loadClient, jsonResponse } = require("./vm-client");

/** What WAF sends instead of the API's answer. Its body is not JSON. */
function wafResponse(status, action) {
  return {
    status,
    headers: {
      get: (name) => (name.toLowerCase() === "x-amzn-waf-action" ? action : null),
    },
    json: () => Promise.reject(new SyntaxError("challenge page is not JSON")),
  };
}

function preQueueStatus() {
  return {
    event_id: "evt-1",
    phase: "pre_queue",
    serving_state: "closed",
    serving_position: 0,
    participant_count: 0,
    target_rate: 10,
  };
}

/** Where the page went to renew its token, decoded back to the return URL. */
function renewedTo(client) {
  const to = client.win.location.replacedTo;
  assert.ok(to, "the page should have navigated to renew its token");
  const m = /^\/_wr\/verify\.html\?back=(.*)$/.exec(to);
  assert.ok(m, `expected the verify page, got ${to}`);
  return decodeURIComponent(m[1]);
}

/** A pre-queue page whose join the edge challenges. */
function challengedJoin(action, status) {
  return (url) => {
    if (url.startsWith("/v1/status")) {
      return jsonResponse(200, preQueueStatus());
    }
    if (url.startsWith("/v1/join")) {
      return wafResponse(status, action);
    }
    throw new Error(`unexpected ${url}`);
  };
}

test("a challenged join is not recorded as a place in line", async () => {
  const client = loadClient({ route: challengedJoin("challenge", 202) });
  await client.flush();

  assert.equal(client.calls.filter((c) => c.url === "/v1/join").length, 1);
  assert.equal(
    client.win.localStorage.getItem("vwr_joined"),
    null,
    "a 202 from WAF is a challenge, not an accepted join"
  );
});

test("a challenged join goes to the verify page to earn a token, and polls no further", async () => {
  const client = loadClient({ route: challengedJoin("challenge", 202) });
  await client.flush();

  assert.match(renewedTo(client), /^\/_wr\/waiting\.html\?waf=\d+$/);
  assert.equal(client.liveTimers().length, 0, "the page is navigating; nothing may be scheduled");
});

test("a CAPTCHA on the join navigates so the verify page can show it", async () => {
  const client = loadClient({ route: challengedJoin("captcha", 405) });
  await client.flush();

  assert.match(renewedTo(client), /\?waf=\d+$/);
});

test("a challenge on /status is renewed the same way", async () => {
  const client = loadClient({ route: () => wafResponse(202, "challenge") });
  await client.flush();

  assert.match(renewedTo(client), /\?waf=\d+$/);
  assert.equal(client.calls.filter((c) => c.url === "/v1/join").length, 0);
});

test("the detour keeps the gate's next= so admission still lands on the refused page", async () => {
  const client = loadClient({
    route: challengedJoin("challenge", 202),
    locationSearch: "?r=none&next=%2Fcheckout",
  });
  await client.flush();

  assert.match(renewedTo(client), /^\/_wr\/waiting\.html\?r=none&next=%2Fcheckout&waf=\d+$/);
});

test("a second WAF answer straight after a renewal stops instead of looping", async () => {
  const now = 1_700_000_000_000;
  const client = loadClient({
    route: challengedJoin("challenge", 202),
    now,
    locationSearch: `?waf=${now - 5000}`,
  });
  await client.flush();

  assert.equal(client.win.location.replacedTo, undefined, "renewing again would loop forever");
  assert.equal(client.elements.headline.textContent, "We couldn't verify this browser");
  assert.equal(client.liveTimers().length, 0);
});

test("a WAF answer long after the last renewal renews again, replacing the old marker", async () => {
  const now = 1_700_000_000_000;
  const client = loadClient({
    route: challengedJoin("challenge", 202),
    now,
    locationSearch: `?next=%2F&waf=${now - 3_600_000}`,
  });
  await client.flush();

  assert.equal(renewedTo(client), `/_wr/waiting.html?next=%2F&waf=${now}`);
});

test("a join the ingest accepts is still recorded when no WAF is in front", async () => {
  const client = loadClient({
    route: (url) =>
      url.startsWith("/v1/status") ? jsonResponse(200, preQueueStatus()) : jsonResponse(200, {}),
  });
  await client.flush();

  assert.notEqual(client.win.localStorage.getItem("vwr_joined"), null);
  assert.equal(client.win.location.replacedTo, undefined);
});

// --- the verify page's return trip -----------------------------------------

const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const VERIFY = fs.readFileSync(path.join(__dirname, "..", "pages", "verify.html"), "utf8");

/** Runs verify.html's inline script with `search` as its query string. */
function verifyReturnsTo(search) {
  const script = /<script>([\s\S]*?)<\/script>/.exec(VERIFY)[1];
  const location = { search, replace: (url) => { location.to = url; } };
  vm.runInNewContext(script, { window: { location } });
  return location.to;
}

test("the verify page returns to the waiting page it was sent from, query intact", () => {
  const back = "/_wr/waiting.html?r=none&next=%2Fcheckout&waf=1";
  assert.equal(verifyReturnsTo("?back=" + encodeURIComponent(back)), back);
});

test("the verify page will not send a visitor off the waiting room's own pages", () => {
  for (const back of ["//evil.example/", "https://evil.example/", "/checkout", "/_wr/../admin", "/\\evil.example"]) {
    assert.equal(
      verifyReturnsTo("?back=" + encodeURIComponent(back)),
      "/_wr/waiting.html",
      `back=${back} must fall back to the waiting page`
    );
  }
});

test("the verify page falls back to the waiting page with no back= or a malformed one", () => {
  assert.equal(verifyReturnsTo(""), "/_wr/waiting.html");
  assert.equal(verifyReturnsTo("?back=%E0%A4%A"), "/_wr/waiting.html");
});
