// Branch coverage of the shipped gate.js decision tree (issue #71), against
// the vm harness in vm-gate.js. Complements gate.conformance.test.js, which
// proves credential/rule agreement with Rust but does not exercise the
// full handler() control flow (dormancy, epochs, refusal shaping, the
// throw-and-pass-through path).
//
// Run with: node --test infra/modules/edge/tests/*.test.js

"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { loadGate, event } = require("./vm-gate");

const EVENT_ID = "smoke";
const COOKIE = "vwr_session";
const WAITING = "/_wr/waiting.html";
const PROTECTED_RULES = [["p", "/checkout"]];
const SECRET = "a-32-byte-test-signing-key-value";

function validSessionCredential(overrides) {
  // Built via node:crypto directly rather than the gate's own verify(),
  // mirroring wr_common::crypto's wire format independently — the same
  // reasoning as crates/wr-common/tests/vectors.rs.
  const crypto = require("node:crypto");
  const fields = Object.assign(
    { eventId: EVENT_ID, requestId: "r1", issuedAt: 1_700_000_000, expiresAt: 1_700_003_600 },
    overrides
  );
  const eventIdBuf = Buffer.from(fields.eventId, "utf8");
  const requestIdBuf = Buffer.from(fields.requestId, "utf8");
  const payload = Buffer.concat([
    u16be(eventIdBuf.length),
    eventIdBuf,
    u16be(requestIdBuf.length),
    requestIdBuf,
    u64be(fields.issuedAt),
    u64be(fields.expiresAt),
  ]);
  const hmac = crypto.createHmac("sha256", SECRET);
  hmac.update(Buffer.concat([Buffer.from([0x02]), payload]));
  const mac = hmac.digest();
  return `${payload.toString("base64url")}.${mac.toString("base64url")}`;
}

function u16be(n) {
  const b = Buffer.alloc(2);
  b.writeUInt16BE(n);
  return b;
}

function u64be(n) {
  const b = Buffer.alloc(8);
  b.writeBigUInt64BE(BigInt(n));
  return b;
}

function dormantConfig() {
  return { v: 1, s: 0, f: 0, r: [] };
}

function protectedConfig(extra) {
  return Object.assign({ v: 1, s: 0, f: 0, r: PROTECTED_RULES }, extra);
}

test("dormant config (r: []) passes every request through untouched", async () => {
  const gate = loadGate({ kvs: { c: dormantConfig() } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result, req.request, "must return the request object unmodified");
});

test("unprotected path (no rule matches) passes through without reading the secret", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig() } });
  await gate.handler(event("/about", { headers: { accept: "text/html" } }));
  assert.deepEqual(gate.kvsCalls, ["c"], "must not read 'k' when no rule matched");
});

test("enforce_from in the future marks pending and passes through", async () => {
  const now = 1000;
  const gate = loadGate({ now, kvs: { c: protectedConfig({ s: now + 60 }) } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result.headers["x-wr-gate"].value, "pending");
  assert.deepEqual(gate.kvsCalls, ["c"], "must not read 'k' while pending");
});

test("fail_open_until in the future marks failopen and passes through", async () => {
  const now = 1000;
  const gate = loadGate({ now, kvs: { c: protectedConfig({ f: now + 60 }) } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result.headers["x-wr-gate"].value, "failopen");
});

test("a lapsed fail_open_until enforces normally again", async () => {
  const now = 1000;
  const gate = loadGate({ now, kvs: { c: protectedConfig({ f: now - 1 }) } });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: {} });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 302, "must enforce once the epoch has passed");
});

test("no credential, navigation: 302 to the waiting page with reason=none", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig() } });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: {} });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 302);
  assert.equal(result.headers["x-wr-reason"].value, "none");
  assert.ok(result.headers.location.value.startsWith(`${WAITING}?r=none`));
  assert.ok(result.headers.location.value.includes("next=%2Fcheckout"));
});

test("no credential, XHR: 403 JSON with reason=none, no redirect", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig() } });
  const req = event("/checkout", {
    headers: { accept: "application/json", "sec-fetch-mode": "cors" },
    cookies: {},
  });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 403);
  assert.equal(result.headers["x-wr-reason"].value, "none");
  assert.ok(!("location" in result.headers));
  const body = JSON.parse(result.body);
  assert.equal(body.reason, "none");
});

test("credential minted under a different key: refused with reason=signature", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: "a-different-deployments-key" } });
  const cred = validSessionCredential();
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: cred } });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 302);
  assert.equal(result.headers["x-wr-reason"].value, "signature");
});

test("tampered credential: refused with reason=signature", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: SECRET } });
  const bad = `${validSessionCredential().split(".")[0]}.AAAA`;
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: bad } });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 302);
  assert.equal(result.headers["x-wr-reason"].value, "signature");
});

test("valid credential for another event: refused with reason=event", async () => {
  const gate = loadGate({ event_id: EVENT_ID, kvs: { c: protectedConfig(), k: SECRET } });
  const cred = validSessionCredential({ eventId: "other-event" });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: cred } });
  const result = await gate.handler(req);
  assert.equal(result.headers["x-wr-reason"].value, "event");
});

test("expired credential: refused with reason=expired", async () => {
  const now = 2_000_000_000;
  const cred = validSessionCredential({ issuedAt: 1_000_000_000, expiresAt: now - 1 });
  const gate = loadGate({ now, kvs: { c: protectedConfig(), k: SECRET } });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: cred } });
  const result = await gate.handler(req);
  assert.equal(result.headers["x-wr-reason"].value, "expired");
});

test("valid credential: passes through untouched", async () => {
  const now = 1_700_000_100;
  const cred = validSessionCredential({ issuedAt: 1_700_000_000, expiresAt: now + 3600 });
  const gate = loadGate({ now, kvs: { c: protectedConfig(), k: SECRET } });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: cred } });
  const result = await gate.handler(req);
  assert.equal(result, req.request);
});

test("an unrecognised config version throws and passes through marked", async () => {
  const gate = loadGate({ kvs: { c: { v: 2, s: 0, f: 0, r: [] } } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result, req.request);
  assert.equal(result.headers["x-wr-gate-failed"].value, "true");
  assert.ok(gate.logs.some((l) => l.includes("gate threw")));
});

test("a failed KeyValueStore read throws and passes through marked (fail-open on a broken gate)", async () => {
  const gate = loadGate({ kvs: { c: new Error("network partition") } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result, req.request);
  assert.equal(result.headers["x-wr-gate-failed"].value, "true");
});

test("spoofed x-wr-gate / x-wr-gate-failed headers are stripped before any decision", async () => {
  const gate = loadGate({ kvs: { c: dormantConfig() } });
  const req = event("/anything", {
    headers: { accept: "text/html", "x-wr-gate": "failopen", "x-wr-gate-failed": "true" },
  });
  const result = await gate.handler(req);
  assert.ok(!("x-wr-gate" in result.headers));
  assert.ok(!("x-wr-gate-failed" in result.headers));
});
