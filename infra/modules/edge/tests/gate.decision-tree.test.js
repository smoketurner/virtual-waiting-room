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
  // mirroring wr_common::crypto's JWS independently — the same reasoning as
  // crates/wr-common/tests/vectors.rs. Field order inside the header and the
  // claims is deliberately not the order the Rust side emits: the gate must
  // read them, not match an encoded string.
  const crypto = require("node:crypto");
  const fields = Object.assign(
    { eventId: EVENT_ID, requestId: "r1", issuedAt: 1_700_000_000, expiresAt: 1_700_003_600 },
    overrides
  );
  const header = b64url({ alg: "HS256", typ: "JWT" });
  const claims = b64url(
    Object.assign(
      {
        aud: fields.eventId,
        sub: fields.requestId,
        exp: fields.expiresAt,
        iat: fields.issuedAt,
      },
      fields.cip === undefined ? {} : { cip: fields.cip }
    )
  );
  const key = crypto.createHmac("sha256", SECRET).update("vwr/jws/session/v1").digest();
  const mac = crypto.createHmac("sha256", key).update(`${header}.${claims}`).digest("base64url");
  return `${header}.${claims}.${mac}`;
}

function b64url(obj) {
  return Buffer.from(JSON.stringify(obj), "utf8").toString("base64url");
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

// --- issue #242: a throw past a rule match refuses --------------------------

function assertPlainRefusal(result, req) {
  assert.notEqual(result, req.request, "must not pass the request through");
  assert.equal(result.statusCode, 403);
  assert.equal(result.headers["x-wr-reason"].value, "error");
  assert.equal(result.headers["cache-control"].value, "no-store");
  assert.ok(!("location" in result.headers));
}

test("unreadable signing key, with a cookie: refused, not passed", async () => {
  for (const kvs of [{ c: protectedConfig() }, { c: protectedConfig(), k: new Error("kvs down") }]) {
    const gate = loadGate({ kvs });
    const req = event("/checkout", {
      headers: { accept: "text/html" },
      cookies: { [COOKIE]: validSessionCredential() },
    });
    const result = await gate.handler(req);
    assertPlainRefusal(result, req);
    assert.ok(!("x-wr-gate-failed" in req.request.headers));
    assert.ok(gate.logs.some((l) => l.includes("gate threw")));
  }
});

test("unreadable signing key, without a cookie: refused to the waiting page", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: new Error("kvs down") } });
  const req = event("/checkout", { headers: { accept: "text/html" }, cookies: {} });
  const result = await gate.handler(req);
  assert.equal(result.statusCode, 302);
  assert.equal(result.headers["x-wr-reason"].value, "none");
});

test("a throw while building the refusal still refuses, plainly", async () => {
  // A querystring entry whose getter throws: refuse() reads it for next=.
  const gate = loadGate({ kvs: { c: protectedConfig(), k: SECRET } });
  for (const cookies of [{}, { [COOKIE]: "not.a.session" }]) {
    const req = event("/checkout", { headers: { accept: "text/html" }, cookies });
    Object.defineProperty(req.request.querystring, "q", {
      enumerable: true,
      get() {
        throw new Error("hostile querystring");
      },
    });
    assertPlainRefusal(await gate.handler(req), req);
  }
});

test("a throw while verifying the cookie refuses", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: SECRET } });
  const req = event("/checkout", { headers: { accept: "text/html" } });
  // A cookie entry with no string value: verify() throws on .split.
  req.request.cookies[COOKIE] = { value: 42 };
  assertPlainRefusal(await gate.handler(req), req);
});

test("a throw on an unprotected request still passes, marked", async () => {
  // A malformed rule throws while matching: nothing yet says the request is
  // protected, so this is the unreadable-config case.
  const gate = loadGate({ kvs: { c: { v: 1, s: 0, f: 0, r: [["p", 7]] } } });
  const req = event("/about", { headers: { accept: "text/html" } });
  const result = await gate.handler(req);
  assert.equal(result, req.request);
  assert.equal(result.headers["x-wr-gate-failed"].value, "true");
});

// --- issue #242: suspicious paths, through the whole handler ----------------

test("a path an origin might canonicalise to a protected one needs a session", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: SECRET } });
  for (const uri of ["/x/../checkout", "/%2e%2e/checkout", "/shop;a/checkout", "/shop\\checkout", "/a/./b"]) {
    const req = event(uri, { headers: { accept: "text/html" }, cookies: {} });
    const result = await gate.handler(req);
    assert.equal(result.statusCode, 302, `${uri} passed with no cookie`);
  }
  const cred = validSessionCredential({ expiresAt: 1_700_003_600 });
  const req = event("/x/../checkout", { headers: { accept: "text/html" }, cookies: { [COOKIE]: cred } });
  assert.equal(await gate.handler(req), req.request, "a valid session still passes");
});

test("a suspicious path passes untouched when no path rule exists", async () => {
  for (const r of [[], [["c", "loyalty_member"]]]) {
    const gate = loadGate({ kvs: { c: protectedConfig({ r }) } });
    const req = event("/x/../checkout", { headers: { accept: "text/html" } });
    assert.equal(await gate.handler(req), req.request);
    assert.ok(!("x-wr-gate-failed" in req.request.headers));
  }
});

// --- issue #247: a session signed under the previous key --------------------

const NEW_SECRET = "the-key-the-deployment-rotated-to";

test("a session signed under the previous key (kp) is accepted", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: NEW_SECRET, kp: SECRET } });
  const req = event("/checkout", {
    headers: { accept: "text/html" },
    cookies: { [COOKIE]: validSessionCredential() },
  });
  assert.equal(await gate.handler(req), req.request);
  assert.deepEqual(gate.kvsCalls, ["c", "k", "kp"]);
});

test("a session under the current key never reads kp", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig(), k: SECRET, kp: "an-older-key" } });
  const req = event("/checkout", {
    headers: { accept: "text/html" },
    cookies: { [COOKIE]: validSessionCredential() },
  });
  assert.equal(await gate.handler(req), req.request);
  assert.deepEqual(gate.kvsCalls, ["c", "k"]);
});

test("a session under neither key is refused with reason=signature", async () => {
  for (const kp of ["some-other-key", new Error("kvs down"), undefined]) {
    const kvs = { c: protectedConfig(), k: NEW_SECRET };
    if (kp !== undefined) {
      kvs.kp = kp;
    }
    const gate = loadGate({ kvs });
    const req = event("/checkout", {
      headers: { accept: "text/html" },
      cookies: { [COOKIE]: validSessionCredential() },
    });
    const result = await gate.handler(req);
    assert.equal(result.statusCode, 302);
    assert.equal(result.headers["x-wr-reason"].value, "signature");
  }
});

test("a previous-key session still has its expiry, event and IP binding checked", async () => {
  const kvs = { c: protectedConfig({ b: true }), k: NEW_SECRET, kp: SECRET };
  const reasonFor = async (overrides, viewerIp, now) => {
    const gate = loadGate({ kvs, now });
    const req = event("/checkout", {
      headers: { accept: "text/html" },
      cookies: { [COOKIE]: validSessionCredential(overrides(gate)) },
      viewerIp,
    });
    const result = await gate.handler(req);
    return result === req.request ? "pass" : result.headers["x-wr-reason"].value;
  };
  // The tag was minted under the key that signed the session, so it is
  // checked under that key, not the current one.
  const tagged = (gate) => ({ cip: gate.ipTag(SECRET, "198.51.100.7") });
  assert.equal(await reasonFor(tagged, "198.51.100.7"), "pass");
  assert.equal(await reasonFor(tagged, "203.0.113.50"), "ip");
  assert.equal(await reasonFor(() => ({ eventId: "other" }), "198.51.100.7"), "event");
  assert.equal(await reasonFor(tagged, "198.51.100.7", 1_700_003_600), "expired");
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

// --- issue #61: optional IP binding -----------------------------------------

async function withBinding({ bind, cip, viewerIp }) {
  const gate = loadGate({ kvs: { c: protectedConfig(bind ? { b: true } : {}), k: SECRET } });
  const tag = cip === "viewer" ? gate.ipTag(SECRET, "198.51.100.7") : cip;
  const req = event("/checkout", {
    headers: { accept: "text/html" },
    cookies: { [COOKIE]: validSessionCredential({ cip: tag }) },
    viewerIp: viewerIp ?? "198.51.100.7",
  });
  return { req, result: await gate.handler(req) };
}

test("binding on: a session tagged with the viewer's network passes", async () => {
  const { req, result } = await withBinding({ bind: true, cip: "viewer" });
  assert.equal(result, req.request);
});

test("binding on: the same session from another address is refused with reason ip", async () => {
  const { result } = await withBinding({ bind: true, cip: "viewer", viewerIp: "203.0.113.50" });
  assert.equal(result.statusCode, 302);
  assert.equal(result.headers["x-wr-reason"].value, "ip");
  assert.match(result.headers.location.value, /\?r=ip&next=/);
});

test("binding on: a session minted without a tag is refused", async () => {
  const { result } = await withBinding({ bind: true, cip: undefined });
  assert.equal(result.headers["x-wr-reason"].value, "ip");
});

test("binding off: the tag is not consulted", async () => {
  for (const cip of [undefined, "viewer", "someone-elses-tag-xxxxxx"]) {
    const { req, result } = await withBinding({ bind: false, cip, viewerIp: "203.0.113.50" });
    assert.equal(result, req.request, `cip=${cip}`);
  }
});

test("binding on: an IPv6 visitor keeps access across addresses in the same /64", async () => {
  const gate = loadGate({ kvs: { c: protectedConfig({ b: true }), k: SECRET } });
  const tag = gate.ipTag(SECRET, "2001:db8:0:1::aaaa");
  const req = event("/checkout", {
    headers: { accept: "text/html" },
    cookies: { [COOKIE]: validSessionCredential({ cip: tag }) },
    viewerIp: "2001:db8:0:1:ffff:1:2:3",
  });
  assert.equal(await gate.handler(req), req.request);
});
