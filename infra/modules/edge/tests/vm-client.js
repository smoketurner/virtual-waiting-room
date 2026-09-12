// Loads the SHIPPED infra/modules/edge/pages/waiting.js under node:vm, with a
// stub document/window/fetch/crypto, so tests exercise the file that
// actually deploys rather than a copy or an extracted subset of its logic.
//
// waiting.js keeps its state (nextIntervalMs, knownPosition, the poll timer,
// ...) inside one IIFE closure and exports nothing, so this harness cannot
// call its functions directly. Instead it observes the same surface a real
// browser would: what fetch() was called with, what setTimeout() was
// scheduled with, and what landed in the DOM stubs — and it can drive the
// script forward by firing a pending timer or the visibilitychange listener,
// the same two things a browser does on its own.

"use strict";

const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SRC = path.join(__dirname, "..", "pages", "waiting.js.tftpl");

/** The cookie name Terraform templates in; matches the module's variable default. */
const ENTRY_TICKET_COOKIE_NAME = "vwr_ticket";

/** Mirrors Terraform's `${var}` interpolation for the plain scalars this template uses. */
function renderTemplate(source, values) {
  return source.replace(/\$\{(\w+)\}/g, (_, name) => {
    if (!(name in values)) {
      throw new Error(`missing template value for \${${name}}`);
    }
    return values[name];
  });
}

/** A `Response`-shaped value `getJSON`/`postJSON` can call `.json()` on. */
function jsonResponse(status, body) {
  return { status: status, json: () => Promise.resolve(body) };
}

/**
 * Loads waiting.js into a fresh vm context.
 *
 * `route(url, opts)` plays the origin: called for every `fetch`, it returns
 * (or resolves to) a value from `jsonResponse`. `now` seeds the fake clock;
 * advance it with `client.clock.now = ...` between calls — the script only
 * ever reads time through `Date.now()`, which this harness redirects to it.
 */
function loadClient({
  route,
  now,
  locationSearch,
  locationHash,
  cookie,
  // Which storage tiers throw. "local" and "session" mimic private browsing
  // and blocked site data; "cookie" mimics a document.cookie that silently
  // refuses to persist, which is what a Secure cookie on a non-HTTPS origin
  // does. Passing all three leaves only the in-memory tier, which is the case
  // the conditional fragment strip exists for.
  storageFails,
}) {
  const clock = { now: now || 1_700_000_000_000 };
  const calls = [];
  const timers = [];
  let nextTimerId = 1;
  const elements = {};
  const listeners = {};
  const fails = new Set(storageFails || []);

  const storageOf = (name) => {
    const store = new Map();
    const guard = () => {
      if (fails.has(name)) {
        throw new Error(`${name}Storage unavailable`);
      }
    };
    return {
      getItem: (k) => {
        guard();
        return store.has(k) ? store.get(k) : null;
      },
      setItem: (k, v) => {
        guard();
        store.set(k, String(v));
      },
      removeItem: (k) => {
        guard();
        store.delete(k);
      },
    };
  };

  // A document.cookie that behaves like the real one: assignment appends or
  // replaces a single pair, reading returns the whole jar. When the cookie
  // tier is failing, writes are silently dropped — which is exactly why
  // storeDurable reads back rather than trusting that no exception was thrown.
  const jar = new Map();
  if (cookie) {
    for (const pair of String(cookie).split(";")) {
      const [k, ...rest] = pair.trim().split("=");
      if (k) {
        jar.set(k, rest.join("="));
      }
    }
  }

  const win = {
    localStorage: storageOf("local"),
    sessionStorage: storageOf("session"),
    atob: (b64) => Buffer.from(b64, "base64").toString("binary"),
    crypto: {
      getRandomValues: (a) => a.fill(7),
      subtle: {
        digest: async (algorithm, data) =>
          require("node:crypto")
            .createHash(String(algorithm).toLowerCase().replace("-", ""))
            .update(Buffer.from(data))
            .digest().buffer,
      },
    },
    history: {
      replaceState: (_state, _title, url) => {
        win.history.replacedWith = url;
        win.location.hash = "";
      },
    },
    location: {
      search: locationSearch || "",
      hash: locationHash || "",
      pathname: "/_wr/waiting.html",
      replace: (url) => { win.location.replacedTo = url; },
    },
    setTimeout: (fn, ms) => {
      const id = nextTimerId++;
      timers.push({ id, fn, ms, cancelled: false });
      return id;
    },
    clearTimeout: (id) => {
      const t = timers.find((t) => t.id === id);
      if (t) {
        t.cancelled = true;
      }
    },
  };

  const doc = {
    hidden: false,
    get cookie() {
      return [...jar.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
    },
    set cookie(value) {
      const [pair] = String(value).split(";");
      const [k, ...rest] = pair.trim().split("=");
      if (!k) {
        return;
      }
      if (/max-age=0/i.test(value)) {
        jar.delete(k);
        return;
      }
      if (fails.has("cookie")) {
        return;
      }
      jar.set(k, rest.join("="));
    },
    getElementById: (id) => {
      const el = { id, textContent: "", hidden: false, className: "", style: {} };
      elements[id] = el;
      return el;
    },
    addEventListener: (type, fn) => {
      (listeners[type] = listeners[type] || []).push(fn);
    },
  };

  // waiting.js reaches the DOM as a bare global, as a browser does; exposing it
  // on window too means a `window.document.x` slip cannot silently no-op here.
  win.document = doc;

  const fetchImpl = (url, opts) => {
    calls.push({ url: String(url), opts });
    try {
      return Promise.resolve(route(String(url), opts));
    } catch (e) {
      return Promise.reject(e);
    }
  };

  const ctx = vm.createContext({
    window: win,
    document: doc,
    crypto: win.crypto,
    TextEncoder,
    TextDecoder,
    fetch: fetchImpl,
    // A real Date, but anchored to the test's clock: `Date.now()` and a bare
    // `new Date()` both read `clock.now`, so a timestamp the page renders is
    // deterministic instead of the wall clock.
    Date: class extends Date {
      constructor(...args) {
        super(...(args.length ? args : [clock.now]));
      }
      static now() {
        return clock.now;
      }
    },
    Math,
    console,
  });
  const source = renderTemplate(fs.readFileSync(SRC, "utf8"), {
    entry_ticket_cookie_name: ENTRY_TICKET_COOKIE_NAME,
  });
  vm.runInContext(source, ctx, { filename: "waiting.js" });

  return {
    win,
    doc,
    clock,
    calls,
    timers,
    elements,
    /** Live (uncancelled) timers, oldest first. */
    liveTimers: () => timers.filter((t) => !t.cancelled),
    /** The most recently scheduled live timer. */
    lastTimer: () => {
      const live = timers.filter((t) => !t.cancelled);
      return live[live.length - 1];
    },
    /** Fires the most recently scheduled live timer, as a real one would. */
    fireLastTimer: function () {
      const t = this.lastTimer();
      t.cancelled = true;
      return t.fn();
    },
    /** Fires every registered `visibilitychange` listener. */
    fireVisibilityChange: () => {
      (listeners.visibilitychange || []).forEach((fn) => fn());
    },
    /** Lets a settled promise chain's microtasks fully drain. */
    flush: () => new Promise((resolve) => setImmediate(resolve)),
  };
}

/** The request_id waiting.js derives for a ticket subject, computed independently. */
function deriveRequestId(aud, sub) {
  const message = Buffer.concat([
    Buffer.from("vwr/rid/v1", "utf8"),
    Buffer.from([0]),
    Buffer.from(aud, "utf8"),
    Buffer.from([0]),
    Buffer.from(sub, "utf8"),
  ]);
  const bytes = require("node:crypto")
    .createHash("sha256")
    .update(message)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0"));
  return (
    hex.slice(0, 4).join("") + "-" + hex.slice(4, 6).join("") + "-" +
    hex.slice(6, 8).join("") + "-" + hex.slice(8, 10).join("") + "-" +
    hex.slice(10, 16).join("")
  );
}

/** An unsigned JWS-shaped ticket. waiting.js only decodes; it never verifies. */
function ticket({ aud = "evt-1", sub = "c3ViamVjdC1vbmUtMjItY2hhcnMtbG9uZw", exp }) {
  const b64 = (o) =>
    Buffer.from(JSON.stringify(o)).toString("base64url");
  return `${b64({ alg: "ES256" })}.${b64({ aud, sub, exp })}.c2ln`;
}

module.exports = { loadClient, jsonResponse, deriveRequestId, ticket };
