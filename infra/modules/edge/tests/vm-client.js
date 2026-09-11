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

const SRC = path.join(__dirname, "..", "pages", "waiting.js");

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
function loadClient({ route, now }) {
  const clock = { now: now || 1_700_000_000_000 };
  const calls = [];
  const timers = [];
  let nextTimerId = 1;
  const elements = {};
  const listeners = {};

  const storageOf = () => {
    const store = new Map();
    return {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    };
  };

  const win = {
    localStorage: storageOf(),
    sessionStorage: storageOf(),
    location: { replace: (url) => { win.location.replacedTo = url; } },
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
    getElementById: (id) => {
      const el = { id, textContent: "", hidden: false, className: "", style: {} };
      elements[id] = el;
      return el;
    },
    addEventListener: (type, fn) => {
      (listeners[type] = listeners[type] || []).push(fn);
    },
  };

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
    crypto: { getRandomValues: (a) => a.fill(7) },
    fetch: fetchImpl,
    Date: { now: () => clock.now },
    Math,
    console,
  });
  vm.runInContext(fs.readFileSync(SRC, "utf8"), ctx, { filename: "waiting.js" });

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

module.exports = { loadClient, jsonResponse };
