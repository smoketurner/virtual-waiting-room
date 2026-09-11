// Loads the SHIPPED infra/modules/edge/functions/gate.js.tftpl under
// node:vm, rendered with test values the same way Terraform's templatefile()
// renders `${var}` tokens, with a stub `cloudfront` module (cf.kvs()) and
// Node's real `crypto` module.
//
// The template's `import cf from 'cloudfront'; import crypto from 'crypto';`
// lines are literal ES module syntax CloudFront's runtime resolves itself;
// node:vm's non-Module APIs cannot run that without --experimental-vm-modules,
// so this strips those two lines and injects the same names as sandbox
// globals instead. Everything after the imports is exactly the shipped
// function body — this is a testing shim around module resolution, not a
// rewrite of production logic.
//
// This tests Node's Buffer, HMAC and base64url — not CloudFront's. It is a
// proxy for what scripts/spike_edge_gate.py proved against a real
// CloudFront Function (ADR-0021 §6), not a replacement for it; a release
// still needs a manual `aws cloudfront test-function` run over these same
// vectors.

"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const SRC = path.join(__dirname, "..", "functions", "gate.js.tftpl");

/** Mirrors Terraform's `${var}` interpolation for the plain scalars this template uses. */
function renderTemplate(source, values) {
  return source.replace(/\$\{(\w+)\}/g, (_, name) => {
    if (!(name in values)) {
      throw new Error(`missing template value for \${${name}}`);
    }
    return values[name];
  });
}

function stripImports(source) {
  return source.replace(/^import .+;\r?\n/gm, "");
}

/**
 * Loads the gate into a fresh vm context.
 *
 * `kvs` maps key -> value (for `get('c', {format:'json'})` pass the parsed
 * object; for `get('k', {format:'string'})` pass the string) or an `Error`
 * instance to make that read reject, exercising the catch-and-pass-through
 * path. `now` is epoch SECONDS (the function computes
 * `Math.floor(Date.now()/1000)`, so the harness multiplies by 1000 for it).
 */
function loadGate({ event_id, session_cookie_name, waiting_path, kvs, now } = {}) {
  const templateSrc = fs.readFileSync(SRC, "utf8");
  const rendered = renderTemplate(templateSrc, {
    event_id: event_id ?? "smoke",
    session_cookie_name: session_cookie_name ?? "vwr_session",
    waiting_path: waiting_path ?? "/_wr/waiting.html",
  });
  const script = stripImports(rendered);

  const kvsStore = kvs || {};
  const clock = { nowMs: (now ?? 1_700_000_500) * 1000 };
  const logs = [];
  const kvsCalls = [];

  const ctx = vm.createContext({
    cf: {
      kvs: () => ({
        get: (key) => {
          kvsCalls.push(key);
          if (!(key in kvsStore)) {
            return Promise.reject(new Error(`no such key: ${key}`));
          }
          const entry = kvsStore[key];
          return entry instanceof Error ? Promise.reject(entry) : Promise.resolve(entry);
        },
      }),
    },
    crypto,
    Buffer,
    console: {
      log: (msg) => logs.push(String(msg)),
    },
    Date: { now: () => clock.nowMs },
  });
  vm.runInContext(script, ctx, { filename: "gate.js" });

  return {
    ctx,
    clock,
    logs,
    kvsCalls,
    /** Calls the shipped handler(event) and returns its settled result. */
    handler: (event) => ctx.handler(event),
    /** Calls the shipped verify(credential, kind, secret) directly. */
    verify: (credential, kind, secret) => ctx.verify(credential, kind, secret),
    /** Calls the shipped matches(rule, request) directly. */
    matches: (rule, request) => ctx.matches(rule, request),
  };
}

/** A viewer-request event object in the shape CloudFront passes a function. */
function event(uri, { headers, cookies } = {}) {
  const hdrs = {};
  for (const [k, v] of Object.entries(headers || {})) {
    hdrs[k] = { value: v };
  }
  const cks = {};
  for (const [k, v] of Object.entries(cookies || {})) {
    cks[k] = { value: v };
  }
  return {
    version: "1.0",
    context: { eventType: "viewer-request" },
    viewer: { ip: "203.0.113.9" },
    request: { method: "GET", uri, querystring: {}, headers: hdrs, cookies: cks },
  };
}

module.exports = { loadGate, event };
