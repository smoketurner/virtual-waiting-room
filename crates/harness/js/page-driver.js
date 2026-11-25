// Runs the shipped waiting.js against the local stack's edge, in real time.
//
// Each page is the deployed infra/modules/edge/pages/waiting.js under node:vm,
// loaded by the same loader its tests use (tests/vm-client.js), with fetch sent
// over HTTP to the edge and its timers fired when they come due. When the page
// navigates after admission, the driver follows it to the protected path with
// the session cookie the edge set, the way a browser would, and records
// whether the gate let it through.
//
//   node page-driver.js <edge-url> <protected-path> <deadline-ms> <arrival-ms>...
//
// Prints one JSON line per page when all have finished.

"use strict";

const crypto = require("node:crypto");
const path = require("node:path");

const { loadClient, jsonResponse } = require(path.join(
  __dirname, "..", "..", "..", "infra", "modules", "edge", "tests", "vm-client.js"
));

const [edge, protectedPath, deadlineArg, ...arrivals] = process.argv.slice(2);
const deadline = Date.now() + Number(deadlineArg);

const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms)));

async function page(arriveInMs) {
  await sleep(arriveInMs);
  const jar = new Map();
  let inFlight = 0;
  let requests = 0;
  const route = async (url, opts) => {
    inFlight += 1;
    requests += 1;
    try {
      const headers = { "content-type": (opts && opts.headers && opts.headers["Content-Type"]) || "application/json" };
      if (jar.size) {
        headers.cookie = [...jar].map(([k, v]) => `${k}=${v}`).join("; ");
      }
      const res = await fetch(edge + url, {
        method: (opts && opts.method) || "GET",
        headers,
        body: opts && opts.body,
      });
      for (const c of res.headers.getSetCookie()) {
        const [pair] = c.split(";");
        const i = pair.indexOf("=");
        jar.set(pair.slice(0, i).trim(), pair.slice(i + 1).trim());
      }
      const text = await res.text();
      let body = {};
      try {
        body = JSON.parse(text);
      } catch {
        body = {};
      }
      return jsonResponse(res.status, body);
    } finally {
      inFlight -= 1;
    }
  };
  const client = loadClient({
    route,
    now: Date.now(),
    randomValues: (a) => crypto.randomFillSync(a),
  });
  const settle = async () => {
    do {
      await client.flush();
      if (inFlight) {
        await sleep(5);
      }
    } while (inFlight);
    await client.flush();
  };

  for (;;) {
    await settle();
    const to = client.win.location.replacedTo;
    if (to !== undefined) {
      const res = await fetch(edge + protectedPath, {
        headers: {
          accept: "text/html",
          cookie: [...jar].map(([k, v]) => `${k}=${v}`).join("; "),
        },
        redirect: "manual",
      });
      return { outcome: res.status === 200 ? "admitted" : `refused ${res.status}`, requests };
    }
    const timer = client.lastTimer();
    if (!timer) {
      return { outcome: "stopped: " + client.elements.headline?.textContent, requests };
    }
    if (Date.now() + timer.ms > deadline) {
      return { outcome: "still waiting", requests };
    }
    await sleep(timer.ms);
    client.clock.now = Date.now();
    timer.cancelled = true;
    timer.fn();
  }
}

Promise.all(arrivals.map((a) => page(Number(a)).catch((e) => ({ outcome: "crashed: " + e }))))
  .then((results) => {
    for (const r of results) {
      process.stdout.write(JSON.stringify(r) + "\n");
    }
  });
