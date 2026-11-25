// Serves the shipped edge gate over HTTP for the local stack harness.
//
// Loads infra/modules/edge/functions/gate.js.tftpl the way the gate's own tests
// do (tests/vm-gate.js renders the template and stubs the KeyValueStore), then
// answers one POST per viewer request with what the function decided. The
// harness supplies the KeyValueStore values on the command line, so the gate
// reads the same ruleset and signing key generate_token signs with.
//
//   node gate-sidecar.js <port> <event_id> <config-json> <signing-key>

"use strict";

const http = require("node:http");
const path = require("node:path");

const { loadGate } = require(path.join(
  __dirname,
  "..",
  "..",
  "..",
  "infra",
  "modules",
  "edge",
  "tests",
  "vm-gate.js"
));

const [port, eventId, config, key] = process.argv.slice(2);
const gate = loadGate({
  event_id: eventId,
  kvs: { c: JSON.parse(config), k: key },
});

const server = http.createServer((req, res) => {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", async () => {
    try {
      const viewer = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      gate.clock.nowMs = Date.now();
      const headers = {};
      for (const [k, v] of Object.entries(viewer.headers || {})) {
        headers[k] = { value: v };
      }
      const cookies = {};
      for (const [k, v] of Object.entries(viewer.cookies || {})) {
        cookies[k] = { value: v };
      }
      const out = await gate.handler({
        version: "1.0",
        context: { eventType: "viewer-request" },
        viewer: { ip: viewer.ip },
        request: { method: viewer.method, uri: viewer.uri, querystring: {}, headers, cookies },
      });
      // A request object comes back when the gate lets the request through; a
      // response object (statusCode) when it refuses.
      const body =
        out && out.statusCode !== undefined
          ? { pass: false, status: out.statusCode, headers: out.headers || {} }
          : { pass: true, failed: Boolean(out && out.headers && out.headers["x-wr-gate-failed"]) };
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    } catch (e) {
      res.writeHead(500, { "content-type": "text/plain" });
      res.end(String(e && e.stack ? e.stack : e));
    }
  });
});

server.keepAliveTimeout = 60000;
server.listen(Number(port), "127.0.0.1", () => {
  process.stdout.write("ready\n");
});
