// Issue #67: a visitor whose browser runs no waiting.js sends no join, so the
// page must say they are not in line rather than promise a let-through that
// will never come. Checks the SHIPPED waiting.html.

"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const HTML = fs.readFileSync(
  path.join(__dirname, "..", "pages", "waiting.html"),
  "utf8",
);

test("with JavaScript off, the page says the visitor is not in line and offers the form", () => {
  const bodyNotice = HTML.match(/<main[\s\S]*<noscript>([\s\S]*?)<\/noscript>/);
  assert.ok(bodyNotice, "the body carries a <noscript> notice");
  assert.match(bodyNotice[1], /not in line/);
  // Issue #67: a plain form into the server-rendered queue, no script needed.
  assert.match(bodyNotice[1], /<form method="post" action="\/v1\/enter">/);
});

test("with JavaScript off, the let-through promise is hidden", () => {
  const head = HTML.slice(0, HTML.indexOf("</head>"));
  const style = head.match(/<noscript>[\s\S]*?<style>([\s\S]*?)<\/style>/);
  assert.ok(style, "the head carries a <noscript> style");
  assert.match(style[1], /\.head[\s\S]*#note[\s\S]*display:\s*none/);
  assert.match(HTML, /class="head"[\s\S]*You'll be let through automatically/);
});

test("a script that fails to load replaces the headline", () => {
  const onerror = HTML.match(/<script[^>]*src="\/_wr\/waiting\.js"[^>]*onerror="([^"]*)"/);
  assert.ok(onerror, "the waiting.js tag has an onerror handler");
  const nodes = {
    headline: { textContent: "Getting your place in line…" },
    subhead: { textContent: "Keep this page open." },
    note: { hidden: false },
    "nojs-join": { hidden: true },
  };
  const document = { getElementById: (id) => nodes[id] };
  vm.runInNewContext(onerror[1], { document });
  assert.equal(nodes.headline.textContent, "You are not in line yet");
  assert.match(nodes.subhead.textContent, /Reload to try again, or join below/);
  assert.equal(nodes.note.hidden, true);
  assert.equal(nodes["nojs-join"].hidden, false, "the script-free form is offered instead");
  assert.match(HTML, /<form id="nojs-join" method="post" action="\/v1\/enter" hidden>/);
});
