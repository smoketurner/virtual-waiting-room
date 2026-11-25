// The waiting room client.
//
// Holds a place in line, shows where that place is, and redeems it the moment
// the queue reaches it. Everything it needs survives a reload, because the one
// thing a visitor must never lose is their position.
//
// The flow:
//   1. mint or recover a request id and its possession secret (issue #62),
//      persisted together across the storage chain below
//   2. POST the id and SHA-256(secret) to /_wr/v1/join once, so a position is
//      claimed; the secret itself goes only to /_wr/v1/generate_token
//   3. poll /_wr/v1/status and /_wr/v1/queue_num
//   4. once the position is reached, POST /_wr/v1/generate_token to collect the
//      admission cookies, then reload onto the origin

(function () {
  "use strict";

  // "<request_id>.<secret>" as one value, so the id is never recovered
  // without the secret that proves it (issue #62).
  var IDENTITY_KEY = "vwr_identity";
  var IDENTITY_RE = /^([0-9a-f-]{36})\.([A-Za-z0-9_-]{43})$/;
  var JOINED_KEY = "vwr_joined";
  var AHEAD_AT_START_KEY = "vwr_ahead_at_start";
  var IP_REBIND_KEY = "vwr_ip_rebind";
  var IP_REBIND_WINDOW_MS = 60000;
  // "<epoch ms>.<bounces>": when this page last redeemed, and how many times
  // in a row the gate had by then sent the visitor straight back (issue #247).
  var REDEEMED_KEY = "vwr_redeemed";
  var BOUNCE_WINDOW_MS = 60000;
  var MAX_BOUNCES = 2;

  // Adaptive poll interval (#69). No policy published ⇒ behave
  // exactly as this client always has: floor === ceiling === 5000ms, divisor
  // 1, so intervalFor always returns the fixed 5000ms this client polled at
  // before this change. The real numbers are set in terraform.tfvars and
  // arrive on /status as `poll_policy`.
  var POLL_FLOOR_MS = 5000;
  var POLL_CEILING_MS = 5000;
  var POLL_DIVISOR = 1;
  // Proportional jitter, spread as a fraction of the interval being jittered
  // rather than a flat window, so a visitor at the ceiling is not spread by
  // the same few seconds as one at the floor. 0.3 at the floor gives
  // 5000..6500ms — the same window this client always used.
  var JITTER_FRACTION = 0.3;
  // Bounds a published policy must fall within to be trusted; see adoptPolicy.
  var MIN_MS = 1000;
  var MAX_MS = 300000;
  var MAX_DIVISOR = 1000;

  var policy = {
    floorMs: POLL_FLOOR_MS,
    ceilingMs: POLL_CEILING_MS,
    divisor: POLL_DIVISOR,
  };
  // Must be initialised, not just declared. Left undefined, the first
  // schedule() computes Math.round(undefined * (1 + jitter)) = NaN,
  // setTimeout clamps a NaN delay to 1ms, and the client retries flat-out —
  // hundreds of times a second — for as long as the first /status call keeps
  // failing, which is exactly when the origin is unhealthy.
  var nextIntervalMs = POLL_FLOOR_MS;

  function num(v, lo, hi) {
    return typeof v === "number" && isFinite(v) && v >= lo && v <= hi ? v : null;
  }

  // No policy published, or one with a bad field, leaves the current policy
  // standing: the fixed-interval default for a client that has not adopted a
  // real one yet, or the last good policy for one that has. The server never
  // sends a partial policy, so one bad field makes the whole document
  // suspect and none of it is adopted — and reverting an already-adaptive
  // client to a fixed interval mid-event because one poll came back
  // malformed would be worse than trusting the policy it already has.
  function adoptPolicy(p) {
    if (!p || typeof p !== "object") {
      return;
    }
    var f = num(p.floor_ms, MIN_MS, MAX_MS);
    var c = num(p.ceiling_ms, MIN_MS, MAX_MS);
    var d = num(p.divisor, 1, MAX_DIVISOR);
    if (f === null || c === null || d === null || c < f) {
      return;
    }
    policy = { floorMs: f, ceilingMs: c, divisor: d };
  }

  var el = {
    headline: document.getElementById("headline"),
    subhead: document.getElementById("subhead"),
    stats: document.getElementById("stats"),
    position: document.getElementById("position"),
    serving: document.getElementById("serving"),
    ahead: document.getElementById("ahead"),
    eta: document.getElementById("eta"),
    bar: document.getElementById("bar"),
    updated: document.getElementById("updated"),
    updatedAt: document.getElementById("updated-at"),
    fill: document.getElementById("fill"),
    broadcast: document.getElementById("broadcast"),
    note: document.getElementById("note"),
  };

  // A UUIDv7: 48-bit big-endian timestamp, version 7, variant 10, random rest.
  // Time-ordered so ids sort by arrival, and the server re-validates the shape
  // before it claims a position for one.
  function uuidv7() {
    var bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    var now = Date.now();
    // Math.floor keeps this exact: Date.now() is well inside 2^53.
    bytes[0] = Math.floor(now / 1099511627776) & 0xff;
    bytes[1] = Math.floor(now / 4294967296) & 0xff;
    bytes[2] = Math.floor(now / 16777216) & 0xff;
    bytes[3] = Math.floor(now / 65536) & 0xff;
    bytes[4] = Math.floor(now / 256) & 0xff;
    bytes[5] = now & 0xff;
    bytes[6] = (bytes[6] & 0x0f) | 0x70; // version 7
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10
    var hex = [];
    for (var i = 0; i < 16; i++) {
      hex.push((bytes[i] + 0x100).toString(16).slice(1));
    }
    return (
      hex.slice(0, 4).join("") +
      "-" +
      hex.slice(4, 6).join("") +
      "-" +
      hex.slice(6, 8).join("") +
      "-" +
      hex.slice(8, 10).join("") +
      "-" +
      hex.slice(10, 16).join("")
    );
  }

  // Storage is a chain, not a single tier: localStorage throws in private mode
  // and when site data is blocked, and a visitor who cannot persist their id
  // mints a fresh one on every reload — which since issue #59 also burns a
  // pre-queue index, inflating the cohort with indices nobody holds.
  //
  // The memory tier always succeeds and never survives a reload. That makes
  // "no exception was thrown" useless as a durability test, so each tier reads
  // the value back and only the ones that return it count.
  var memoryStore = {};

  function localGet(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function localSet(key, value) {
    try {
      window.localStorage.setItem(key, value);
      return localGet(key) === value;
    } catch (e) {
      return false;
    }
  }

  function sessionGet(key) {
    try {
      return window.sessionStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function sessionSet(key, value) {
    try {
      window.sessionStorage.setItem(key, value);
      return sessionGet(key) === value;
    } catch (e) {
      return false;
    }
  }

  function cookieGet(name) {
    try {
      var parts = String(document.cookie || "").split(";");
      for (var i = 0; i < parts.length; i++) {
        var part = parts[i].trim();
        if (part.slice(0, name.length + 1) === name + "=") {
          return decodeURIComponent(part.slice(name.length + 1));
        }
      }
    } catch (e) {
      /* fall through to the next tier */
    }
    return null;
  }

  // The identity's cookie is scoped to the waiting room's own path: at "/"
  // the browser would send the secret to the origin on every request. The
  // room's API shares that path (issue #251) and is the secret's destination
  // anyway.
  function cookiePath(name) {
    return name === IDENTITY_KEY ? "/_wr/" : "/";
  }

  function cookieSet(name, value) {
    try {
      document.cookie =
        name +
        "=" +
        encodeURIComponent(value) +
        ";path=" +
        cookiePath(name) +
        ";max-age=86400;SameSite=Lax;Secure";
      return cookieGet(name) === value;
    } catch (e) {
      return false;
    }
  }

  function readStored(key) {
    var value = localGet(key);
    if (value !== null) {
      return value;
    }
    value = cookieGet(key);
    if (value !== null) {
      return value;
    }
    value = sessionGet(key);
    if (value !== null) {
      return value;
    }
    return Object.prototype.hasOwnProperty.call(memoryStore, key)
      ? memoryStore[key]
      : null;
  }

  function writeStored(key, value) {
    localSet(key, value) || cookieSet(key, value) || sessionSet(key, value);
    memoryStore[key] = value;
  }

  function removeStored(key) {
    try {
      window.localStorage.removeItem(key);
    } catch (e) {
      /* tier unavailable */
    }
    try {
      window.sessionStorage.removeItem(key);
    } catch (e) {
      /* tier unavailable */
    }
    try {
      document.cookie =
        key + "=;path=" + cookiePath(key) + ";max-age=0;SameSite=Lax;Secure";
    } catch (e) {
      /* tier unavailable */
    }
    delete memoryStore[key];
  }

  // The gate sends a refused visitor here with a reason (r=). Arriving that
  // way moments after this page redeemed means the gate turned the fresh
  // session down -- a rotated key, blocked cookies, an event id that does not
  // match -- and redeeming again would only repeat it, at navigation speed
  // (issue #247). Counted once, at load: by the time redeem() runs again the
  // window may have passed, and the loop it guards is the immediate one.
  // r=ip has its own, stricter guard in redeem().
  var bounces = (function () {
    if (!/[?&]r=/.test(window.location.search || "")) {
      return 0;
    }
    var m = /^(\d+)\.(\d+)$/.exec(readStored(REDEEMED_KEY) || "");
    if (!m || Date.now() - Number(m[1]) >= BOUNCE_WINDOW_MS) {
      return 0;
    }
    return Number(m[2]) + 1;
  })();

  var requestId = null;
  var secret = null;

  function base64url(bytes) {
    var abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    var out = "";
    for (var i = 0; i < bytes.length; i += 3) {
      var n = (bytes[i] << 16) | ((bytes[i + 1] || 0) << 8) | (bytes[i + 2] || 0);
      out += abc[(n >> 18) & 63] + abc[(n >> 12) & 63];
      if (i + 1 < bytes.length) {
        out += abc[(n >> 6) & 63];
      }
      if (i + 2 < bytes.length) {
        out += abc[n & 63];
      }
    }
    return out;
  }

  // Resolves the identity this page queues under, before the first poll. The
  // id and its secret survive a reload together through the storage chain
  // above; a visitor whose every tier fails mints a fresh pair and so takes a
  // new place rather than recovering their old one.
  function resolveIdentity() {
    var match = IDENTITY_RE.exec(readStored(IDENTITY_KEY) || "");
    if (match) {
      requestId = match[1];
      secret = match[2];
      return;
    }
    var bytes = new Uint8Array(32);
    crypto.getRandomValues(bytes);
    requestId = uuidv7();
    secret = base64url(bytes);
    writeStored(IDENTITY_KEY, requestId + "." + secret);
  }

  // base64url(SHA-256(secret)) over the secret's ASCII, which is what
  // /_wr/v1/join stores and wr_common::PossessionSecret::digest recomputes.
  var digestPromise = null;
  function secretDigest() {
    if (!digestPromise) {
      var ascii = new Uint8Array(secret.length);
      for (var i = 0; i < secret.length; i++) {
        ascii[i] = secret.charCodeAt(i);
      }
      digestPromise = crypto.subtle
        .digest("SHA-256", ascii)
        .then(function (buf) {
          return base64url(new Uint8Array(buf));
        });
    }
    return digestPromise;
  }

  // Learned from /status. The join request is validated at the edge against a
  // schema requiring a non-empty event_id, so joining before this is known
  // produces a 400, no queue message, and a visitor who waits forever for a
  // position that was never claimed.
  var eventId = null;
  var admitting = false;

  // Where a successful redemption navigates: the request the gate refused
  // before sending this visitor here, preserved across the round trip by its
  // own next= query parameter (docs/adr/0021-edge-function-gate.md §8) —
  // this replaces a client-side bounce guard, which needed a loop to survive
  // in the first place because nothing used to carry the destination through
  // the redirect. Falls back to "/" when next is absent or unsafe: a direct
  // visit to the waiting page carries no next, and a value starting "//"
  // would navigate off-site rather than to a same-origin path.
  function nextDestination() {
    var match = /[?&]next=([^&]*)/.exec(window.location.search || "");
    if (!match) {
      return "/";
    }
    var next;
    try {
      next = decodeURIComponent(match[1]);
    } catch (e) {
      return "/";
    }
    // A browser strips tabs and newlines out of a URL before resolving it, so
    // "/<tab>/host" navigates to "//host" while its second character here is
    // the tab (issue #244). No path this page is sent back to carries a raw
    // control character, so one anywhere refuses the whole value -- the same
    // rule the no-JavaScript path applies (crates/nojs safe_next).
    if (/[\x00-\x1f\x7f]/.test(next)) {
      return "/";
    }
    // A browser reads "/\host" as "//host", which leaves the site.
    if (next.charAt(0) !== "/" || next.charAt(1) === "/" || next.charAt(1) === "\\") {
      return "/";
    }
    return next;
  }

  function jitter() {
    return Math.round(nextIntervalMs * (1 + Math.random() * JITTER_FRACTION));
  }

  function say(headline, sub) {
    el.headline.textContent = headline;
    if (sub !== undefined) {
      el.subhead.textContent = sub;
    }
  }

  // The edge web ACL (ADR-0038) answers an API call that carries no valid WAF
  // token with a challenge (202) or, past an address's rate limit, a CAPTCHA
  // (405), both marked by this header. The 202 is the trap: read by status
  // alone it is a successful join, and the page would poll for a place nothing
  // was ever asked to write. Neither can be solved from fetch(), only by
  // navigating, so the page goes to the verify page: WAF challenges that
  // navigation (silently) or shows the CAPTCHA, the browser gets its token,
  // and the verify page sends it back here, where the request id is picked
  // back up from storage. The challenge lives on its own page, not this one,
  // because this page also serves visitors without JavaScript (ADR-0037).
  var WAF_VERIFY_PATH = "/_wr/verify.html";
  var WAF_RENEW_WINDOW_MS = 60000;

  function wafAction(r) {
    return r.headers && r.headers.get ? r.headers.get("x-amzn-waf-action") : null;
  }

  function renewWafToken() {
    stop();
    // A second WAF answer within a minute of renewing one means the
    // token is not sticking — most likely cookies are blocked for this site.
    // Renewing again would loop forever, so say what is wrong instead. The
    // marker rides in the URL because storage may be exactly what is failing.
    var search = window.location.search || "";
    var last = /[?&]waf=(\d+)/.exec(search);
    if (last && Date.now() - Number(last[1]) < WAF_RENEW_WINDOW_MS) {
      say(
        "We couldn't verify this browser",
        "Allow cookies for this site, then reload this page to keep your place in line."
      );
      return;
    }
    var kept = search
      .replace(/^\?/, "")
      .split("&")
      .filter(function (pair) {
        return pair && pair.indexOf("waf=") !== 0;
      });
    kept.push("waf=" + Date.now());
    say("Checking your browser…", "This takes a moment. You keep your place.");
    var back = window.location.pathname + "?" + kept.join("&");
    window.location.replace(WAF_VERIFY_PATH + "?back=" + encodeURIComponent(back));
  }

  // Rejects rather than resolving, so whichever chain made the call stops
  // where it is; stop() above has already cancelled the next poll.
  function checkWaf(r) {
    if (wafAction(r)) {
      renewWafToken();
      throw new Error("waf " + wafAction(r));
    }
    return r;
  }

  function getJSON(url) {
    return fetch(url, {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    }).then(checkWaf).then(function (r) {
      return r.json().then(function (body) {
        return { status: r.status, body: body };
      });
    });
  }

  function postJSON(url, payload) {
    return fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(payload),
    }).then(checkWaf).then(function (r) {
      return r
        .json()
        .catch(function () {
          return {};
        })
        .then(function (body) {
          return { status: r.status, body: body };
        });
    });
  }

  // Claimed once per visitor, not once per page load, so a reload does not send
  // a second message for a place already held.
  //
  // The claim is not permanent. A position can stop existing after it was
  // taken — the row expired, or the event was reset out from under a browser
  // holding the id in localStorage — and a client that treats "joined" as
  // final polls a row that will never come back, forever. So the flag is
  // cleared after enough consecutive misses and the next tick re-joins. The
  // position write is guarded on the request id not already existing, so
  // re-joining is a no-op when the row is merely slow to arrive.
  // An in-memory mirror of JOINED_KEY. Without it, a visitor whose storage all
  // fails reads null on every poll and re-POSTs /_wr/v1/join for the entire wait —
  // one join per poll interval, per waiter, for the whole event.
  var joinedId = null;

  // A join can fail permanently for structural reasons: an ingest that rejects
  // every record, or an unconfigured event. Backoff
  // alone only slows that down — it never stops — so both counters below end in
  // a terminal state that calls stop().
  var MAX_JOIN_ATTEMPTS = 8;
  var MAX_REJOIN_CYCLES = 3;
  var JOIN_BACKOFF_CEILING_MS = 120000;
  var joinAttempts = 0;
  // Joins answered 429. Paces the retry; never ends it.
  var joinThrottles = 0;
  var rejoinCycles = 0;
  var joinBlockedUntil = 0;

  function giveUp(message) {
    stop();
    el.note.textContent = message;
  }

  function join(countdown) {
    if (joinedId === requestId || readStored(JOINED_KEY) === requestId) {
      joinedId = requestId;
      return Promise.resolve();
    }
    if (Date.now() < joinBlockedUntil) {
      return Promise.resolve();
    }
    if (joinAttempts >= MAX_JOIN_ATTEMPTS) {
      giveUp(
        "We could not get you into the queue. Please reopen the link you were sent."
      );
      return Promise.resolve();
    }
    if (!crypto.subtle) {
      // Only an insecure (http) page lacks it, and the join cannot be made
      // without the digest.
      giveUp("This page must be opened over https to hold your place in line.");
      return Promise.resolve();
    }
    joinAttempts += 1;
    return secretDigest().then(function (h) {
      return postJSON("/_wr/v1/join", {
        request_id: requestId,
        event_id: eventId,
        h: h,
      });
    }).then(function (res) {
      // Only a request the ingest accepted claims a place. postJSON resolves
      // for any status, so recording unconditionally marks a rejected join as
      // done and leaves the visitor polling a position nothing will ever write.
      if (res.status >= 200 && res.status < 300) {
        joinedId = requestId;
        joinAttempts = 0;
        joinThrottles = 0;
        writeStored(JOINED_KEY, requestId);
        return;
      }
      // A 429 is the API smoothing a burst, not refusing this visitor: when a
      // whole crowd arrives at once the gateway takes them at its own rate and
      // everyone else is told to come back. That can take minutes at a million
      // people, so it never counts towards giving up -- a visitor who is
      // turned away for being one of many must not be told the queue is
      // broken. Structural failures still count.
      if (res.status === 429) {
        joinAttempts -= 1;
        joinThrottles += 1;
      }
      // Exponential, capped. The cap bounds the rate; MAX_JOIN_ATTEMPTS is what
      // bounds the total. Spread by half either way, because every visitor
      // turned away in the same second would otherwise come back in the same
      // second, and the gateway would sit idle in between. A throttled
      // registration during the countdown does not grow: it has to land before
      // the open counts the cohort, or the visitor is numbered behind all of it.
      var exponent =
        countdown && res.status === 429 ? 0 : Math.min(joinAttempts + joinThrottles, 7) - 1;
      var backoff =
        Math.min(JOIN_BACKOFF_CEILING_MS, policy.floorMs * Math.pow(2, exponent)) *
        (0.5 + Math.random());
      joinBlockedUntil = Date.now() + backoff;
    });
  }

  // How many consecutive "no position for this id" answers to accept before
  // concluding the place is gone rather than late.
  //
  // This has to clear the ingest's worst-case delivery latency, not its typical
  // one. A Lambda event source mapping with a batch window set may wait up to
  // 20 seconds before invoking on a quiet queue, which AWS documents and which
  // no amount of tuning below 20s avoids. At roughly six seconds a poll, four
  // misses lands inside that window, so the recovery fired on every normal join
  // and sent a duplicate message for a place that was simply still in flight.
  var MAX_MISSES = 8;
  // What a poll resolves to while no join has been accepted.
  var UNJOINED = {};
  var misses = 0;

  // A place in line, once known, never changes: it is derived from an opened
  // permutation for a pre-queue registrant and from a claimed row for a live
  // joiner. Only the serving cursor moves, and /status carries that. So the
  // number is fetched once and held, and every later poll reads /status alone —
  // which is keyed on path, so the edge collapses the whole waiting room into
  // about one origin request a second. Re-fetching it each poll instead would
  // put one request per visitor per interval on an endpoint that cannot
  // collapse, because its answer is per visitor.
  var knownPosition = null;
  var knownLiveJoin = false;

  // Progress is shown against where this visitor started, not against the whole
  // cohort. Someone who joined 900,000 deep in a million-person queue is moving
  // steadily, but a cohort-wide bar would sit near empty and appear stuck for
  // the entire wait.
  var aheadAtStart = null;

  // The wait currently on screen, held so it is not allowed to drift upward on
  // noise. See settleWait.
  var shownWait = null;

  // Stamped from the client's own clock on every successful /status, whether or
  // not this visitor has a number yet. It reports that the page is still
  // talking to the room — which is most in doubt before a position arrives and
  // the rest of the page is still showing dashes.
  function markUpdated() {
    el.updated.hidden = false;
    el.updatedAt.textContent = new Date().toLocaleTimeString();
  }

  function forgetPosition() {
    knownPosition = null;
    knownLiveJoin = false;
    // The bar measures travel from where this visitor started, so a new place
    // in line needs a new starting point — keeping the old one would show
    // progress already made towards a position they no longer hold.
    aheadAtStart = null;
    try {
      window.localStorage.removeItem(AHEAD_AT_START_KEY);
    } catch (e) {
      // Storage unavailable; the in-memory reset above is what matters.
    }
    // A new place is further back, so the honest estimate jumps up. Clearing
    // this lets it: the rise band exists to absorb noise, and holding a wait
    // from the abandoned place would suppress a real and much longer one.
    shownWait = null;
  }

  // Everyone learns their number at once when the event opens, so asking the
  // instant the queue goes live would turn a whole cohort into one spike
  // against an endpoint with no collapsing. Each client waits a random slice of
  // a window sized to the cohort: a small room barely waits, a large one
  // spreads. Nobody loses their place by waiting — the number already exists,
  // and the page keeps showing the queue moving meanwhile.
  //
  // The window is capped because a visitor should not stare at a page that will
  // not say where they are. Past roughly FIRST_ASK_TARGET_RPS * the cap, the
  // spread alone stops being enough and the account's API Gateway throttle
  // (10,000 requests a second by default, raisable on request) has to go up.
  var FIRST_ASK_TARGET_RPS = 5000;
  var FIRST_ASK_MAX_SPREAD_MS = 120000;
  var firstAskAt = null;

  function scheduleFirstAsk(participants) {
    if (firstAskAt !== null) {
      return;
    }
    var spread = Math.min(
      FIRST_ASK_MAX_SPREAD_MS,
      ((participants || 0) / FIRST_ASK_TARGET_RPS) * 1000
    );
    firstAskAt = Date.now() + Math.floor(Math.random() * spread);
  }

  // How fast the cursor is actually moving, in positions per second, measured
  // from /status rather than taken on trust. The operator's target rate says
  // what the origin was asked to absorb; the controller then releases extra
  // positions for admissions nobody took (ADR-0039), so the cursor's real
  // speed is not the target. A visitor should be told what is happening,
  // not what was intended.
  //
  // Measured across a window rather than between consecutive polls. The cursor
  // moves in steps — it jumps once per control interval and sits still in
  // between — so a rate taken from one pair of polls alternates between a spike
  // and a stall, and the estimate built on it swings by minutes either way.
  // Comparing the ends of a window that spans several intervals averages the
  // steps out without needing to know the interval.
  var RATE_WINDOW_MS = 60000;
  var RATE_MIN_SPAN_MS = 30000;
  var cursorSamples = [];

  function observeCursor(serving) {
    var now = Date.now();
    // The cursor never retreats in normal operation. If it does, the event was
    // reset underneath us and every sample describes a queue that is gone.
    if (
      cursorSamples.length &&
      serving < cursorSamples[cursorSamples.length - 1].serving
    ) {
      cursorSamples = [];
      shownWait = null;
    }
    cursorSamples.push({ at: now, serving: serving });
    while (
      cursorSamples.length > 2 &&
      now - cursorSamples[0].at > RATE_WINDOW_MS
    ) {
      cursorSamples.shift();
    }
  }

  function measuredRate() {
    if (cursorSamples.length < 2) {
      return null;
    }
    var first = cursorSamples[0];
    var last = cursorSamples[cursorSamples.length - 1];
    var seconds = (last.at - first.at) / 1000;
    // Under a span this short the window may not contain a whole control
    // interval, and a rate read off a partial one is worse than the operator's
    // declared target.
    if (seconds < RATE_MIN_SPAN_MS / 1000) {
      return null;
    }
    var moved = last.serving - first.serving;
    return moved > 0 ? moved / seconds : null;
  }

  // Prefer what was observed over what was merely declared, and share that
  // choice between the on-screen ETA and the poll interval so the two never
  // disagree: the controller releases extra positions for no-shows, so
  // the cursor's real speed differs from the operator's target, and the
  // interval should track what is actually happening rather than what was
  // only intended.
  function currentRate(targetRate) {
    var rate = measuredRate();
    return rate !== null ? rate : targetRate;
  }

  // What the room expects to admit a second before any movement has been
  // seen: a full origin turns over once per session length.
  function expectedRate(s) {
    return s.origin_concurrency > 0 && s.session_seconds > 0
      ? s.origin_concurrency / s.session_seconds
      : 0;
  }

  // How this client turns "distance to the front" into a poll interval (#69).
  // Proportional to the wait this visitor can see, floored so the
  // front of the queue stays responsive, and capped so a visitor deep in a
  // huge queue is not made to wait the length of the queue between polls.
  // Without a wait to be proportional to — no rate, no known position —
  // there is nothing to divide, so this falls back to the floor, which is
  // the interval every client used before this change.
  function intervalFor(waitSeconds) {
    if (!(waitSeconds > 0)) {
      return policy.floorMs;
    }
    return Math.min(
      policy.ceilingMs,
      Math.max(policy.floorMs, (waitSeconds * 1000) / policy.divisor)
    );
  }

  function intervalForPosition(ahead, rate) {
    return rate > 0 ? intervalFor(ahead / rate) : policy.floorMs;
  }

  // How far ahead this visitor is right now, from the position last learned
  // and the cursor on this poll. null before a position is known — there is
  // nothing to measure distance from yet.
  function aheadNow(serving) {
    return knownPosition === null ? null : Math.max(0, knownPosition - serving);
  }

  // The estimate falls freely and rises only when the queue has genuinely
  // slowed. Anything smaller than the band is measurement noise, and a wait
  // that creeps upward in front of someone reads as the system losing their
  // place — the one thing this page exists to reassure them about.
  var WAIT_RISE_BAND = 1.25;

  function settleWait(seconds) {
    if (shownWait === null || seconds <= shownWait) {
      shownWait = seconds;
    } else if (seconds > shownWait * WAIT_RISE_BAND) {
      shownWait = seconds;
    }
    return shownWait;
  }

  // Deliberately coarse. The inputs are a smoothed rate and a queue whose
  // drain the operator can change at any moment, so a figure to the second
  // claims precision that is not there, and a countdown that visibly stalls
  // reads as broken.
  function humanWait(seconds) {
    if (seconds < 30) {
      return "under a minute";
    }
    var minutes = Math.ceil(seconds / 60);
    if (minutes <= 60) {
      return "about " + minutes + " min";
    }
    var hours = Math.round(seconds / 3600);
    return "over " + hours + (hours === 1 ? " hour" : " hours");
  }

  // Seconds from now until an absolute epoch published on /status, or null
  // when there is no usable one. Validated like adoptPolicy does: a malformed
  // field must leave the page on its unscheduled text rather than rendering
  // NaN at every waiting visitor.
  //
  // The comparison is against the visitor's own clock, which may be wrong. It
  // is only ever used to choose wording, never to decide that the event has
  // opened -- that is the next poll's job -- so a skewed clock costs a
  // slightly early or late label and nothing else.
  function secondsUntil(epochSecs) {
    if (typeof epochSecs !== "number" || !isFinite(epochSecs)) {
      return null;
    }
    return epochSecs - Math.floor(Date.now() / 1000);
  }

  function renderEta(ahead, targetRate) {
    // Falls back to the operator's target so the first poll says something,
    // rather than leaving a dash for the half minute it takes to watch the
    // cursor move.
    var rate = currentRate(targetRate);
    if (!rate || rate <= 0) {
      // No rate set means the operator is not admitting anyone yet, which is
      // not a long wait — it is an unknown one, and saying so is honest.
      el.eta.textContent = "—";
      return;
    }
    el.eta.textContent =
      ahead === 0 ? "any moment" : humanWait(settleWait(ahead / rate));
  }

  function forgetJoin() {
    misses = 0;
    forgetPosition();
    joinedId = null;
    joinAttempts = 0;
    joinBlockedUntil = 0;
    removeStored(JOINED_KEY);
  }

  function renderQueue(position, serving, participants, targetRate) {
    el.stats.hidden = false;
    el.position.textContent = position.toLocaleString();
    el.serving.textContent = serving.toLocaleString();
    var ahead = Math.max(0, position - serving);
    el.ahead.textContent = ahead.toLocaleString();
    renderEta(ahead, targetRate);

    if (aheadAtStart === null) {
      // Recovered across a reload, so closing the tab and reopening it does not
      // reset the bar to empty on a visitor who has already waited. A stored
      // value below the current distance belongs to a place this visitor no
      // longer holds, so it is discarded rather than shown as progress.
      var stored = Number(readStored(AHEAD_AT_START_KEY));
      aheadAtStart = stored >= ahead && stored > 0 ? stored : ahead;
      writeStored(AHEAD_AT_START_KEY, String(aheadAtStart));
    }
    // A visitor already at the front has nothing to travel, so any fraction
    // would be arbitrary; show the bar full rather than empty.
    if (aheadAtStart > 0) {
      el.bar.hidden = false;
      var done = ((aheadAtStart - ahead) / aheadAtStart) * 100;
      el.fill.style.width = Math.min(100, Math.max(0, done)).toFixed(1) + "%";
    } else if (participants && participants > 0) {
      el.bar.hidden = false;
      el.fill.style.width = "100.0%";
    }

    // The headline follows the admission rule, not `ahead`. A visitor is
    // admitted once position < serving, but `ahead` reaches zero one release
    // earlier — so keying the headline off `ahead` promises entry while the
    // gate still refuses, and the promise stands until the cursor moves again.
    // At the end of a cohort it never does: the controller stops at
    // queue_counter + 1, so the last visitor in line sits at ahead === 0.
    if (position < serving) {
      say("You're next", "Letting you through…");
    } else if (ahead === 0) {
      say("You're next", "Waiting for the next release…");
    } else {
      say(
        "You're in line",
        ahead === 1
          ? "1 person ahead of you."
          : ahead.toLocaleString() + " people ahead of you."
      );
    }
  }

  function showBroadcast(message) {
    if (message) {
      el.broadcast.hidden = false;
      el.broadcast.textContent = message;
    } else {
      el.broadcast.hidden = true;
    }
  }

  // The moment of admission: collect the cookies, then reload. The reload goes
  // to the origin because CloudFront now accepts the request.
  function redeem() {
    if (admitting) {
      return Promise.resolve();
    }
    if (bounces >= MAX_BOUNCES) {
      // Twice admitted, twice turned back. A reload after the window starts
      // the count again, which is what the message asks for (issue #247).
      say(
        "We let you in, but the site turned you back",
        "Please reload in a minute."
      );
      stop();
      return Promise.resolve();
    }
    admitting = true;
    return postJSON("/_wr/v1/generate_token", {
      request_id: requestId,
      event_id: eventId,
      secret: secret,
    }).then(function (res) {
      if (res.status === 200 && res.body.admitted) {
        // The gate refused a session from another network (IP binding, issue
        // #61) and this page just re-minted it. A second such refusal inside
        // a minute means the new session is not holding either: stop, rather
        // than bounce between the gate and this page.
        if (/[?&]r=ip(&|$)/.test(window.location.search || "")) {
          var last = Number(readStored(IP_REBIND_KEY)) || 0;
          if (Date.now() - last < IP_REBIND_WINDOW_MS) {
            say(
              "Your connection keeps changing",
              "This site only lets a pass work from the network it was issued to. Stay on one connection (Wi-Fi or mobile data) and reload this page."
            );
            stop();
            return;
          }
          writeStored(IP_REBIND_KEY, String(Date.now()));
        }
        writeStored(REDEEMED_KEY, Date.now() + "." + bounces);
        say("You're through", "Taking you to the site…");
        // Replace so the waiting page does not sit in the back history.
        window.location.replace(nextDestination());
        return;
      }
      // Not admitted this time: the origin has had its share for the moment
      // (a 425 while the admission budget is spent, ADR-0039), or the operator
      // paused. Fall back to polling rather than hammering.
      admitting = false;
      // A 425 is a slot not yet free: the controller releases the people for
      // a slot a little before its session ends. Ask again at the floor, so the
      // slot is taken within seconds of coming free; the crowd asking is only
      // the people released for the next slots, so this is not a storm. A 429
      // or a 5xx is a throttle or an overloaded function, and retrying it at
      // the floor only feeds the overload, so that backs off.
      var busy = res.status === 429 || res.status >= 500;
      capacityMisses = busy ? capacityMisses + 1 : 0;
      if (res.status === 403) {
        // The secret this browser holds is not the one the place was taken
        // with (issue #62). Nothing on this page can recover that.
        say(
          "We couldn't confirm this place is yours",
          "Open the waiting room again from the link you were given to take a new place."
        );
        stop();
      } else if (res.status === 410) {
        // This place was used and the visit it bought is over (issue #237).
        // It cannot be redeemed again, so drop it: the next load of this page
        // takes a new place instead of asking for the old one forever.
        removeStored(IDENTITY_KEY);
        removeStored(JOINED_KEY);
        say(
          "Your visit has ended",
          "Reload this page to take a new place in line."
        );
        stop();
      }
    }, function (err) {
      // Failed in flight (a network error, or an error page that is not
      // JSON). Clear the flag before tick()'s catch retries, or every later
      // redeem() returns early and nothing is ever scheduled again.
      admitting = false;
      throw err;
    });
  }

  // Consecutive "at capacity" answers to a redemption. See redeem().
  var capacityMisses = 0;

  var timer = null;
  var stopped = false;
  // Set at the start of every /status fetch, so the visibility handler can
  // tell how long it has actually been since the last poll.
  var lastPollAt = 0;
  // True from the moment a poll starts until its whole chain — /status, the
  // join()/queue_num() that may follow, an admission attempt — settles. The
  // visibility handler can fire tick() out of band with a chain already
  // running; without this guard the two chains both call join() before
  // either has written JOINED_KEY, taking two positions for one visitor.
  var inFlight = false;
  // Set when the visibility handler wants to poll but tick() is still
  // in-flight (inFlight above), so the request would otherwise be dropped
  // outright: a visitor who returns while a poll is mid-flight then waited
  // out the running chain's own, possibly much longer, interval instead of
  // being caught up promptly. schedule() consumes this once the running
  // chain settles.
  var catchUpWanted = false;
  // The interval the last failed poll retried at; 0 once a poll succeeds.
  var backoffMs = 0;

  function stop() {
    stopped = true;
    if (timer) {
      window.clearTimeout(timer);
      timer = null;
    }
  }

  function schedule() {
    if (stopped) {
      return;
    }
    if (timer) {
      window.clearTimeout(timer);
    }
    // A hidden tab costs requests and sees nothing. Leave no timer running;
    // the visibilitychange handler below restarts polling on return. Nothing
    // is left for catchUpWanted to carry either: hidden again means the
    // handler recomputes freshly from lastPollAt the next time this tab is
    // shown.
    if (document.hidden) {
      catchUpWanted = false;
      timer = null;
      return;
    }
    if (catchUpWanted) {
      catchUpWanted = false;
      // Honour the dropped poll now, measured from when this chain's own
      // poll actually started — the floor stays the bound, but the wait for
      // it does not reset just because this chain was already running.
      timer = window.setTimeout(
        tick,
        Math.max(0, policy.floorMs - (Date.now() - lastPollAt))
      );
      return;
    }
    timer = window.setTimeout(tick, jitter());
  }

  function tick() {
    if (inFlight) {
      catchUpWanted = true;
      return;
    }
    inFlight = true;
    lastPollAt = Date.now();
    getJSON("/_wr/v1/status")
      .then(function (res) {
        if (res.status !== 200) {
          throw new Error("status " + res.status);
        }
        var s = res.body;
        markUpdated();
        showBroadcast(s.message);
        // Sampled from every poll, including the ones before this visitor knows
        // their own number, so an estimate is ready the moment there is
        // something to estimate.
        observeCursor(s.serving_position);
        // Before any serving_state branch: a stale policy would otherwise
        // govern the closed/paused early returns below.
        adoptPolicy(s.poll_policy);
        eventId = s.event_id || eventId;
        if (!eventId) {
          // Nothing to join yet; the next poll tries again.
          nextIntervalMs = policy.floorMs;
          say("Getting your place in line…", "Just a moment.");
          return schedule();
        }

        if (s.serving_state === "closed") {
          el.stats.hidden = true;
          el.bar.hidden = true;
          // The freshness line stays: a closed event keeps polling for the
          // opening, which is exactly when a visitor wants to know the page is
          // still watching rather than dead.
          // A closed event has not dealt this visitor a number, and if one was
          // held from an earlier run of the same page it belongs to a cohort
          // that no longer exists — an operator who resets an event opens a new
          // one with a different permutation.
          forgetPosition();
          nextIntervalMs = policy.ceilingMs;
          // A scheduled event counts down to its start; an unscheduled one has
          // no moment to name, so it keeps the original text. Branch rather
          // than render then overwrite -- both write the same two elements.
          var startsIn = secondsUntil(s.starts_at);
          if (startsIn === null) {
            say(
              "The event isn't open yet",
              "This page updates on its own when it opens."
            );
          } else if (startsIn > 0) {
            say(
              "Opens in " + humanWait(startsIn),
              "This page updates on its own when it opens."
            );
          } else {
            // The start has passed but the open has not landed yet. Saying
            // "opens in under a minute" forever would read as stuck.
            say("Opening now", "This page updates on its own when it opens.");
          }
          if (s.phase === "pre_queue") {
            // Registration during the countdown is a single direct write
            // (join() no-ops on reload via JOINED_KEY), not a poll: the
            // /queue_num call below stays skipped, since that endpoint
            // answers 409 until the open and a poll against it would only
            // churn the catch handler. Must return the promise chain, or a
            // rejected join never reaches tick()'s catch and schedule()
            // never runs again.
            return join(true)
              .then(function () {
                // Until the registration lands, ask again at the floor rather
                // than the closed event's ceiling.
                if (joinedId !== requestId) {
                  nextIntervalMs = policy.floorMs;
                }
              })
              .then(schedule);
          }
          return schedule();
        }
        if (s.serving_state === "paused") {
          say(
            "Admission is paused",
            "You keep your place in line. This page updates when it resumes."
          );
          // A near-front visitor stays near the floor across a pause instead
          // of drifting to the ceiling: the cursor is frozen while paused, so
          // `ahead` is stable, and the operator's rate is preserved across
          // the hold, so this is the same estimate the visitor had a moment
          // ago. A visitor with no position yet has nothing to be
          // proportional to, so the ceiling is the right default there.
          var pausedAhead = aheadNow(s.serving_position);
          nextIntervalMs =
            pausedAhead === null
              ? policy.ceilingMs
              : intervalForPosition(pausedAhead, currentRate(expectedRate(s)));
          return schedule();
        }
        if (s.serving_state === "fail_open") {
          say("Come on in", "Taking you to the site…");
          window.location.replace("/");
          return;
        }

        scheduleFirstAsk(s.participant_count);

        return join()
          .then(function () {
            if (knownPosition !== null) {
              // Already known, and it cannot have changed. Serve it from here
              // so the poll costs only the /status request above.
              return {
                status: 200,
                body: { position: knownPosition, live_join: knownLiveJoin },
              };
            }
            if (joinedId !== requestId) {
              // No join has been accepted yet, so there is no number to ask
              // for. Asking anyway costs a request per visitor per poll on an
              // endpoint the edge cannot collapse, and when a crowd arrives at
              // once those requests crowd out the joins they are waiting on.
              return UNJOINED;
            }
            if (Date.now() < firstAskAt) {
              return null;
            }
            return getJSON(
              "/_wr/v1/queue_num?request_id=" + encodeURIComponent(requestId)
            );
          })
          .then(function (q) {
            if (q === UNJOINED) {
              nextIntervalMs = policy.floorMs;
              if (!stopped) {
                say("Getting your place in line…", "Just a moment.");
              }
              return schedule();
            }
            if (q === null) {
              // Inside the spread window. The queue is open and moving, and
              // saying so is more honest than a spinner — this visitor has a
              // number already, it just has not been asked for yet.
              nextIntervalMs = policy.floorMs;
              say("You're in line", "Finding your number…");
              return schedule();
            }
            if (q.status === 404) {
              misses += 1;
              nextIntervalMs = policy.floorMs;
              if (misses >= MAX_MISSES) {
                // The place is gone rather than late. Drop the claim so the
                // next tick takes a new one; keeping the same request id means
                // a row that does reappear is still ours.
                rejoinCycles += 1;
                if (rejoinCycles > MAX_REJOIN_CYCLES) {
                  // Re-joining has stopped being recovery: with a derived id it
                  // resolves to the same row every time, so a record the ingest
                  // keeps discarding will never appear however often we ask.
                  giveUp(
                    "We could not get you into the queue. Please reopen the link you were sent."
                  );
                  return;
                }
                forgetJoin();
                say("Getting your place in line…", "Taking a new place.");
              } else {
                say("Getting your place in line…", "Just a moment.");
              }
              return schedule();
            }
            misses = 0;
            if (q.status !== 200) {
              throw new Error("queue_num " + q.status);
            }
            knownPosition = q.body.position;
            knownLiveJoin = q.body.live_join;
            renderQueue(
              q.body.position,
              s.serving_position,
              s.participant_count,
              expectedRate(s)
            );
            if (q.body.position < s.serving_position) {
              nextIntervalMs = policy.floorMs;
              return redeem().then(function () {
                // Still queued after all, or refused: keep polling. A success
                // navigates away and a permanent refusal has called stop(),
                // which schedule() honours.
                if (!admitting) {
                  nextIntervalMs = Math.min(
                    policy.ceilingMs,
                    policy.floorMs * Math.pow(2, Math.min(capacityMisses, 6))
                  );
                  schedule();
                }
              });
            }
            var ahead = aheadNow(s.serving_position);
            // Within one origin's worth of the front, the visitor is released
            // as soon as the next sessions end, which the wait estimate cannot
            // see coming: ask at the floor so the slot is not left waiting.
            nextIntervalMs =
              ahead === 0 || ahead <= s.origin_concurrency
                ? policy.floorMs
                : intervalForPosition(ahead, currentRate(expectedRate(s)));
            return schedule();
          });
      })
      .then(function () {
        backoffMs = 0;
      })
      .catch(function () {
        // Transient. Say nothing alarming and try again — a blip must not
        // look like a lost place. But an origin answering 5xx or 429 is not
        // helped by every waiter retrying at the pace it was polled at when
        // healthy (issue #251): each consecutive failure doubles the interval,
        // up to the ceiling a healthy deep-queue poll already uses, and the
        // first success above drops straight back. Kept apart from
        // nextIntervalMs because a chain that fails after /status succeeded
        // has already reset that to the floor.
        backoffMs = Math.min(policy.ceilingMs, (backoffMs || nextIntervalMs) * 2);
        nextIntervalMs = backoffMs;
        schedule();
      })
      .then(function () {
        inFlight = false;
      });
  }

  // A hidden tab is stopped outright (schedule() above) rather than polled
  // at a slower rate: browsers throttle it only after minutes and only some
  // of them, so stopping is the only way to actually save the requests.
  // Catching up the moment the visitor looks again is what makes that safe.
  // #97: a visitor hidden longer than the admission
  // grace loses their place — a pre-existing gap this sharpens but does not
  // create, tracked separately rather than in scope here.
  //
  // Registered here rather than earlier: the first tick() below always runs
  // regardless of visibility — a page loaded into a background tab still has
  // to join(), and claiming a place is not deferrable.
  document.addEventListener("visibilitychange", function () {
    if (stopped) {
      return;
    }
    if (document.hidden) {
      if (timer) {
        window.clearTimeout(timer);
        timer = null;
      }
      return;
    }
    if (timer) {
      window.clearTimeout(timer);
      timer = null;
    }
    // Poll now only if the floor has genuinely elapsed since the last one;
    // otherwise a visitor flapping between tabs (switching away and back
    // every couple of seconds) polls every couple of seconds, beating the
    // floor the whole design treats as a hard bound. Schedule the remainder
    // of the floor window instead.
    var sinceLastPoll = Date.now() - lastPollAt;
    if (sinceLastPoll >= policy.floorMs) {
      tick();
    } else {
      timer = window.setTimeout(tick, policy.floorMs - sinceLastPoll);
    }
  });

  el.note.textContent =
    "Closing this page keeps your place — reopening it picks the same place back up.";

  // Nothing may join before the identity it would join under is known.
  resolveIdentity();
  tick();
})();
