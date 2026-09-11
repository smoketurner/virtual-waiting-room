// The waiting room client.
//
// Holds a place in line, shows where that place is, and redeems it the moment
// the queue reaches it. Everything it needs survives a reload, because the one
// thing a visitor must never lose is their position.
//
// The flow:
//   1. mint or recover a request id, persisted in localStorage
//   2. POST it to /v1/join once, so a position is claimed
//   3. poll /v1/status and /v1/queue_num
//   4. once the position is reached, POST /v1/generate_token to collect the
//      admission cookies, then reload onto the origin

(function () {
  "use strict";

  var STORAGE_KEY = "vwr_request_id";
  var JOINED_KEY = "vwr_joined";
  var AHEAD_AT_START_KEY = "vwr_ahead_at_start";

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

  // localStorage can throw (private mode, disabled storage). A visitor who
  // cannot persist still gets a place — they just lose it on reload, which is
  // better than being unable to queue at all.
  function readStored(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function writeStored(key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (e) {
      /* not fatal; see readStored */
    }
  }

  var requestId = readStored(STORAGE_KEY);
  if (!requestId) {
    requestId = uuidv7();
    writeStored(STORAGE_KEY, requestId);
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
    if (next.charAt(0) !== "/" || next.charAt(1) === "/") {
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

  function getJSON(url) {
    return fetch(url, {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    }).then(function (r) {
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
    }).then(function (r) {
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
  function join() {
    if (readStored(JOINED_KEY) === requestId) {
      return Promise.resolve();
    }
    return postJSON("/v1/join", {
      request_id: requestId,
      event_id: eventId,
    }).then(function (res) {
      // Only a request the ingest accepted claims a place. postJSON resolves
      // for any status, so recording unconditionally marks a rejected join as
      // done and leaves the visitor polling a position nothing will ever write.
      if (res.status >= 200 && res.status < 300) {
        writeStored(JOINED_KEY, requestId);
      }
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
  var misses = 0;

  // A place in line, once known, never changes: it is derived from a sealed
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
  var FIRST_ASK_MAX_SPREAD_MS = 60000;
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
  // what the origin was asked to absorb; the controller then corrects releases
  // against the share of admitted visitors who never arrive, so the cursor's
  // real speed is not the target. A visitor should be told what is happening,
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
  // disagree: the controller corrects releases against a no-show rate, so
  // the cursor's real speed differs from the operator's target, and the
  // interval should track what is actually happening rather than what was
  // only intended.
  function currentRate(targetRate) {
    var rate = measuredRate();
    return rate !== null ? rate : targetRate;
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
    try {
      window.localStorage.removeItem(JOINED_KEY);
    } catch (e) {
      /* storage unavailable; the in-memory retry below still applies */
    }
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

    // Stamped from the client's own clock on each successful poll. It answers
    // "is this page still live, or has it silently stopped updating" — the
    // question a visitor watching an unchanging number actually has.
    if (!el.bar.hidden) {
      el.updated.hidden = false;
      el.updatedAt.textContent = new Date().toLocaleTimeString();
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
    admitting = true;
    return postJSON("/v1/generate_token", {
      request_id: requestId,
      event_id: eventId,
    }).then(function (res) {
      if (res.status === 200 && res.body.admitted) {
        say("You're through", "Taking you to the site…");
        // Replace so the waiting page does not sit in the back history.
        window.location.replace(nextDestination());
        return;
      }
      // Not admitted after all: the cursor moved back, the position expired, or
      // the operator paused. Fall back to polling rather than hammering.
      admitting = false;
      if (res.status === 410) {
        say(
          "Your place expired",
          "You waited longer than the hold allows. Reload to take a new place in line."
        );
        stop();
      }
    });
  }

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
    getJSON("/v1/status")
      .then(function (res) {
        if (res.status !== 200) {
          throw new Error("status " + res.status);
        }
        var s = res.body;
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
          el.updated.hidden = true;
          // A closed event has not dealt this visitor a number, and if one was
          // held from an earlier run of the same page it belongs to a cohort
          // that no longer exists — an operator who resets an event seals a new
          // one with a different permutation.
          forgetPosition();
          nextIntervalMs = policy.ceilingMs;
          say(
            "The event isn't open yet",
            "This page updates on its own when it opens."
          );
          if (s.phase === "pre_queue") {
            // Registration during the countdown is a single direct write
            // (join() no-ops on reload via JOINED_KEY), not a poll: the
            // /queue_num call below stays skipped, since that endpoint
            // answers 409 until the seal and a poll against it would only
            // churn the catch handler. Must return the promise chain, or a
            // rejected join never reaches tick()'s catch and schedule()
            // never runs again.
            return join().then(schedule);
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
              : intervalForPosition(pausedAhead, currentRate(s.target_rate));
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
            if (Date.now() < firstAskAt) {
              return null;
            }
            return getJSON(
              "/v1/queue_num?request_id=" + encodeURIComponent(requestId)
            );
          })
          .then(function (q) {
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
              s.target_rate
            );
            if (q.body.position < s.serving_position) {
              nextIntervalMs = policy.floorMs;
              return redeem().then(function () {
                // Still queued after all, or refused: keep polling. A success
                // navigates away and a permanent refusal has called stop(),
                // which schedule() honours.
                if (!admitting) {
                  schedule();
                }
              });
            }
            var ahead = aheadNow(s.serving_position);
            nextIntervalMs =
              ahead === 0
                ? policy.floorMs
                : intervalForPosition(ahead, currentRate(s.target_rate));
            return schedule();
          });
      })
      .catch(function () {
        // Transient. Say nothing alarming and try again — a blip must not
        // look like a lost place. nextIntervalMs is left at its last value:
        // an unknown state deserves no faster and no slower a retry than the
        // visitor was already getting.
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
  tick();
})();
