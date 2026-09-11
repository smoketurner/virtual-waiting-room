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
  var POLL_MS = 5000;
  // Spread reconnects so a whole cohort does not retry in lockstep after a blip.
  var JITTER_MS = 1500;

  var el = {
    headline: document.getElementById("headline"),
    subhead: document.getElementById("subhead"),
    stats: document.getElementById("stats"),
    position: document.getElementById("position"),
    serving: document.getElementById("serving"),
    ahead: document.getElementById("ahead"),
    eta: document.getElementById("eta"),
    bar: document.getElementById("bar"),
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

  // Admission is a navigation away from this page, so if the page loads again
  // straight afterwards the pass was not accepted — the cookies did not stick,
  // or the edge refused them. Without this the page redeems, navigates, is
  // refused, loads again, and redeems again as fast as the network allows.
  var REDEEM_KEY = "vwr_redeemed_at";
  var BOUNCE_KEY = "vwr_bounces";
  var BOUNCE_WINDOW_MS = 30000;
  // A refused pass is usually temporary — a newly rotated signing key takes a
  // few minutes to reach every edge, and an edge that has not learned it yet
  // refuses a cookie that is otherwise perfectly valid. So back off and try the
  // same pass again rather than tearing through the join-poll-redeem cycle at
  // network speed, and only give up after the delay has covered that window.
  var BOUNCE_RETRY_MS = 8000;
  var MAX_BOUNCE_RETRIES = 5;

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
    } catch (e) {
      /* storage unavailable; the loop guard degrades to off */
    }
  }

  function noteRedeem() {
    sessionSet(REDEEM_KEY, String(Date.now()));
  }

  /// How many times in a row this page has loaded straight after a redeem that
  /// should have navigated away. Zero means this is a normal arrival.
  function bounceCount() {
    var at = parseInt(sessionGet(REDEEM_KEY) || "0", 10);
    if (!at || Date.now() - at > BOUNCE_WINDOW_MS) {
      sessionSet(BOUNCE_KEY, "0");
      return 0;
    }
    var n = parseInt(sessionGet(BOUNCE_KEY) || "0", 10) + 1;
    sessionSet(BOUNCE_KEY, String(n));
    return n;
  }

  function jitter() {
    return POLL_MS + Math.floor(Math.random() * JITTER_MS);
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
    // Prefer what was observed; fall back to the operator's target so the first
    // poll says something, rather than leaving a dash for the half minute it
    // takes to watch the cursor move.
    var rate = measuredRate();
    if (rate === null && targetRate > 0) {
      rate = targetRate;
    }
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
      aheadAtStart = ahead;
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
    admitting = true;
    return postJSON("/v1/generate_token", {
      request_id: requestId,
      event_id: eventId,
    }).then(function (res) {
      if (res.status === 200 && res.body.admitted) {
        say("You're through", "Taking you to the site…");
        noteRedeem();
        // Replace so the waiting page does not sit in the back history.
        window.location.replace("/");
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
    timer = window.setTimeout(tick, jitter());
  }

  function tick() {
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
        eventId = s.event_id || eventId;
        if (!eventId) {
          // Nothing to join yet; the next poll tries again.
          say("Getting your place in line…", "Just a moment.");
          return schedule();
        }

        if (s.serving_state === "closed") {
          el.stats.hidden = true;
          el.bar.hidden = true;
          // A closed event has not dealt this visitor a number, and if one was
          // held from an earlier run of the same page it belongs to a cohort
          // that no longer exists — an operator who resets an event seals a new
          // one with a different permutation.
          forgetPosition();
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
              say("You're in line", "Finding your number…");
              return schedule();
            }
            if (q.status === 404) {
              misses += 1;
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
              return redeem().then(function () {
                // Still queued after all, or refused: keep polling. A success
                // navigates away and a permanent refusal has called stop(),
                // which schedule() honours.
                if (!admitting) {
                  schedule();
                }
              });
            }
            return schedule();
          });
      })
      .catch(function () {
        // Transient. Say nothing alarming and try again — a blip must not look
        // like a lost place.
        schedule();
      });
  }

  var bounces = bounceCount();
  if (bounces > 0) {
    // The pass exists and was refused. Hold it and try the same one again
    // shortly; re-running the queue would take a second place in line for a
    // visitor who already has one.
    if (bounces <= MAX_BOUNCE_RETRIES) {
      say("Almost through", "Getting you in — one moment.");
      noteRedeem();
      window.setTimeout(function () {
        window.location.replace("/");
      }, BOUNCE_RETRY_MS);
      return;
    }
    stop();
    say(
      "We couldn't get you through",
      "You were admitted, but the pass kept being refused on the way back."
    );
    el.note.textContent =
      "Reload to try again. If it keeps happening, check that cookies are enabled for this site.";
    el.note.className = "note error";
    return;
  }

  el.note.textContent =
    "Closing this page keeps your place — reopening it picks the same place back up.";
  tick();
})();
