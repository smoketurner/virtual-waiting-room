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

  function forgetJoin() {
    misses = 0;
    try {
      window.localStorage.removeItem(JOINED_KEY);
    } catch (e) {
      /* storage unavailable; the in-memory retry below still applies */
    }
  }

  function renderQueue(position, serving, participants) {
    el.stats.hidden = false;
    el.position.textContent = position.toLocaleString();
    el.serving.textContent = serving.toLocaleString();
    var ahead = Math.max(0, position - serving);
    el.ahead.textContent = ahead.toLocaleString();

    // Progress is only meaningful once the cohort size is known.
    if (participants && participants > 0) {
      el.bar.hidden = false;
      var done = Math.min(100, Math.max(0, (serving / participants) * 100));
      el.fill.style.width = done.toFixed(1) + "%";
    }

    if (ahead === 0) {
      say("You're next", "Letting you through…");
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
        eventId = s.event_id || eventId;
        if (!eventId) {
          // Nothing to join yet; the next poll tries again.
          say("Getting your place in line…", "Just a moment.");
          return schedule();
        }

        if (s.serving_state === "closed") {
          el.stats.hidden = true;
          el.bar.hidden = true;
          say(
            "The event isn't open yet",
            "This page updates on its own when it opens."
          );
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

        return join()
          .then(function () {
            return getJSON(
              "/v1/queue_num?request_id=" + encodeURIComponent(requestId)
            );
          })
          .then(function (q) {
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
            renderQueue(
              q.body.position,
              s.serving_position,
              s.participant_count
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
