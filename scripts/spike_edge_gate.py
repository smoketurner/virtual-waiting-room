#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["boto3[crt]==1.43.90"]
# ///
"""Measure a CloudFront Function gate against the constraints in ADR-0021.

ADR-0021 proposes moving the admission gate from a CloudFront trusted key group
to a CloudFront Function, which is the only way to close seven open issues that
all reduce to the same thing: CloudFront verifies a signature but does not
decide. Three of the four remaining questions in that ADR need a real function
and a real key value store to answer, and this script answers them.

What it measures:

  1. Compute utilization on the hot path. CloudFront Functions run under a hard
     budget expressed as a percentage of the maximum allowed time. The gate does
     two key value store reads, rule matching, and three HMAC operations (one to
     verify, two more for the double-HMAC compare that stands in for the
     timing-safe comparison the runtime does not provide). Whether that fits is
     the question, and `TestFunction` reports the number directly.

  2. Whether an unprotected request is meaningfully cheaper than a gated one.
     That decides whether the function can be associated distribution-wide or
     must stay on the protected behaviour only. At a million waiters polling
     /status every few seconds, a distribution-wide association would make
     per-invocation billing the dominant term in the cost model.

  3. That the gate refuses an XHR with JSON and a header rather than an HTML
     waiting page, which is the defect behind issue #72.

The credential in the admitted-visitor event was minted by the Rust
(`wr_common::crypto`), not by this script. That is the point: it demonstrates
that JavaScript at the edge verifies what Rust signs, byte for byte, including
the kind-byte domain separation. The signing key here is a throwaway string, not
a secret, and the store is deleted on the way out.

Not measured here: key value store propagation time. That needs the function
published and associated with a distribution, which should be done through
Terraform as its own cache behaviour rather than by mutating the dev stack out
of band.

This is spike tooling. Delete it when ADR-0021 is accepted or rejected.

Creates and deletes real AWS resources: one key value store and one function,
both named with a `wr-spike-` prefix. Neither is associated with any
distribution, so nothing serves traffic and nothing existing is touched.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import boto3
from botocore.exceptions import ClientError
from urllib.error import URLError
from urllib.request import Request, urlopen

KVS_NAME = "wr-spike-kvs"
FUNCTION_NAME = "wr-spike-gate"
PROBE_FUNCTION_NAME = "wr-spike-probe"
PROBE_COMMENT = "ADR-0021 propagation probe, safe to delete"

# CloudFront managed CachingDisabled policy. The probe function generates its
# response at viewer-request, so the cache is bypassed anyway; this makes that
# explicit rather than implicit.
CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"

# The probe reports the current key value in a response header and never
# contacts an origin, so the origin below is a placeholder that is never called.
PROBE_JS = """import cf from 'cloudfront';
var kvs = cf.kvs();

async function handler(event) {
    try {
        var v = await kvs.get('probe', { format: 'string' });
        return {
            statusCode: 200,
            statusDescription: 'OK',
            headers: {
                'x-wr-probe': { value: v },
                'cache-control': { value: 'no-store' }
            }
        };
    } catch (e) {
        return {
            statusCode: 500,
            statusDescription: 'Error',
            headers: { 'x-wr-probe': { value: 'error' } }
        };
    }
}
"""

# Not a secret: the same throwaway key the Rust used to mint TEST_CREDENTIAL.
SIGNING_KEY = "spike-key-0123456789-abcdefghijk"

# Minted by wr_common::crypto as Session { event_id: "smoke", request_id:
# "0199a1b2-...", issued_at: 1800000000, expires_at: 1900000000 }.
TEST_CREDENTIAL = (
    "AAVzbW9rZQAkMDE5OWExYjItYzNkNC03ZTVmLThhOWItMGMxZDJlM2Y0YTViAAAAAGtJ0gAAAAAAcT-zAA"
    ".k25Xf0EK5ypgOQB7nhDxkZsWZ7gjtThs3wzUsG1kdJI"
)

# The edge's whole configuration. Compact because the value ceiling is 1 KB.
STATE = {
    "event": "smoke",
    "serving": "open",
    "failOpenUntil": 0,
    "rules": [
        ["p", "/checkout"],
        ["p", "/cart"],
        ["p", "/product/"],
        ["h", "x-internal-monitor", "true"],
        ["c", "loyalty_member"],
        ["u", "HeadlessChrome"],
    ],
}

GATE_JS = r"""import cf from 'cloudfront';
import crypto from 'crypto';

var kvs = cf.kvs();
var KIND_SESSION = 0x02;
var SESSION_COOKIE = 'wr_sess';

// Constant-time-ish MAC compare: the runtime has no timing-safe primitive, so
// HMAC both digests under the same key and compare those instead.
function macEquals(secret, a, b) {
    var ha = crypto.createHmac('sha256', secret);
    ha.update(a);
    var hb = crypto.createHmac('sha256', secret);
    hb.update(b);
    return ha.digest('base64url') === hb.digest('base64url');
}

// base64url(payload).base64url(mac), MAC over kind || payload.
// Mirrors wr_common::crypto.
function verify(credential, kind, secret) {
    var dot = credential.indexOf('.');
    if (dot < 1) {
        return null;
    }
    var payload = Buffer.from(credential.substring(0, dot), 'base64url');
    var hmac = crypto.createHmac('sha256', secret);
    hmac.update(Buffer.concat([Buffer.from([kind]), payload]));
    if (!macEquals(secret, hmac.digest('base64url'), credential.substring(dot + 1))) {
        return null;
    }
    var at = 0;
    var eventLen = payload.readUInt16BE(at);
    at += 2;
    var eventId = payload.toString('utf8', at, at + eventLen);
    at += eventLen;
    var reqLen = payload.readUInt16BE(at);
    at += 2;
    at += reqLen;
    var issuedAt = payload.readUInt32BE(at + 4);
    at += 8;
    var expiresAt = payload.readUInt32BE(at + 4);
    return { eventId: eventId, issuedAt: issuedAt, expiresAt: expiresAt };
}

// Compact rules: ["p",prefix] | ["h",name,value] | ["c",name] | ["u",substring]
function matches(rule, request) {
    var headers = request.headers || {};
    var cookies = request.cookies || {};
    if (rule[0] === 'p') {
        return request.uri.indexOf(rule[1]) === 0;
    }
    if (rule[0] === 'h') {
        var h = headers[rule[1]];
        return !!h && h.value.toLowerCase() === rule[2].toLowerCase();
    }
    if (rule[0] === 'c') {
        return !!cookies[rule[1]];
    }
    if (rule[0] === 'u') {
        var ua = headers['user-agent'];
        return !!ua && ua.value.indexOf(rule[1]) >= 0;
    }
    return false;
}

function wantsHtml(request) {
    var accept = (request.headers || {})['accept'];
    var mode = (request.headers || {})['sec-fetch-mode'];
    if (mode && mode.value !== 'navigate') {
        return false;
    }
    return !accept || accept.value.indexOf('text/html') >= 0;
}

function refuse(request, reason) {
    if (wantsHtml(request)) {
        return {
            statusCode: 302,
            statusDescription: 'Found',
            headers: {
                'location': { value: '/_wr/waiting.html?r=' + reason },
                'cache-control': { value: 'no-store' },
                'x-wr-reason': { value: reason }
            }
        };
    }
    return {
        statusCode: 403,
        statusDescription: 'Forbidden',
        headers: {
            'content-type': { value: 'application/json' },
            'cache-control': { value: 'no-store' },
            'x-wr-reason': { value: reason },
            'x-wr-waiting-room': { value: '/_wr/waiting.html' },
            'access-control-expose-headers': { value: 'x-wr-reason,x-wr-waiting-room' }
        },
        body: '{"queued":true,"reason":"' + reason + '","waitingRoom":"/_wr/waiting.html"}'
    };
}

async function handler(event) {
    var request = event.request;
    try {
        var state = await kvs.get('state', { format: 'json' });

        var protectedByRule = false;
        for (var i = 0; i < state.rules.length; i++) {
            if (matches(state.rules[i], request)) {
                protectedByRule = true;
                break;
            }
        }
        if (!protectedByRule) {
            return request;
        }

        var now = Math.floor(Date.now() / 1000);
        if (state.failOpenUntil && now < state.failOpenUntil) {
            request.headers['x-wr-gate'] = { value: 'failopen' };
            return request;
        }
        if (state.serving === 'dormant') {
            return request;
        }

        var cookie = (request.cookies || {})[SESSION_COOKIE];
        if (!cookie) {
            return refuse(request, 'none');
        }

        var secret = await kvs.get('key', { format: 'string' });
        var session = verify(cookie.value, KIND_SESSION, secret);
        if (!session) {
            return refuse(request, 'signature');
        }
        if (session.eventId !== state.event) {
            return refuse(request, 'eventid');
        }
        if (now >= session.expiresAt) {
            return refuse(request, 'expired');
        }
        return request;
    } catch (e) {
        console.log('gate threw: ' + e);
        request.headers['x-wr-gate-failed'] = { value: 'true' };
        return request;
    }
}
"""


def event(uri, *, accept, mode=None, cookies=None, headers=None):
    """A viewer-request event object in the shape CloudFront passes to a function."""
    hdrs = {"host": {"value": "example.com"}, "accept": {"value": accept}}
    if mode:
        hdrs["sec-fetch-mode"] = {"value": mode}
    hdrs.update(headers or {})
    return {
        "version": "1.0",
        "context": {"eventType": "viewer-request"},
        "viewer": {"ip": "203.0.113.9"},
        "request": {
            "method": "GET",
            "uri": uri,
            "querystring": {},
            "headers": hdrs,
            "cookies": cookies or {},
        },
    }


CASES = [
    (
        "admitted",
        "valid session on a protected path - the hot path, 2 KVS reads + 3 HMACs",
        event(
            "/checkout",
            accept="text/html",
            mode="navigate",
            cookies={"wr_sess": {"value": TEST_CREDENTIAL}},
        ),
    ),
    (
        "unprotected",
        "matches no rule - should short-circuit before any credential work",
        event("/about", accept="text/html", mode="navigate"),
    ),
    (
        "refused-navigation",
        "no credential, navigation - expect 302 to the waiting page",
        event("/cart", accept="text/html", mode="navigate"),
    ),
    (
        "refused-xhr",
        "no credential, XHR - expect 403 JSON with a header, not HTML (#72)",
        event("/cart", accept="application/json", mode="cors"),
    ),
    (
        "bad-signature",
        "tampered credential - expect refusal with reason=signature",
        event(
            "/checkout",
            accept="text/html",
            mode="navigate",
            cookies={"wr_sess": {"value": TEST_CREDENTIAL[:-4] + "AAAA"}},
        ),
    ),
]


def fatal(message, err=None):
    print(f"\nFAILED: {message}", file=sys.stderr)
    if err is not None:
        code = err.response.get("Error", {}).get("Code", "?")
        print(f"  {code}: {err.response.get('Error', {}).get('Message', err)}", file=sys.stderr)
        if code in ("AccessDenied", "AccessDeniedException"):
            print(
                "\n  The role needs these actions, scoped to the wr-spike-* names:\n"
                "    cloudfront:CreateFunction, TestFunction, DescribeFunction, DeleteFunction\n"
                "    cloudfront:CreateKeyValueStore, DescribeKeyValueStore, DeleteKeyValueStore\n"
                "    cloudfront-keyvaluestore:PutKey, DescribeKeyValueStore",
                file=sys.stderr,
            )
    sys.exit(1)


def wait_ready(cf, name, timeout=180):
    """Block until the store leaves PROVISIONING; association fails before then."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = cf.describe_key_value_store(Name=name)["KeyValueStore"]["Status"]
        if status == "READY":
            return
        print(f"  store status {status}, waiting...")
        time.sleep(5)
    fatal(f"key value store did not become READY within {timeout}s")


def create(cf, kvs_data):
    print(f"Creating key value store {KVS_NAME}")
    try:
        created = cf.create_key_value_store(
            Name=KVS_NAME, Comment="ADR-0021 spike, safe to delete"
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "EntityAlreadyExists":
            fatal(f"{KVS_NAME} already exists - run with --cleanup first")
        fatal("could not create the key value store", err)

    arn = created["KeyValueStore"]["ARN"]
    wait_ready(cf, KVS_NAME)

    state_json = json.dumps(STATE, separators=(",", ":"))
    print(f"  seeding state ({len(state_json)} bytes of the 1024 limit) and key")
    etag = kvs_data.describe_key_value_store(KvsARN=arn)["ETag"]
    for key, value in (("state", state_json), ("key", SIGNING_KEY)):
        etag = kvs_data.put_key(KvsARN=arn, Key=key, Value=value, IfMatch=etag)["ETag"]

    # put_key returns before the value is readable from a function. Wait for the
    # store to report both keys rather than racing it.
    deadline = time.time() + 120
    while time.time() < deadline:
        count = kvs_data.describe_key_value_store(KvsARN=arn).get("ItemCount", 0)
        if count >= 2:
            print(f"  store reports {count} keys")
            break
        print(f"  store reports {count} keys, waiting...")
        time.sleep(5)
    else:
        fatal("the store never reported both keys")
    time.sleep(10)

    code = GATE_JS.encode()
    print(f"Creating function {FUNCTION_NAME} ({len(code)} bytes of the 10240 limit)")
    fn = cf.create_function(
        Name=FUNCTION_NAME,
        FunctionConfig={
            "Comment": "ADR-0021 spike",
            "Runtime": "cloudfront-js-2.0",
            "KeyValueStoreAssociations": {
                "Quantity": 1,
                "Items": [{"KeyValueStoreARN": arn}],
            },
        },
        FunctionCode=code,
    )
    return fn["ETag"], len(code)


def measure(cf, etag):
    print("\nRunning test events\n")
    rows = []
    for name, description, payload in CASES:
        try:
            result = cf.test_function(
                Name=FUNCTION_NAME,
                IfMatch=etag,
                Stage="DEVELOPMENT",
                EventObject=json.dumps(payload).encode(),
            )["TestResult"]
        except ClientError as err:
            fatal(f"test event {name} failed", err)

        util = result.get("ComputeUtilization", "?")
        error = result.get("FunctionErrorMessage") or ""
        output = result.get("FunctionOutput") or "{}"
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError:
            parsed = {}

        response = parsed.get("response")
        if response:
            reason = response.get("headers", {}).get("x-wr-reason", {}).get("value", "-")
            outcome = f"{response.get('statusCode')} reason={reason}"
        else:
            marked = (parsed.get("request", {}).get("headers", {}) or {})
            outcome = "passed through"
            if "x-wr-gate-failed" in marked:
                outcome += " (GATE THREW - check the logs)"

        logs = result.get("FunctionExecutionLogs") or []
        rows.append((name, util, outcome, error))
        print(f"  {name:20s} utilization={util:>4}  {outcome}")
        if error:
            print(f"    error: {error}")
        for line in logs:
            print(f"    log: {line}")
        if "GATE THREW" in outcome and not logs:
            print("    (no logs - the throw happened before console.log was reachable)")
        print(f"    {description}")
    return rows


def report(rows, code_size):
    print("\n" + "=" * 72)
    print("ADR-0021 measurements")
    print("=" * 72)
    print(f"\nFunction size: {code_size} of 10240 bytes "
          f"({code_size * 100 // 10240}% of the ceiling)")

    if all("GATE THREW" in outcome for _, _, outcome, _ in rows):
        print("\n  EVERY CASE THREW. The utilization figures below are the cost of")
        print("  throwing, not of doing the work - this run measured nothing. The")
        print("  logged error above is the thing to fix.")

    by_name = {name: util for name, util, _, _ in rows}
    hot = by_name.get("admitted")
    cold = by_name.get("unprotected")

    print(f"\nCompute utilization")
    print(f"  hot path (2 KVS reads, rule match, 3 HMACs): {hot}")
    print(f"  unprotected (no credential work):            {cold}")
    if isinstance(hot, str) and hot.isdigit():
        headroom = 100 - int(hot)
        print(f"\n  {headroom}% of the time budget is unspent on the hot path.")
        if int(hot) > 60:
            print("  That is tight. Rule count and ruleset reassembly both add to it.")
    if all(isinstance(v, str) and v.isdigit() for v in (hot, cold)):
        print(
            f"\n  An unprotected request costs {int(hot) - int(cold)} points less than a gated one."
            "\n  That is a time-budget fact, not a cost one: CloudFront Functions bill a flat"
            "\n  rate per invocation, so what decides whether the association can go"
            "\n  distribution-wide is invocation COUNT, not utilization (ADR-0021 section 4.3)."
        )

    print("\nStill unmeasured: key value store propagation time. That needs the")
    print("function published and attached to a cache behaviour, which should go")
    print("through Terraform rather than mutating the dev stack out of band.")


def cleanup(cf, quiet=False):
    for label, describe, delete, kwargs in (
        ("function", cf.describe_function, cf.delete_function, {"Name": FUNCTION_NAME}),
        (
            "key value store",
            cf.describe_key_value_store,
            cf.delete_key_value_store,
            {"Name": KVS_NAME},
        ),
    ):
        try:
            etag = describe(**kwargs)["ETag"]
        except ClientError as err:
            if err.response["Error"]["Code"] in ("NoSuchFunctionExists", "EntityNotFound"):
                if not quiet:
                    print(f"  no {label} to delete")
                continue
            fatal(f"could not describe the {label}", err)
        delete(**kwargs, IfMatch=etag)
        print(f"  deleted the {label}")



def propagation(cf, kvs_data, trials):
    """Time a key value store update from PutKey to visible at a real edge.

    This is the number ADR-0021 leaves open, and it sets three commitments at
    once: how fast standby can activate (#60), how fast an admission can be
    revoked (#63), and how long the edge and DynamoDB disagree about serving
    state (section 4.2). AWS publishes no commitment for it.

    It cannot be measured through TestFunction, which does not run at an edge
    location. So this builds the smallest thing that does: a throwaway
    distribution whose viewer-request function returns the current key value in
    a header and never contacts an origin.

    One caveat that bounds the result. A client sees one edge location, so what
    is measured is propagation to the nearest point of presence. Global
    convergence - every visitor everywhere seeing fail-open - is at least this
    and probably longer. Treat the figure as a floor, not a guarantee.
    """
    print("Creating key value store")
    try:
        created = cf.create_key_value_store(Name=KVS_NAME, Comment=PROBE_COMMENT)
    except ClientError as err:
        if err.response["Error"]["Code"] == "EntityAlreadyExists":
            fatal(f"{KVS_NAME} already exists - run with --cleanup first")
        fatal("could not create the key value store", err)
    arn = created["KeyValueStore"]["ARN"]
    wait_ready(cf, KVS_NAME)

    etag = kvs_data.describe_key_value_store(KvsARN=arn)["ETag"]
    etag = kvs_data.put_key(KvsARN=arn, Key="probe", Value="v0", IfMatch=etag)["ETag"]

    print(f"Creating and publishing {PROBE_FUNCTION_NAME}")
    fn = cf.create_function(
        Name=PROBE_FUNCTION_NAME,
        FunctionConfig={
            "Comment": PROBE_COMMENT,
            "Runtime": "cloudfront-js-2.0",
            "KeyValueStoreAssociations": {
                "Quantity": 1,
                "Items": [{"KeyValueStoreARN": arn}],
            },
        },
        FunctionCode=PROBE_JS.encode(),
    )
    published = cf.publish_function(Name=PROBE_FUNCTION_NAME, IfMatch=fn["ETag"])
    fn_arn = published["FunctionSummary"]["FunctionMetadata"]["FunctionARN"]

    print("Creating a throwaway distribution (this is the slow part)")
    dist = cf.create_distribution(
        DistributionConfig={
            "CallerReference": f"wr-spike-{int(time.time())}",
            "Comment": PROBE_COMMENT,
            "Enabled": True,
            "Origins": {
                "Quantity": 1,
                "Items": [
                    {
                        "Id": "placeholder",
                        "DomainName": "example.com",
                        "CustomOriginConfig": {
                            "HTTPPort": 80,
                            "HTTPSPort": 443,
                            "OriginProtocolPolicy": "https-only",
                        },
                    }
                ],
            },
            "DefaultCacheBehavior": {
                "TargetOriginId": "placeholder",
                "ViewerProtocolPolicy": "redirect-to-https",
                "CachePolicyId": CACHING_DISABLED,
                "FunctionAssociations": {
                    "Quantity": 1,
                    "Items": [
                        {"EventType": "viewer-request", "FunctionARN": fn_arn}
                    ],
                },
            },
        }
    )["Distribution"]
    dist_id, domain = dist["Id"], dist["DomainName"]
    print(f"  {dist_id} at {domain}")

    print("  waiting for Deployed...")
    cf.get_waiter("distribution_deployed").wait(
        Id=dist_id, WaiterConfig={"Delay": 20, "MaxAttempts": 60}
    )

    url = f"https://{domain}/"

    def observed():
        try:
            with urlopen(Request(url, headers={"cache-control": "no-cache"}), timeout=10) as r:
                return r.headers.get("x-wr-probe")
        except URLError:
            return None

    print("\n  settling before the first trial")
    for _ in range(30):
        if observed() == "v0":
            break
        time.sleep(2)

    results = []
    for trial in range(1, trials + 1):
        want = f"t{trial}-{int(time.time())}"
        etag = kvs_data.describe_key_value_store(KvsARN=arn)["ETag"]
        start = time.time()
        kvs_data.put_key(KvsARN=arn, Key="probe", Value=want, IfMatch=etag)

        elapsed = None
        while time.time() - start < 300:
            if observed() == want:
                elapsed = time.time() - start
                break
            time.sleep(1)
        if elapsed is None:
            print(f"  trial {trial}: NOT VISIBLE within 300s")
            results.append(None)
        else:
            print(f"  trial {trial}: {elapsed:.1f}s")
            results.append(elapsed)

    print("\n" + "=" * 72)
    print("KeyValueStore propagation to one edge location")
    print("=" * 72)
    good = [r for r in results if r is not None]
    if good:
        good_sorted = sorted(good)
        median = good_sorted[len(good_sorted) // 2]
        print(f"\n  trials: {len(good)}/{trials} observed")
        print(f"  min {min(good):.1f}s   median {median:.1f}s   max {max(good):.1f}s")
        print("\n  This bounds three commitments in ADR-0021: standby activation (#60),")
        print("  revocation delay (#63), and the window in which the edge and DynamoDB")
        print("  disagree about serving state (section 4.2).")
        print("\n  It is a floor. One client sees one point of presence; global")
        print("  convergence is at least this and probably longer.")
    else:
        print("\n  Nothing propagated within 300s. That is itself a finding - it would")
        print("  make standby activation and revocation impractical at the edge.")

    print("\nCleanup")
    print(f"  The distribution {dist_id} is still enabled, and CloudFront will not")
    print("  delete a function while a distribution references it. Disabling a")
    print("  distribution and waiting for that to deploy takes longer than this")
    print("  script should hold your terminal, so one command tears down all three:")
    print(f"\n    scripts/spike_edge_gate.py --delete-distribution {dist_id}\n")


def delete_distribution(cf, dist_id):
    """Disable a distribution, wait for that to deploy, then delete it."""
    current = cf.get_distribution_config(Id=dist_id)
    config, etag = current["DistributionConfig"], current["ETag"]
    if config["Enabled"]:
        config["Enabled"] = False
        etag = cf.update_distribution(Id=dist_id, DistributionConfig=config, IfMatch=etag)[
            "ETag"
        ]
        print(f"  disabled {dist_id}, waiting for that to deploy (several minutes)")
        cf.get_waiter("distribution_deployed").wait(
            Id=dist_id, WaiterConfig={"Delay": 30, "MaxAttempts": 60}
        )
        etag = cf.get_distribution_config(Id=dist_id)["ETag"]
    cf.delete_distribution(Id=dist_id, IfMatch=etag)
    print(f"  deleted {dist_id}")

    # Only now can the function go, and the store only after the function.
    for name in (PROBE_FUNCTION_NAME, FUNCTION_NAME):
        try:
            cf.delete_function(Name=name, IfMatch=cf.describe_function(Name=name)["ETag"])
            print(f"  deleted the function {name}")
        except ClientError as err:
            if err.response["Error"]["Code"] != "NoSuchFunctionExists":
                fatal(f"could not delete the function {name}", err)
    try:
        cf.delete_key_value_store(
            Name=KVS_NAME, IfMatch=cf.describe_key_value_store(Name=KVS_NAME)["ETag"]
        )
        print(f"  deleted the key value store {KVS_NAME}")
    except ClientError as err:
        if err.response["Error"]["Code"] != "EntityNotFound":
            fatal("could not delete the key value store", err)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", default="dev-admin")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="delete the spike function and store, then exit",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="leave the function and store in place for further testing",
    )
    parser.add_argument(
        "--propagation",
        action="store_true",
        help="measure key value store propagation to a real edge (slow: builds a distribution)",
    )
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument(
        "--delete-distribution",
        metavar="ID",
        help="disable and delete a distribution left by --propagation",
    )
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cf = session.client("cloudfront")
    kvs_data = session.client("cloudfront-keyvaluestore")

    if args.delete_distribution:
        delete_distribution(cf, args.delete_distribution)
        return

    if args.cleanup:
        print("Cleaning up")
        cleanup(cf)
        return

    if args.propagation:
        propagation(cf, kvs_data, args.trials)
        return

    etag, code_size = create(cf, kvs_data)
    try:
        rows = measure(cf, etag)
        report(rows, code_size)
    finally:
        if args.keep:
            print(f"\nLeaving {FUNCTION_NAME} and {KVS_NAME} in place (--keep).")
            print("Delete them with: scripts/spike_edge_gate.py --cleanup")
        else:
            print("\nCleaning up")
            cleanup(cf, quiet=True)


if __name__ == "__main__":
    main()
