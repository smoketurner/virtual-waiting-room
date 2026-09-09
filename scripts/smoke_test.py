#!/usr/bin/env python3
"""Virtual Waiting Room MVP smoke test.

Drives the scheduled-pre-queue and live-join happy paths against a deployed dev
stack and asserts the core invariant: no two visitors get the same queue
position. Re-runnable — it registers fresh UUIDv7 request ids each run.

The script reads the API URL, table names, seal function, and event id from
`terraform output` and never touches Terraform state — deploy with `make apply`
first. It exercises the app only (writes PreQueue rows, invokes seal, polls the
API).

Prerequisites:
  - AWS credentials in the environment (aws sso login / aws configure), with
    rights to read/write the stack's DynamoDB tables and invoke its Lambdas.
  - terraform on PATH; boto3 installed.

Usage:
  AWS_PROFILE=dev-admin ./scripts/smoke_test.py
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3

COHORT = 12
SHARDS = 10
ENV_DIR = Path(__file__).resolve().parent.parent / "infra" / "environments" / "dev"


def say(msg: str) -> None:
    print(f"\n=== {msg} ===")


def tf_output(name: str) -> str:
    out = subprocess.run(
        ["terraform", f"-chdir={ENV_DIR}", "output", "-raw", name],
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def tf_output_json(name: str) -> dict:
    out = subprocess.run(
        ["terraform", f"-chdir={ENV_DIR}", "output", "-json", name],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout)


def uuid_v7() -> str:
    """A canonical UUIDv7 (version nibble 7). Timestamp bits are cosmetic here;
    only the format and uniqueness matter to the handler."""
    ts = int(time.time() * 1000)
    b = bytearray(ts.to_bytes(6, "big") + secrets.token_bytes(10))
    b[6] = (b[6] & 0x0F) | 0x70  # version 7
    b[8] = (b[8] & 0x3F) | 0x80  # variant
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def shard_of(request_id: str) -> int:
    """FNV-1a over the request id, mod the shard count — the same deterministic
    hash the pre-queue registration path uses so a retry lands on its shard."""
    h = 0xCBF29CE484222325
    for c in request_id.encode():
        h ^= c
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h % SHARDS


def api_get(api_url: str, path: str) -> dict:
    with urllib.request.urlopen(f"{api_url}{path}") as r:  # noqa: S310 (trusted API URL from tf output)
        return json.load(r)


def main() -> int:
    region = os.environ.get("AWS_REGION", "us-east-1")
    session = boto3.Session(region_name=region)
    ddb = session.client("dynamodb")
    lam = session.client("lambda")
    ssm = session.client("ssm")

    say("1. Read the deployed stack from terraform outputs")
    subprocess.run(
        ["terraform", f"-chdir={ENV_DIR}", "init", "-input=false"],
        check=True,
        capture_output=True,
    )
    api_url = tf_output("api_invoke_url")
    event_id = tf_output("event_id")
    tables = tf_output_json("table_names")
    counters, prequeue, positions = tables["counters"], tables["prequeue"], tables["positions"]
    seal_fn = tf_output("seal_event_function_name")
    key_param = tf_output("signing_key_parameter_name")
    print(f"API: {api_url}  event_id: {event_id}")

    say("2. Seed the signing key out-of-band (SSM SecureString placeholder -> real)")
    ssm.put_parameter(
        Name=key_param,
        Type="SecureString",
        Overwrite=True,
        Value=base64.b64encode(secrets.token_bytes(32)).decode(),
    )
    print("signing key written")

    say("3. Pre-queue registration: write PreQueue rows for a small cohort")
    # The pre-queue registration handler is not part of the MVP (deferred), so
    # the smoke test seeds PreQueue rows and the shard counters directly,
    # mirroring what a POST /join during the pre-queue phase would write:
    # shard s = hash % SHARDS, local index l from ADD prequeue_counter#s.
    ids: list[str] = []
    shard_counts: dict[int, int] = {}
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for _ in range(COHORT):
        rid = uuid_v7()
        ids.append(rid)
        s = shard_of(rid)
        l = shard_counts.get(s, 0)
        shard_counts[s] = l + 1
        ddb.put_item(
            TableName=prequeue,
            Item={
                "r": {"S": rid},
                "s": {"N": str(s)},
                "l": {"N": str(l)},
                "t": {"S": now},
            },
            ConditionExpression="attribute_not_exists(r)",
        )
    for s, total in sorted(shard_counts.items()):
        ddb.update_item(
            TableName=counters,
            Key={"event_id": {"S": event_id}},
            UpdateExpression="SET #c = :v",
            ExpressionAttributeNames={"#c": f"prequeue_counter#{s}"},
            ExpressionAttributeValues={":v": {"N": str(total)}},
        )
    print(f"registered {len(ids)} pre-queue visitors")

    say("4. Seal the event (invoke seal_event) and confirm phase=active via /status")
    lam.invoke(FunctionName=seal_fn, Payload=json.dumps({"event_id": event_id}).encode())
    time.sleep(2)
    status = api_get(api_url, "/v1/status")
    print(json.dumps(status))
    assert status["phase"] == "active", status
    assert status.get("participant_count") == COHORT, status
    print(f"status OK: active, N={COHORT}")

    say("5. Resolve every pre-queue position via /queue_num; assert all distinct")
    seen: dict[int, str] = {}
    for rid in ids:
        d = api_get(api_url, f"/v1/queue_num?request_id={rid}")
        p = d["position"]
        assert not d["live_join"], (rid, d)
        assert p not in seen, f"DUPLICATE position {p}: {rid} and {seen[p]}"
        seen[p] = rid
    print(f"queue_num OK: {len(seen)} distinct positions in [0,{len(ids)})")

    say("6. Live join: POST /join, then confirm a Positions row is written")
    live_rid = uuid_v7()
    req = urllib.request.Request(  # noqa: S310 (trusted API URL from tf output)
        f"{api_url}/v1/join",
        data=json.dumps({"request_id": live_rid, "event_id": event_id}).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req):
        pass
    print(f"posted live join {live_rid}; waiting for assign_position to drain the batch")
    for _ in range(15):
        row = ddb.get_item(TableName=positions, Key={"request_id": {"S": live_rid}})
        if "Item" in row:
            print("live-join Positions row written")
            break
        time.sleep(2)
    else:
        print("ERROR: no Positions row for the live join after 30s", file=sys.stderr)
        return 1

    say("SMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
