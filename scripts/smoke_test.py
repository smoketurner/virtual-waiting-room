#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["boto3[crt]==1.43.90"]
# ///
"""Virtual Waiting Room MVP smoke test.

Drives the scheduled-pre-queue and live-join happy paths against a deployed dev
stack and asserts the core invariant: no two visitors get the same queue
position. Registers fresh UUIDv7 request ids each run.

Re-runnable: it resets the environment first, via `scripts/reset-env.py`, which
clears the seal outputs from any previous run. Without that a second run finds
`shuffle_seed` already set, so the joins take the live path, no PreQueue rows
appear, and step 2 fails after 90s with nothing to say why. Pass `--no-reset`
if you have just reset by hand.

The script reads the API URL, table names, seal function, and event id from
`terraform output` and never touches Terraform state — deploy with `make apply`
first. It exercises the app only (writes PreQueue rows, invokes seal, polls the
API).

It does NOT touch the signing key. Terraform generates that during apply and
writes the same value to both readers — the SSM SecureString the Lambdas read
and the edge gate's KeyValueStore — so overwriting one desynchronizes them and
every cookie minted afterwards is refused at the gate until the next apply.

Self-contained via uv: the PEP 723 block above declares boto3, so `uv run`
creates an ephemeral virtualenv and installs it — no manual venv or pip step.

Prerequisites:
  - AWS credentials in the environment (aws sso login / aws configure), with
    rights to read/write the stack's DynamoDB tables and invoke its Lambdas.
  - terraform and uv on PATH.

Usage:
  AWS_PROFILE=dev-admin uv run scripts/smoke_test.py
  # or, since the shebang runs uv, just: AWS_PROFILE=dev-admin ./scripts/smoke_test.py
  AWS_PROFILE=dev-admin ./scripts/smoke_test.py --no-reset
"""

from __future__ import annotations

import argparse
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
    with urllib.request.urlopen(f"{api_url}{path}") as r:
        return json.load(r)


def api_post(api_url: str, path: str, body: dict) -> None:
    req = urllib.request.Request(
        f"{api_url}{path}",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req):
        pass


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--no-reset",
        action="store_true",
        help="Skip the reset-env.py call. Only correct if you just reset by hand: "
        "a stack whose event is already sealed will hang in step 2.",
    )
    args = p.parse_args()

    region = os.environ.get("AWS_REGION", "us-east-1")
    session = boto3.Session(region_name=region)
    ddb = session.client("dynamodb")
    lam = session.client("lambda")

    say("1. Read the deployed stack from terraform outputs")
    subprocess.run(
        ["terraform", f"-chdir={ENV_DIR}", "init", "-input=false"],
        check=True,
        capture_output=True,
    )
    api_url = tf_output("api_invoke_url")
    event_id = tf_output("event_id")
    tables = tf_output_json("table_names")
    counters, prequeue, positions = (
        tables["counters"],
        tables["prequeue"],
        tables["positions"],
    )
    seal_fn = tf_output("seal_event_function_name")
    print(f"API: {api_url}  event_id: {event_id}")

    if args.no_reset:
        say("2. Skipping the reset (--no-reset)")
    else:
        # reset-env.py owns the Counters item's shape: it deletes and rewrites
        # the whole item, which is what clears a previous run's shuffle_seed.
        # Patching phase here instead would leave that seed in place and send
        # the joins below down the live path.
        say("2. Reset the environment to a fresh pre_queue event")
        subprocess.run(
            [
                str(Path(__file__).resolve().parent / "reset-env.py"),
                "--phase",
                "pre_queue",
                "--yes",
            ],
            check=True,
        )

    say("3. Pre-queue registration: POST /v1/join for a small cohort during pre_queue")
    ids: list[str] = [uuid_v7() for _ in range(COHORT)]
    for rid in ids:
        api_post(api_url, "/v1/join", {"request_id": rid, "event_id": event_id})
    print(
        f"posted {len(ids)} pre-queue joins; waiting for assign_position to drain the batch"
    )

    # The event-source mapping batches with up to a 1s window but AWS
    # documents up to 20s of Lambda-side delay on a quiet queue, so poll
    # rather than assume a fixed delay.
    pending = set(ids)
    deadline = time.monotonic() + 90
    rows: dict[str, dict] = {}
    while pending and time.monotonic() < deadline:
        for rid in list(pending):
            row = ddb.get_item(TableName=prequeue, Key={"r": {"S": rid}})
            if "Item" in row:
                rows[rid] = row["Item"]
                pending.discard(rid)
        if pending:
            time.sleep(2)
    if pending:
        print(
            f"ERROR: {len(pending)} PreQueue rows never appeared after 90s",
            file=sys.stderr,
        )
        return 1

    for rid, item in rows.items():
        assert int(item["s"]["N"]) == shard_of(rid), (rid, item)
    print(
        f"registered {len(ids)} pre-queue visitors; all rows landed on their expected shard"
    )

    shard_total = 0
    for s in range(SHARDS):
        shard_item = ddb.get_item(
            TableName=counters, Key={"event_id": {"S": f"EVT#{event_id}#PQ#{s}"}}
        )
        shard_total += int(shard_item.get("Item", {}).get("n", {}).get("N", "0"))
    assert shard_total == COHORT, (shard_total, COHORT)
    print(f"shard counts sum to {shard_total} == COHORT")

    say("4. Seal the event (invoke seal_event) and confirm phase=active via /status")
    lam.invoke(
        FunctionName=seal_fn, Payload=json.dumps({"event_id": event_id}).encode()
    )
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
    api_post(api_url, "/v1/join", {"request_id": live_rid, "event_id": event_id})
    print(
        f"posted live join {live_rid}; waiting for assign_position to drain the batch"
    )
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
