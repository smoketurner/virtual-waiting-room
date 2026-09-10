#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["boto3[crt]==1.43.90"]
# ///
"""Reset a deployed Virtual Waiting Room environment back to a testable state.

Empties the per-visitor tables and rewrites the event's `Counters` item from
scratch, so the next test run starts from a known state instead of inheriting
positions, seal outputs, and counters from the last one.

The `Counters` item is deleted and rewritten rather than patched. Stale
attributes are the whole problem this script exists to solve: a leftover
`shuffle_seed` and `participant_count` describe a cohort whose `PreQueue` rows
have just been deleted, so positions resolve against a cohort that no longer
exists, and attributes from superseded schema versions linger indefinitely
because nothing writes them any more and nothing removes them either.

Reads table names and the event id from `terraform output`, so it always acts on
the stack that is actually deployed. It never touches Terraform state.

DESTRUCTIVE: every row in PreQueue, Positions, and Tokens is deleted. Prompts
before doing it unless --yes is passed.

Prerequisites:
  - AWS credentials with read/write on the stack's DynamoDB tables.
  - terraform and uv on PATH, and a deployed stack (`make apply`).

Usage:
  AWS_PROFILE=dev-admin ./scripts/reset-env.py
  AWS_PROFILE=dev-admin ./scripts/reset-env.py --target-rate 20
  AWS_PROFILE=dev-admin ./scripts/reset-env.py --phase idle --yes
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3

ENV_DIR = Path(__file__).resolve().parent.parent / "infra" / "environments" / "dev"

# Partition key per table. Every table is keyed by a single string attribute.
VISITOR_TABLES = {
    "prequeue": "r",
    "positions": "request_id",
    "tokens": "request_id",
}

PHASES = ("idle", "pre_queue", "active", "post_event", "maintenance")

# DynamoDB caps a BatchWriteItem at 25 requests.
BATCH_LIMIT = 25


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


def empty_table(ddb, table: str, key_attr: str) -> int:
    """Deletes every item, returning the count removed.

    Scans for keys only: the tables can hold a row per visitor, so pulling whole
    items back to throw them away would be wasteful at any real event size.
    """
    deleted = 0
    paginator = ddb.get_paginator("scan")
    pages = paginator.paginate(
        TableName=table,
        ProjectionExpression="#k",
        ExpressionAttributeNames={"#k": key_attr},
    )

    batch: list[dict] = []
    for page in pages:
        for item in page.get("Items", []):
            value = item.get(key_attr, {}).get("S")
            if value is None:
                continue
            batch.append({"DeleteRequest": {"Key": {key_attr: {"S": value}}}})
            if len(batch) == BATCH_LIMIT:
                deleted += flush(ddb, table, batch)
                batch = []
    if batch:
        deleted += flush(ddb, table, batch)
    return deleted


def flush(ddb, table: str, batch: list[dict]) -> int:
    """Writes one batch, retrying whatever DynamoDB declines to process.

    A throttled BatchWriteItem returns 200 with the rejected requests in
    UnprocessedItems rather than raising, so dropping them here would leave rows
    behind and report a reset that did not happen.
    """
    count = len(batch)
    pending = {table: batch}
    for _ in range(10):
        response = ddb.batch_write_item(RequestItems=pending)
        pending = {t: reqs for t, reqs in response.get("UnprocessedItems", {}).items() if reqs}
        if not pending:
            return count
    remaining = sum(len(reqs) for reqs in pending.values())
    raise RuntimeError(f"{table}: {remaining} deletes still unprocessed after 10 attempts")


def fresh_counters(event_id: str, phase: str, target_rate: int) -> dict:
    """The `Counters` item for an event that has never run.

    Only the attributes a fresh event genuinely has. The sequences are written
    explicitly at zero rather than left absent — readers default a missing
    counter to zero, so the two are equivalent to the code, but an explicit zero
    is the difference between "this event has not started" and "somebody deleted
    an attribute".

    Deliberately absent: shuffle_seed, participant_count, and prequeue_offsets
    (written only by the seal), the prequeue_counter and arrivals shards, and
    the controller's carried state (last_arrivals_total, last_serving_counter,
    no_show_rate).
    """
    return {
        "event_id": {"S": event_id},
        "phase": {"S": phase},
        "admission_control": {"S": "open"},
        "queue_counter": {"N": "0"},
        "serving_counter": {"N": "0"},
        "target_rate": {"N": str(target_rate)},
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Reset a deployed Virtual Waiting Room environment for testing.",
    )
    p.add_argument(
        "--phase",
        choices=PHASES,
        default="active",
        help="Phase to leave the event in. Default active, which is the only one that admits.",
    )
    p.add_argument(
        "--target-rate",
        type=int,
        default=5,
        help="Visitors per second the controller meters at. Default 5, so a pass releases 50.",
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.target_rate < 1:
        print("--target-rate must be at least 1; use --phase to stop admission", file=sys.stderr)
        return 2

    region = os.environ.get("AWS_REGION", "us-east-1")
    ddb = boto3.Session(region_name=region).client("dynamodb")

    say("Reading the deployed stack from terraform outputs")
    subprocess.run(
        ["terraform", f"-chdir={ENV_DIR}", "init", "-input=false"],
        check=True,
        capture_output=True,
    )
    event_id = tf_output("event_id")
    tables = tf_output_json("table_names")
    print(f"region:   {region}")
    print(f"event_id: {event_id}")
    for name in ("counters", *VISITOR_TABLES):
        print(f"  {name:9} {tables[name]}")

    if not args.yes:
        print(
            f"\nThis deletes every row in {', '.join(tables[t] for t in VISITOR_TABLES)}"
            f"\nand rewrites the '{event_id}' item in {tables['counters']}."
        )
        if input("Type 'reset' to continue: ").strip() != "reset":
            print("aborted")
            return 1

    say("Emptying the per-visitor tables")
    for name, key_attr in VISITOR_TABLES.items():
        count = empty_table(ddb, tables[name], key_attr)
        print(f"{tables[name]}: deleted {count}")

    say("Rewriting the Counters item")
    # Deleted first so no attribute from the previous run can survive: PutItem
    # replaces the item, but only for the attributes it names.
    ddb.delete_item(TableName=tables["counters"], Key={"event_id": {"S": event_id}})
    item = fresh_counters(event_id, args.phase, args.target_rate)
    ddb.put_item(TableName=tables["counters"], Item=item)
    for key in sorted(item):
        print(f"  {key:18} {next(iter(item[key].values()))}")

    say("Reset complete")
    if args.phase != "active":
        print(f"Phase is {args.phase}: nobody will be admitted until it is active.")
    else:
        print("The event is live. A visitor arriving now queues and is admitted within a minute.")
    print(
        "\nAdmission cookies already in a browser survive this reset — CloudFront verifies"
        "\nthem against the signing key, not against anything just deleted. Clear cookies for"
        "\nthe distribution to be sent back to the queue."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
