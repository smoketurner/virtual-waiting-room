#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = ["boto3==1.40.71"]
# ///
"""Writes the per-deployment signing key to both places the edge gate reads it
from (issue #71).

Terraform creates two placeholders and then ignores their value
(`lifecycle { ignore_changes = [value] }`): the SSM SecureString
`/<name_prefix>/signing-key`, and the CloudFront KeyValueStore key `k`. This
script is the one thing that writes the real secret to both, in one run:

  1. If SSM still holds the literal `PLACEHOLDER-overwrite-out-of-band`
     (or is unreadable), a fresh 32-byte secret is generated here and written
     to SSM.
  2. The same secret (freshly generated, or already in SSM) is written to the
     KeyValueStore's `k` key.

One origin for the secret means SSM and the KeyValueStore cannot disagree as
a result of a *successful* run of this script — a failure between the two
writes is still possible and is not silently retried; re-run the script,
which is safe because step 2 always mirrors whatever SSM currently holds.

`generate_token` and `authorizer` both refuse to start if the parameter they
read is still the placeholder literal (see `PLACEHOLDER_SIGNING_KEY` in
`crates/wr-common/src/crypto.rs`). That is the only thing that catches this
script never having been run at all — by construction this script cannot
detect its own absence — so run it before the first `POST /v1/generate_token`
of every deployment, not only the first one ever.

Refuses to overwrite a non-placeholder SSM value unless `--force` is given:
regenerating the secret invalidates every live session immediately, and
re-running this script is normally accidental once a deployment is live.

Usage:
  AWS_PROFILE=dev-admin uv run scripts/bootstrap_edge_gate.py
  AWS_PROFILE=dev-admin uv run scripts/bootstrap_edge_gate.py --force
"""

from __future__ import annotations

import argparse
import secrets
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ENV_DIR = Path(__file__).resolve().parent.parent / "infra" / "environments" / "dev"
PLACEHOLDER = "PLACEHOLDER-overwrite-out-of-band"
SECRET_BYTES = 32
GATE_CONFIG_KEY = "k"


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


def fatal(message: str, err: ClientError | None = None) -> None:
    print(f"\nFAILED: {message}", file=sys.stderr)
    if err is not None:
        code = err.response.get("Error", {}).get("Code", "?")
        detail = err.response.get("Error", {}).get("Message", str(err))
        print(f"  {code}: {detail}", file=sys.stderr)
    sys.exit(1)


def read_current_secret(ssm, parameter_name: str) -> str | None:
    """The SSM parameter's current value, or `None` if it does not exist yet."""
    try:
        resp = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return resp["Parameter"]["Value"]
    except ClientError as err:
        if err.response["Error"]["Code"] == "ParameterNotFound":
            return None
        fatal(f"could not read {parameter_name}", err)
        raise  # unreachable; fatal() exits


def write_ssm_secret(ssm, parameter_name: str, secret: str) -> None:
    try:
        ssm.put_parameter(
            Name=parameter_name,
            Value=secret,
            Type="SecureString",
            Overwrite=True,
        )
    except ClientError as err:
        fatal(f"could not write {parameter_name}", err)


def write_kvs_secret(kvs_data, kvs_arn: str, secret: str) -> None:
    try:
        etag = kvs_data.describe_key_value_store(KvsARN=kvs_arn)["ETag"]
        kvs_data.put_key(KvsARN=kvs_arn, Key=GATE_CONFIG_KEY, Value=secret, IfMatch=etag)
    except ClientError as err:
        fatal(f"could not write key '{GATE_CONFIG_KEY}' to the gate KeyValueStore", err)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--force",
        action="store_true",
        help="regenerate and overwrite even if SSM already holds a real secret "
        "(invalidates every live session immediately)",
    )
    args = parser.parse_args()

    say("Reading Terraform outputs")
    parameter_name = tf_output("signing_key_parameter_name")
    kvs_arn = tf_output("gate_kvs_arn")
    print(f"  signing key parameter: {parameter_name}")
    print(f"  gate KeyValueStore ARN: {kvs_arn}")

    ssm = boto3.client("ssm")
    kvs_data = boto3.client("cloudfront-keyvaluestore")

    say("Checking the current SSM value")
    current = read_current_secret(ssm, parameter_name)
    if current is not None and current != PLACEHOLDER and not args.force:
        fatal(
            f"{parameter_name} already holds a non-placeholder value. "
            "Re-running would regenerate the secret and invalidate every live "
            "session. Pass --force if that is really what you want."
        )

    if current is None or current == PLACEHOLDER or args.force:
        say("Generating a new secret")
        secret = secrets.token_urlsafe(SECRET_BYTES)
        write_ssm_secret(ssm, parameter_name, secret)
        print(f"  wrote {parameter_name}")
    else:
        secret = current
        print("  SSM already holds a real secret; reusing it (mirroring only)")

    say("Mirroring the secret to the gate's KeyValueStore")
    write_kvs_secret(kvs_data, kvs_arn, secret)
    print(f"  wrote key '{GATE_CONFIG_KEY}'")

    say("Done")
    print(
        "SSM and the KeyValueStore now agree. Allow the KeyValueStore write a "
        "moment to propagate (median ~31s to one edge location, ADR-0021 §6) "
        "before relying on the gate for a real event."
    )


if __name__ == "__main__":
    main()
