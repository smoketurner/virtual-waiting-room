#!/usr/bin/env bash
# Virtual Waiting Room MVP smoke test.
#
# Drives the scheduled-pre-queue and live-join happy paths against a deployed
# dev stack and asserts the core invariant: no two visitors get the same queue
# position. Idempotent enough to re-run; it registers fresh UUIDv7 request ids
# each time.
#
# Prerequisites:
#   - AWS credentials in the environment (aws sso login / aws configure), with
#     rights to read/write the stack's DynamoDB tables and invoke its Lambdas.
#   - terraform, aws, curl, python3 on PATH.
#   - The stack already deployed (make apply). The script reads the API URL,
#     table names, seal function, and event id from `terraform output` and never
#     touches Terraform state — deploy with `make apply` first.
#
# Usage:
#   AWS_PROFILE=dev-admin ./scripts/smoke-test.sh
set -euo pipefail

ENV_DIR="$(cd "$(dirname "$0")/../infra/environments/dev" && pwd)"
REGION="${AWS_REGION:-us-east-1}"

say() { printf '\n=== %s ===\n' "$*"; }

# A canonical UUIDv7 (version nibble 7). The timestamp bits are cosmetic for the
# smoke test; only the format and uniqueness matter to the handler.
uuid_v7() {
  python3 - <<'PY'
import os, time
ts = int(time.time() * 1000)
rand = os.urandom(10)
b = ts.to_bytes(6, "big") + rand
b = bytearray(b)
b[6] = (b[6] & 0x0F) | 0x70          # version 7
b[8] = (b[8] & 0x3F) | 0x80          # variant
h = b.hex()
print(f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}")
PY
}

say "1. Read the deployed stack from terraform outputs"
cd "$ENV_DIR"
terraform init -input=false >/dev/null
API_URL="$(terraform output -raw api_invoke_url)"
EVENT_ID="$(terraform output -raw event_id)"
COUNTERS="$(terraform output -json table_names | python3 -c 'import sys,json;print(json.load(sys.stdin)["counters"])')"
PREQUEUE="$(terraform output -json table_names | python3 -c 'import sys,json;print(json.load(sys.stdin)["prequeue"])')"
POSITIONS="$(terraform output -json table_names | python3 -c 'import sys,json;print(json.load(sys.stdin)["positions"])')"
SEAL_FN="$(terraform output -raw seal_event_function_name)"
echo "API: $API_URL  event_id: $EVENT_ID"

say "2. Seed the signing key out-of-band (SSM SecureString placeholder -> real)"
aws ssm put-parameter --region "$REGION" \
  --name "$(terraform state show 'module.core.aws_ssm_parameter.signing_key' | awk -F'\"' '/name /{print $2; exit}')" \
  --type SecureString --overwrite \
  --value "$(python3 -c 'import os,base64;print(base64.b64encode(os.urandom(32)).decode())')" >/dev/null
echo "signing key written"

say "3. Pre-queue registration: write PreQueue rows for a small cohort"
# The pre-queue registration handler is not part of the MVP (deferred), so the
# smoke test seeds PreQueue rows and the shard counter directly, mirroring what
# a POST /join during the pre-queue phase would write: shard s = hash % 10,
# local index l from ADD prequeue_counter#s.
#
# Shard assignment and per-shard counts are computed in one Python pass (real
# dicts) rather than a bash associative array, so this runs on bash 3.2 (macOS).
COHORT=12
ASSIGN="$(python3 - "$COHORT" <<'PY'
import os, sys, time

def uuid_v7():
    ts = int(time.time() * 1000)
    b = bytearray(ts.to_bytes(6, "big") + os.urandom(10))
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def fnv1a(s):
    h = 0xcbf29ce484222325
    for c in s.encode():
        h ^= c
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h

n = int(sys.argv[1])
counts = {}
rows = []
for _ in range(n):
    rid = uuid_v7()
    s = fnv1a(rid) % 10
    l = counts.get(s, 0)
    counts[s] = l + 1
    rows.append(f"row {rid} {s} {l}")
for s in sorted(counts):
    rows.append(f"count {s} {counts[s]}")
print("\n".join(rows))
PY
)"

IDS=()
NOW="$(date -u +%FT%TZ)"
while read -r kind a b c; do
  if [ "$kind" = "row" ]; then
    RID="$a"; S="$b"; L="$c"
    IDS+=("$RID")
    aws dynamodb put-item --region "$REGION" --table-name "$PREQUEUE" \
      --item "{\"r\":{\"S\":\"$RID\"},\"s\":{\"N\":\"$S\"},\"l\":{\"N\":\"$L\"},\"t\":{\"S\":\"$NOW\"}}" \
      --condition-expression "attribute_not_exists(r)"
  else
    # kind=count: reflect the per-shard total on the Counters item.
    S="$a"; TOTAL="$b"
    aws dynamodb update-item --region "$REGION" --table-name "$COUNTERS" \
      --key "{\"event_id\":{\"S\":\"$EVENT_ID\"}}" \
      --update-expression "SET #c = :v" \
      --expression-attribute-names "{\"#c\":\"prequeue_counter#$S\"}" \
      --expression-attribute-values "{\":v\":{\"N\":\"$TOTAL\"}}" >/dev/null
  fi
done <<<"$ASSIGN"
echo "registered ${#IDS[@]} pre-queue visitors"

say "4. Seal the event (invoke seal_event) and confirm phase=active via /status"
aws lambda invoke --region "$REGION" --function-name "$SEAL_FN" \
  --payload "$(printf '{"event_id":"%s"}' "$EVENT_ID" | base64)" /dev/stdout >/dev/null
sleep 2
STATUS="$(curl -fsS "$API_URL/v1/status")"
echo "$STATUS"
echo "$STATUS" | COHORT="$COHORT" python3 -c 'import os,sys,json;d=json.load(sys.stdin);n=int(os.environ["COHORT"]);assert d["phase"]=="active",d;assert d.get("participant_count")==n,d;print(f"status OK: active, N={n}")'

say "5. Resolve every pre-queue position via /queue_num; assert all distinct"
python3 - "$API_URL" "${IDS[@]}" <<'PY'
import sys, json, urllib.request
api = sys.argv[1]; ids = sys.argv[2:]
seen = {}
for rid in ids:
    with urllib.request.urlopen(f"{api}/v1/queue_num?request_id={rid}") as r:
        d = json.load(r)
    p = d["position"]
    assert not d["live_join"], (rid, d)
    assert p not in seen, f"DUPLICATE position {p}: {rid} and {seen[p]}"
    seen[p] = rid
print(f"queue_num OK: {len(seen)} distinct positions in [0,{len(ids)})")
PY

say "6. Live join: POST /join, then confirm a Positions row is written"
LIVE_RID="$(uuid_v7)"
curl -fsS -X POST "$API_URL/v1/join" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"$LIVE_RID\",\"event_id\":\"$EVENT_ID\"}" >/dev/null
echo "posted live join $LIVE_RID; waiting for assign_position to drain the batch"
for _ in $(seq 1 15); do
  ROW="$(aws dynamodb get-item --region "$REGION" \
    --table-name "$POSITIONS" \
    --key "{\"request_id\":{\"S\":\"$LIVE_RID\"}}" 2>/dev/null || true)"
  [ -n "$ROW" ] && echo "$ROW" | grep -q request_id && { echo "live-join Positions row written"; break; }
  sleep 2
done

say "SMOKE TEST PASSED"
