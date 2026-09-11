# modules/core - the CloudFront KeyValueStore backing the edge gate (issue
# #71): everything the CloudFront Function (modules/edge) reads at
# viewer-request. One store, two keys — reading more than a couple of keys
# costs sequential awaits on the hot path, so the design stays at two
# (docs/adr/0021-edge-function-gate.md §2).
#
# The store lives here rather than in modules/edge because its only writer is
# the admin Lambda, which lives here: putting the store in edge would need
# edge to export its ARN back to core, a module cycle. core exports
# gate_kvs_arn; edge consumes it. Same shape, same resolution, as the
# CloudFront key pair ADR-0020 put in core for the same reason (now retired).

resource "aws_cloudfront_key_value_store" "gate" {
  name    = "${var.name_prefix}-gate"
  comment = "Virtual Waiting Room edge gate config (issue #71): 'c' the ruleset + epochs, 'k' the signing secret."
}

# 'c': the gate's whole configuration. Seeded as valid, dormant JSON — 'r': []
# matches no rule, so a fresh stack passes every request through *by
# configuration* rather than being broken-open by an unrecognised or
# unparsable value (issue #60). ignore_changes because the admin Lambda
# mutates fail_open_until (the 'f' field) out of band.
resource "aws_cloudfrontkeyvaluestore_key" "config" {
  key                 = "c"
  key_value_store_arn = aws_cloudfront_key_value_store.gate.arn
  value               = jsonencode({ v = 1, s = 0, f = 0, r = [] })

  lifecycle {
    ignore_changes = [value]
  }
}

# 'k': the signing secret, byte-identical to aws_ssm_parameter.signing_key.
# Both start on the same placeholder literal and are overwritten together,
# out of band, by scripts/bootstrap_edge_gate.py — see main.tf's
# aws_ssm_parameter.signing_key for why a placeholder rather than a
# Terraform-generated secret (it would land in state).
resource "aws_cloudfrontkeyvaluestore_key" "secret" {
  key                 = "k"
  key_value_store_arn = aws_cloudfront_key_value_store.gate.arn
  value               = "PLACEHOLDER-overwrite-out-of-band" # nosemgrep: not a real secret

  lifecycle {
    ignore_changes = [value]
  }
}
