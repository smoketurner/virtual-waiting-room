# --- CloudFront flat-rate plan (ADR-0038) --------------------------------------
#
# Bundles the distribution, the web ACL it requires, and the gate's
# KeyValueStore into one monthly price with no overage charges.
#
# IMMEDIATE approval: setting the tier and confirming the apply is the decision
# to pay. MANUAL would park the plan unbilled and inactive while the web ACL
# already bills pay-as-you-go.
#
# Destroying this cancels the plan, but AWS keeps it, and refuses to delete the
# distribution or detach its web ACL, until the billing period ends.

resource "aws_pricingplanmanager_subscription" "this" {
  count = local.waf_enabled ? 1 : 0

  plan_family   = "CloudFront"
  plan_tier     = var.flat_rate_plan_tier
  approval_mode = "IMMEDIATE"

  resource_arns = [
    aws_cloudfront_distribution.this.arn,
    aws_wafv2_web_acl.this[0].arn,
    var.gate_kvs_arn,
  ]
}
