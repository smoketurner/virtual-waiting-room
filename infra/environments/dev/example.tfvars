# Copy to terraform.tfvars and edit. Terraform auto-loads terraform.tfvars from
# this directory (the -chdir root); the Makefile passes no -var, so this file is
# the single source of truth. terraform.tfvars is gitignored; this template is
# tracked.

# --- Deployment ---------------------------------------------------------------
region              = "us-east-1"
aws_profile         = "dev-admin" # "" = default credential chain
event_id            = "smoke"
lambda_architecture = "arm64" # must match `make build ARCH=...`

# Bare domain of the protected origin CloudFront fronts as its default
# behaviour. Host only - no scheme, no path.
#
# Leave it empty to protect the built-in demo origin instead, which is what you
# want for testing: CloudFront forwards the viewer's Host to the protected
# origin, so a third-party site answers 404 for a hostname it does not serve and
# any redirect it issues takes the visitor off the distribution entirely.
client_origin_domain_name = ""

# --- Custom domain ------------------------------------------------------------
# The hostname visitors actually use. Empty deploys on the distribution's own
# *.cloudfront.net name, which works and is the testing default.
#
# A real event sets both: the session cookie is issued for the host the visitor
# is on, so a waiting room on one domain and the origin it protects on another
# hands the origin a cookie its requests never carry. The certificate must be in
# us-east-1 and cover every name listed, and the two must be set together.
aliases             = []
acm_certificate_arn = ""

# --- Admin OIDC login (ADR-0016) ----------------------------------------------
# The client SECRET is NOT here — write it to the SSM SecureString out of band:
#   aws ssm put-parameter --name /<name_prefix>/oidc-client-secret \
#     --type SecureString --value '<secret>' --overwrite
oidc_issuer         = "https://us.vouch.sh"
oidc_client_id      = ""
oidc_redirect_uri   = "" # e.g. https://<host>/admin/callback
oidc_allowed_emails = "" # comma-separated; "" = deny all (fail closed)

# --- Adaptive poll policy (#69, ADR-0023) -------------------------------------
# Published on /status; waiting.js clamps its poll interval to these, polling
# less often the further a visitor is from the front. Omitting all three (or
# leaving this section out) makes the client fall back to the fixed 5s
# interval it always used.
poll_floor_ms   = 5000
poll_ceiling_ms = 30000
poll_divisor    = 10

# --- Event seeding ------------------------------------------------------------
# Seeded onto the stack at apply so a fresh deployment serves without an
# operator driving a script or the dashboard first. The control plane owns all
# of it from there: Terraform writes these once and then ignores changes.

# Visitors per second. Zero would release nobody, forever, while the controller
# logged a successful pass every ten seconds, so there is no zero default.
admission_rate = 5

# Which requests the gate covers, one rule per line, in the same grammar the
# dashboard's Set rules form takes. Empty (the default) is dormant: every
# request passes through untouched.
#
# gate_rules = <<-RULES
#   p /checkout
#   c loyalty_member
#   u HeadlessChrome
#   h x-internal-monitor true
# RULES

# When the event opens, local date-time with no zone, evaluated in the timezone
# below. Empty leaves the one-time schedule disabled at a placeholder instant
# that is never the real value, so an accidental enable cannot fire an open.
#
# starts_at          = "2027-03-14T10:00:00"
# starts_at_timezone = "America/New_York"

# --- Sessions -----------------------------------------------------------------
# How long an admitted visitor's session cookie lasts. Nothing renews it at the
# edge, so a visitor still on the origin when it lapses is returned to the
# queue: set it comfortably longer than the worst realistic time on the origin,
# not tightly around the average.
session_ttl_seconds = 3600
