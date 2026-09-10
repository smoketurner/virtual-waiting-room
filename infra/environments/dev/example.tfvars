# Copy to terraform.tfvars and edit. Terraform auto-loads terraform.tfvars from
# this directory (the -chdir root); the Makefile passes no -var, so this file is
# the single source of truth. terraform.tfvars is gitignored; this template is
# tracked.

# --- Deployment ---------------------------------------------------------------
region              = "us-east-1"
aws_profile         = "dev-admin" # "" = default credential chain
event_id            = "smoke"
lambda_architecture = "arm64" # must match `make build ARCH=...`
seal_start_time     = ""      # EventBridge at() value; "" = manual seal

# REQUIRED (no default): bare domain of the protected origin CloudFront fronts as
# its default behaviour. Host only - no scheme, no path. An empty or missing
# value fails the plan rather than silently destroying the distribution.
client_origin_domain_name = "www.example.com"

# --- Built Lambda artifacts ---------------------------------------------------
# Deterministic build outputs, relative to this directory. Leave empty ("") to
# fall back to the vendored placeholder Lambda (lets `make plan` run pre-build).
assign_position_artifact_path = "../../../.artifacts/assign_position/bootstrap/bootstrap.zip"
seal_event_artifact_path      = "../../../.artifacts/seal_event/bootstrap/bootstrap.zip"
read_artifact_path            = "../../../.artifacts/read/bootstrap/bootstrap.zip"
admin_artifact_path           = "../../../.artifacts/admin/bootstrap/bootstrap.zip"
# Supplying controller_artifact_path also creates the schedule that meters
# admission and expires positions. Leave it empty and the queue forms but never
# drains.
controller_artifact_path = "../../../.artifacts/controller/bootstrap/bootstrap.zip"

# The authorizer gates the customer's protected origin and is the only writer of
# the arrivals counters the controller measures no-shows against. Building it
# creates the function and exports its ARN; attaching it at the origin happens
# where the origin lives.
authorizer_artifact_path = "../../../.artifacts/authorizer/bootstrap/bootstrap.zip"

# Mints the CloudFront admission cookies. Without it nobody can be let through,
# because the protected behaviour refuses every request that carries none.
generate_token_artifact_path = "../../../.artifacts/generate_token/bootstrap/bootstrap.zip"

# --- Admin OIDC login (ADR-0016) ----------------------------------------------
# The client SECRET is NOT here — write it to the SSM SecureString out of band:
#   aws ssm put-parameter --name /<name_prefix>/oidc-client-secret \
#     --type SecureString --value '<secret>' --overwrite
oidc_issuer         = "https://us.vouch.sh"
oidc_client_id      = ""
oidc_redirect_uri   = "" # e.g. https://<host>/admin/callback
oidc_allowed_emails = "" # comma-separated; "" = deny all (fail closed)
