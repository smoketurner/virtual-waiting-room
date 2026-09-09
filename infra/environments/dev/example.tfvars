# Copy to terraform.tfvars and edit. Terraform auto-loads terraform.tfvars from
# this directory (the -chdir root); the Makefile passes no -var, so this file is
# the single source of truth. terraform.tfvars is gitignored; this template is
# tracked.

# --- Deployment ---------------------------------------------------------------
region              = "us-east-1"
aws_profile         = "dev-admin" # "" = default credential chain
event_id            = "smoke"
lambda_architecture = "arm64"     # must match `make build ARCH=...`
seal_start_time     = ""          # EventBridge at() value; "" = manual seal

# --- Built Lambda artifacts ---------------------------------------------------
# Deterministic build outputs, relative to this directory. Leave empty ("") to
# fall back to the vendored placeholder Lambda (lets `make plan` run pre-build).
assign_position_artifact_path = "../../../.artifacts/assign_position/bootstrap/bootstrap.zip"
seal_event_artifact_path      = "../../../.artifacts/seal_event/bootstrap/bootstrap.zip"
read_artifact_path            = "../../../.artifacts/read/bootstrap/bootstrap.zip"
admin_artifact_path           = "../../../.artifacts/admin/bootstrap/bootstrap.zip"

# --- Admin OIDC login (ADR-0016) ----------------------------------------------
# The client SECRET is NOT here — write it to the SSM SecureString out of band:
#   aws ssm put-parameter --name /<name_prefix>/oidc-client-secret \
#     --type SecureString --value '<secret>' --overwrite
oidc_issuer         = "https://us.vouch.sh"
oidc_client_id      = ""
oidc_redirect_uri   = "" # e.g. https://<host>/admin/callback
oidc_allowed_emails = "" # comma-separated; "" = deny all (fail closed)
