# Virtual Waiting Room — deploy convenience targets.
#
# The Terraform root is infra/environments/dev. `build` cross-compiles the Rust
# Lambdas with cargo-lambda, which writes a ready-to-deploy zip per function at
# $(ARTIFACTS)/<crate>/bootstrap.zip; plan/apply pass those zip paths as vars and
# Terraform deploys them directly (no re-zip).
#
# Usage:
#   make build                 # cross-compile every function to a zip
#   make plan                  # terraform plan with the built artifacts
#   make apply                 # terraform apply (real deploy — needs AWS creds)
#   make destroy               # tear the stack down (prompts to confirm)
#
# Deployment config (region, aws_profile, event_id, lambda_architecture,
# seal_start_time) lives in infra/environments/dev/terraform.tfvars and is
# authoritative — this Makefile does not pass those as -var (which would override
# the file). Only the built artifact paths are passed.
#
# The one build-time override (make build ARCH=arm64):
#   ARCH        cargo-lambda build target: x86_64 (default) or arm64. Keep this
#               in sync with lambda_architecture in terraform.tfvars.

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

ROOT        := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ENV_DIR     := $(ROOT)/infra/environments/dev
MANIFEST    := $(ROOT)/Cargo.toml
ARTIFACTS   := $(ROOT)/.artifacts

ARCH        ?= x86_64

# cargo-lambda cross-compiles for x86_64 by default; --arm64 selects Graviton.
# ARCH only selects the BUILD target here; the Lambda's lambda_architecture is
# set in terraform.tfvars and must be kept in sync with what you build.
ifeq ($(ARCH),arm64)
ARCH_FLAG := --arm64
else
ARCH_FLAG :=
endif

# Each crate is built separately (every bin is named `bootstrap`, so a single
# --output-format zip invocation would collide them under one dir). Each
# per-crate build writes $(ARTIFACTS)/<crate>/bootstrap/bootstrap.zip.
#
# controller and authorizer are built here but deploy only when their enable_*
# variable is set: the controller needs a schedule to fire it, and the authorizer
# is attached at the customer's origin rather than to anything in this account.
LAMBDA_CRATES := assign_position seal_event read admin controller authorizer

ASSIGN_ARTIFACT     := $(ARTIFACTS)/assign_position/bootstrap/bootstrap.zip
SEAL_ARTIFACT       := $(ARTIFACTS)/seal_event/bootstrap/bootstrap.zip
READ_ARTIFACT       := $(ARTIFACTS)/read/bootstrap/bootstrap.zip
ADMIN_ARTIFACT      := $(ARTIFACTS)/admin/bootstrap/bootstrap.zip
CONTROLLER_ARTIFACT := $(ARTIFACTS)/controller/bootstrap/bootstrap.zip
AUTHORIZER_ARTIFACT := $(ARTIFACTS)/authorizer/bootstrap/bootstrap.zip

# All Terraform config — region, aws_profile, event_id, lambda_architecture,
# seal_start_time, and the *_artifact_path values — lives in
# infra/environments/dev/terraform.tfvars, which Terraform auto-loads from the
# -chdir root and is the single source of truth. This Makefile passes no -var:
# a command-line -var would override the file. See example.tfvars for the shape;
# the artifact paths are the deterministic build outputs
# (.artifacts/<crate>/bootstrap/bootstrap.zip), empty/unset -> placeholder Lambda.

.PHONY: help build init plan apply destroy fmt validate clean

help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Cross-compile every Lambda to a zip under .artifacts/<crate>/.
	@for crate in $(LAMBDA_CRATES); do \
		cargo lambda build --release $(ARCH_FLAG) --output-format zip \
			--lambda-dir $(ARTIFACTS)/$$crate \
			-p $$crate --manifest-path $(MANIFEST); \
	done
	@echo "built: $(ASSIGN_ARTIFACT) $(SEAL_ARTIFACT) $(READ_ARTIFACT) $(ADMIN_ARTIFACT) $(CONTROLLER_ARTIFACT) $(AUTHORIZER_ARTIFACT)"

init: ## terraform init (safe, idempotent).
	terraform -chdir=$(ENV_DIR) init -input=false

plan: init ## terraform plan (config + artifact paths from terraform.tfvars).
	terraform -chdir=$(ENV_DIR) plan -input=false

apply: build init ## Build the Lambdas then terraform apply (needs AWS credentials).
	terraform -chdir=$(ENV_DIR) apply -input=false

destroy: init ## Tear the stack down (Terraform prompts for confirmation).
	terraform -chdir=$(ENV_DIR) destroy

fmt: ## terraform fmt across the infra tree.
	terraform -chdir=$(ROOT)/infra fmt -recursive

validate: init ## terraform validate the dev root.
	terraform -chdir=$(ENV_DIR) validate

clean: ## Remove staged Lambda artifacts.
	rm -rf $(ARTIFACTS)
