# Virtual Waiting Room — deploy convenience targets.
#
# The Terraform root is infra/environments/dev. `build` cross-compiles the Rust
# Lambdas with cargo-lambda, which writes a ready-to-deploy zip per function at
# $(ARTIFACTS)/<crate>/bootstrap.zip, which the Terraform root reads from a fixed
# path per crate and deploys directly (no re-zip). plan and apply build first,
# because every function is required and there is no stub to fall back to.
#
# Usage:
#   make build                 # cross-compile every function to a zip
#   make plan                  # terraform plan with the built artifacts
#   make apply                 # terraform apply (real deploy — needs AWS creds)
#   make destroy               # tear the stack down (prompts to confirm)
#
# Deployment config (region, aws_profile, event_id, lambda_architecture)
# lives in infra/environments/dev/terraform.tfvars and is
# authoritative — this Makefile passes no -var, which would override the file.

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

ROOT        := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ENV_DIR     := $(ROOT)/infra/environments/dev
MANIFEST    := $(ROOT)/Cargo.toml
ARTIFACTS   := $(ROOT)/.artifacts
TFVARS      := $(ENV_DIR)/terraform.tfvars

# The build target is read from lambda_architecture in terraform.tfvars, the
# same value Terraform deploys the functions with, so the two cannot drift into
# a stack whose binaries do not match its functions. Falling back to arm64
# matches the Terraform default for a tfvars that does not set it (the file is
# gitignored, so it may not exist at all). ARCH= still overrides for a one-off.
ARCH ?= $(shell awk -F'"' '/^[[:space:]]*lambda_architecture[[:space:]]*=/ {print $$2; found=1} END {if (!found) print "arm64"}' $(TFVARS) 2>/dev/null || echo arm64)

# cargo-lambda cross-compiles for x86_64 by default; --arm64 selects Graviton.
ifeq ($(ARCH),arm64)
ARCH_FLAG := --arm64
else
ARCH_FLAG :=
endif

# Each crate is built separately (every bin is named `bootstrap`, so a single
# --output-format zip invocation would collide them under one dir). Each
# per-crate build writes $(ARTIFACTS)/<crate>/bootstrap/bootstrap.zip.
#
# The authorizer is built and deployed like the rest, but nothing in this
# account invokes it: it attaches at the customer's own origin.
LAMBDA_CRATES := assign_position seal_event read admin controller authorizer generate_token

ASSIGN_ARTIFACT     := $(ARTIFACTS)/assign_position/bootstrap/bootstrap.zip
SEAL_ARTIFACT       := $(ARTIFACTS)/seal_event/bootstrap/bootstrap.zip
READ_ARTIFACT       := $(ARTIFACTS)/read/bootstrap/bootstrap.zip
ADMIN_ARTIFACT      := $(ARTIFACTS)/admin/bootstrap/bootstrap.zip
CONTROLLER_ARTIFACT := $(ARTIFACTS)/controller/bootstrap/bootstrap.zip
AUTHORIZER_ARTIFACT := $(ARTIFACTS)/authorizer/bootstrap/bootstrap.zip
TOKEN_ARTIFACT      := $(ARTIFACTS)/generate_token/bootstrap/bootstrap.zip

# Local load generation. `harness` is a development tool, not a Lambda, so it is
# deliberately absent from LAMBDA_CRATES and never packaged. Override on the
# command line: make load VISITORS=2000 POLLING=every-tick
VISITORS := 500
SECONDS  := 30
POLLING  := hold-position

.PHONY: help build init plan apply destroy fmt validate clean load

help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Cross-compile every Lambda to a zip under .artifacts/<crate>/.
	@for crate in $(LAMBDA_CRATES); do \
		cargo lambda build --release $(ARCH_FLAG) --output-format zip \
			--lambda-dir $(ARTIFACTS)/$$crate \
			-p $$crate --manifest-path $(MANIFEST); \
	done
	@echo "built: $(ASSIGN_ARTIFACT) $(SEAL_ARTIFACT) $(READ_ARTIFACT) $(ADMIN_ARTIFACT) $(CONTROLLER_ARTIFACT) $(AUTHORIZER_ARTIFACT) $(TOKEN_ARTIFACT)"

init: ## terraform init (safe, idempotent).
	terraform -chdir=$(ENV_DIR) init -input=false

plan: build init ## Build the Lambdas then terraform plan.
	terraform -chdir=$(ENV_DIR) plan -input=false

apply: build init ## Build the Lambdas then terraform apply (needs AWS credentials).
	terraform -chdir=$(ENV_DIR) apply -input=false

destroy: init ## Tear the stack down (Terraform prompts for confirmation).
	terraform -chdir=$(ENV_DIR) destroy

fmt: ## terraform fmt across the infra tree.
	terraform -chdir=$(ROOT)/infra fmt -recursive

validate: build init ## Build the Lambdas then terraform validate the dev root.
	terraform -chdir=$(ENV_DIR) validate

clean: ## Remove staged Lambda artifacts.
	rm -rf -- $(ARTIFACTS)

load: ## Generate waiting-room load locally and report origin requests per visitor.
	cargo run -q -p harness -- --visitors $(VISITORS) --seconds $(SECONDS) --polling $(POLLING)
