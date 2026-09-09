# Virtual Waiting Room — deploy convenience targets.
#
# The Terraform root is infra/environments/dev. `build` cross-compiles the three
# Rust Lambdas with cargo-lambda, which writes a ready-to-deploy zip per function
# at $(ARTIFACTS)/<crate>/bootstrap.zip; plan/apply pass those zip paths as vars
# and Terraform deploys them directly (no re-zip).
#
# Usage:
#   make build                 # cross-compile the three functions to zips
#   make plan                  # terraform plan with the built artifacts
#   make apply                 # terraform apply (real deploy — needs AWS creds)
#   make destroy               # tear the stack down (prompts to confirm)
#
# Common overrides (make apply ARCH=arm64 EVENT_ID=launch SEAL_START=2026-09-10T18:00:00):
#   ARCH        Lambda CPU architecture: x86_64 (default) or arm64. Must match
#               the built artifacts.
#   EVENT_ID    The single event id this deployment serves (default: default).
#   SEAL_START  One-time UTC seal time, EventBridge at() value. Empty = manual.
#   REGION      AWS region (default: us-east-1).
#   PROFILE     Named AWS profile to authenticate with. Empty = default chain.

SHELL       := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

ROOT        := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ENV_DIR     := $(ROOT)/infra/environments/dev
CRATES_DIR  := $(ROOT)/crates
ARTIFACTS   := $(ROOT)/.artifacts

ARCH        ?= x86_64
EVENT_ID    ?= default
SEAL_START  ?=
REGION      ?= us-east-1
PROFILE     ?= dev-admin

# cargo-lambda cross-compiles for x86_64 by default; --arm64 selects Graviton.
ifeq ($(ARCH),arm64)
ARCH_FLAG := --arm64
else
ARCH_FLAG :=
endif

# Each crate is built separately (all three bins are named `bootstrap`, so a
# single --output-format zip invocation would collide them under one dir). Each
# per-crate build writes $(ARTIFACTS)/<crate>/bootstrap/bootstrap.zip.
ASSIGN_ARTIFACT := $(ARTIFACTS)/assign_position/bootstrap/bootstrap.zip
SEAL_ARTIFACT   := $(ARTIFACTS)/seal_event/bootstrap/bootstrap.zip
READ_ARTIFACT   := $(ARTIFACTS)/read/bootstrap/bootstrap.zip

# An artifact path is passed to Terraform only when its zip is actually built
# ($(wildcard) is empty when absent). A missing zip falls back to the vendored
# placeholder Lambda, so `plan` works before `build`. Run `make build` first to
# deploy the real functions.
TF_VARS := \
	-var "region=$(REGION)" \
	-var "aws_profile=$(PROFILE)" \
	-var "event_id=$(EVENT_ID)" \
	-var "lambda_architecture=$(ARCH)" \
	-var "seal_start_time=$(SEAL_START)" \
	-var "assign_position_artifact_path=$(wildcard $(ASSIGN_ARTIFACT))" \
	-var "seal_event_artifact_path=$(wildcard $(SEAL_ARTIFACT))" \
	-var "read_artifact_path=$(wildcard $(READ_ARTIFACT))"

.PHONY: help build init plan apply destroy fmt validate clean

help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Cross-compile the three Lambdas to zips under .artifacts/<crate>/.
	@for crate in assign_position seal_event read; do \
		cargo lambda build --release $(ARCH_FLAG) --output-format zip \
			--lambda-dir $(ARTIFACTS)/$$crate \
			-p $$crate --manifest-path $(CRATES_DIR)/Cargo.toml; \
	done
	@echo "built: $(ASSIGN_ARTIFACT) $(SEAL_ARTIFACT) $(READ_ARTIFACT)"

init: ## terraform init (safe, idempotent).
	terraform -chdir=$(ENV_DIR) init -input=false

plan: init ## terraform plan (uses staged artifacts if built, else placeholders).
	terraform -chdir=$(ENV_DIR) plan -input=false $(TF_VARS)

apply: build init ## Build the Lambdas then terraform apply (needs AWS credentials).
	terraform -chdir=$(ENV_DIR) apply -input=false $(TF_VARS)

destroy: init ## Tear the stack down (Terraform prompts for confirmation).
	terraform -chdir=$(ENV_DIR) destroy $(TF_VARS)

fmt: ## terraform fmt across the infra tree.
	terraform -chdir=$(ROOT)/infra fmt -recursive

validate: init ## terraform validate the dev root.
	terraform -chdir=$(ENV_DIR) validate

clean: ## Remove staged Lambda artifacts.
	rm -rf $(ARTIFACTS)
