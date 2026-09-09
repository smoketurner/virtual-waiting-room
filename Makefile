# Virtual Waiting Room — deploy convenience targets.
#
# The Terraform root is infra/environments/dev. The three Rust Lambdas each
# compile to a binary named `bootstrap`, so `build` stages them under
# $(ARTIFACTS) with distinct names, and plan/apply pass those paths as vars.
#
# Usage:
#   make build                 # compile + stage the three bootstraps
#   make plan                  # terraform plan with the built artifacts
#   make apply                 # terraform apply (real deploy — needs AWS creds)
#   make destroy               # tear the stack down (prompts to confirm)
#
# Common overrides (make apply ARCH=arm64 EVENT_ID=launch SEAL_START=2026-09-10T18:00:00):
#   ARCH        Lambda CPU architecture: x86_64 (default) or arm64. Must match
#               the built artifacts — the host toolchain here builds x86_64.
#   EVENT_ID    The single event id this deployment serves (default: default).
#   SEAL_START  One-time UTC seal time, EventBridge at() value. Empty = manual.
#   REGION      AWS region (default: us-east-1).

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

# cargo-lambda's --target flag wants the Rust triple for the chosen arch.
ifeq ($(ARCH),arm64)
LAMBDA_TARGET := aarch64-unknown-linux-gnu
else
LAMBDA_TARGET := x86_64-unknown-linux-gnu
endif

ASSIGN_ARTIFACT := $(ARTIFACTS)/assign_position-bootstrap
SEAL_ARTIFACT   := $(ARTIFACTS)/seal_event-bootstrap
READ_ARTIFACT   := $(ARTIFACTS)/read-bootstrap

# Vars threaded into every plan/apply so the real functions deploy (empty paths
# leave the vendored placeholders and disable the join event source mapping).
TF_VARS := \
	-var "event_id=$(EVENT_ID)" \
	-var "lambda_architecture=$(ARCH)" \
	-var "seal_start_time=$(SEAL_START)" \
	-var "assign_position_artifact_path=$(ASSIGN_ARTIFACT)" \
	-var "seal_event_artifact_path=$(SEAL_ARTIFACT)" \
	-var "read_artifact_path=$(READ_ARTIFACT)"

.PHONY: help build init plan apply destroy fmt validate clean

help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Compile the three Lambdas and stage their bootstraps under .artifacts/.
	cargo lambda build --release --target $(LAMBDA_TARGET) \
		-p assign_position -p seal_event -p read \
		--manifest-path $(CRATES_DIR)/Cargo.toml
	@mkdir -p $(ARTIFACTS)
	@cp $(CRATES_DIR)/target/lambda/assign_position/bootstrap $(ASSIGN_ARTIFACT)
	@cp $(CRATES_DIR)/target/lambda/seal_event/bootstrap      $(SEAL_ARTIFACT)
	@cp $(CRATES_DIR)/target/lambda/read/bootstrap            $(READ_ARTIFACT)
	@echo "staged: $(ASSIGN_ARTIFACT) $(SEAL_ARTIFACT) $(READ_ARTIFACT)"

init: ## terraform init (safe, idempotent).
	terraform -chdir=$(ENV_DIR) init -input=false

plan: init ## terraform plan against the built artifacts.
	terraform -chdir=$(ENV_DIR) plan -input=false $(TF_VARS)

apply: init ## terraform apply — real deploy (needs AWS credentials).
	terraform -chdir=$(ENV_DIR) apply -input=false $(TF_VARS)

destroy: init ## Tear the stack down (Terraform prompts for confirmation).
	terraform -chdir=$(ENV_DIR) destroy $(TF_VARS)

fmt: ## terraform fmt across the infra tree.
	terraform -chdir=$(ROOT)/infra fmt -recursive

validate: init ## terraform validate the dev root.
	terraform -chdir=$(ENV_DIR) validate

clean: ## Remove staged Lambda artifacts.
	rm -rf $(ARTIFACTS)
