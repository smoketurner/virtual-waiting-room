# Asserts the PreQueue table name does not include `event_id`: it is
# `${var.name_prefix}-PreQueue`, so changing `event_id` and running `make apply`
# does not recreate the table, which is the trigger that leaves foreign rows in
# the shared PreQueue table the seal-time demotion scan reads.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }
}
mock_provider "archive" {}
mock_provider "random" {}

variables {
  name_prefix                   = "test"
  env                           = "test"
  event_id                      = "A"
  assign_position_artifact_path = "tests/fixtures/bootstrap.zip"
  seal_event_artifact_path      = "tests/fixtures/bootstrap.zip"
  read_artifact_path            = "tests/fixtures/bootstrap.zip"
  admin_artifact_path           = "tests/fixtures/bootstrap.zip"
  controller_artifact_path      = "tests/fixtures/bootstrap.zip"
  generate_token_artifact_path  = "tests/fixtures/bootstrap.zip"
}

run "prequeue_table_name_contains_name_prefix_not_event_id" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.prequeue.name == "test-PreQueue"
    error_message = "PreQueue table name must be ${var.name_prefix}-PreQueue, not scoped by event_id; event_id changes must not recreate the shared table."
  }
}

run "changing_event_id_does_not_change_prequeue_table_name" {
  command = plan

  variables {
    event_id = "B"
  }

  assert {
    condition     = aws_dynamodb_table.prequeue.name == "test-PreQueue"
    error_message = "Changing event_id must not change the PreQueue table name; the table is shared across events and must not be recreated on event_id changes."
  }
}
