# Asserts the gate config seeded into the KeyValueStore is exactly what the
# CloudFront Function reads. The variable takes the dashboard's own
# one-rule-per-line grammar, so this is the one place the HCL encoding is
# checked against the compact wire tuple `wr_common::rules` round-trips:
# ["p","/checkout"], ["c","name"], ["u","substring"], ["h","name","value"].
#
# A wrong shape here is silent: the gate reads cfg.r, matches nothing, and
# passes every request through — which looks exactly like deliberate dormancy.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}
mock_provider "archive" {}
mock_provider "random" {}

variables {
  name_prefix                   = "test"
  env                           = "test"
  event_id                      = "an-event-id"
  assign_position_artifact_path = "tests/fixtures/bootstrap.zip"
  open_event_artifact_path      = "tests/fixtures/bootstrap.zip"
  read_artifact_path            = "tests/fixtures/bootstrap.zip"
  admin_artifact_path           = "tests/fixtures/bootstrap.zip"
  controller_artifact_path      = "tests/fixtures/bootstrap.zip"
  generate_token_artifact_path  = "tests/fixtures/bootstrap.zip"

  # The admin Lambda's preconditions refuse a stack whose control plane cannot
  # start, so a test harness has to describe a deployable configuration.
  oidc_client_id      = "test-client"
  oidc_redirect_uri   = "https://example.invalid/admin/callback"
  oidc_allowed_emails = "operator@example.invalid"
}

run "an_empty_ruleset_seeds_dormant_config" {
  command = plan

  variables {
    gate_rules = ""
  }

  assert {
    condition     = jsondecode(aws_cloudfrontkeyvaluestore_key.config.value).r == []
    error_message = "an empty gate_rules must seed 'r': [], the dormancy the gate reads as 'no rule matches'"
  }

  assert {
    condition     = jsondecode(aws_cloudfrontkeyvaluestore_key.config.value).v == 1
    error_message = "the gate throws on an unrecognised config version, so the seed must carry v = 1"
  }
}

run "every_rule_kind_encodes_to_its_wire_tuple" {
  command = plan

  variables {
    gate_rules = <<-RULES
      # a comment, and the blank line below, are both ignored

      p /checkout
      c loyalty_member
      u HeadlessChrome
      h x-internal-monitor true
    RULES
  }

  assert {
    condition = jsondecode(aws_cloudfrontkeyvaluestore_key.config.value).r == [
      ["p", "/checkout"],
      ["c", "loyalty_member"],
      ["u", "HeadlessChrome"],
      ["h", "x-internal-monitor", "true"],
    ]
    error_message = "the seeded ruleset must match the compact wire tuple wr_common::rules produces, in order"
  }
}

run "a_header_value_may_contain_spaces" {
  command = plan

  variables {
    gate_rules = "h x-reason checkout is open"
  }

  # The name is the first token and the value is everything after it: splitting
  # on every space would silently truncate the value to "checkout".
  assert {
    condition = jsondecode(aws_cloudfrontkeyvaluestore_key.config.value).r == [
      ["h", "x-reason", "checkout is open"],
    ]
    error_message = "a header value must keep its spaces; only the name is a single token"
  }
}

run "a_path_prefix_may_contain_spaces_too" {
  command = plan

  variables {
    gate_rules = "p /black friday"
  }

  assert {
    condition = jsondecode(aws_cloudfrontkeyvaluestore_key.config.value).r == [
      ["p", "/black friday"],
    ]
    error_message = "a path prefix is everything after the tag, spaces included"
  }
}
