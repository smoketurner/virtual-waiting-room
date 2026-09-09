# Ambient account/region context for the environment root.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
