locals {
  # A single input-validation guard so a misconfigured VPC seam fails at plan
  # time rather than producing a half-built private-origin topology.
  vpc_config_valid = !var.enable_vpc || length(var.vpc_origin_subnet_ids) > 0
}
