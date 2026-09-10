locals {
  # `make build` writes each crate's zip to a fixed path under .artifacts, so
  # these are derived rather than configured: the build output location is not a
  # decision anyone makes, and a variable for it is one more thing that can be
  # set to the wrong value or, as happened, quietly not passed through at all.
  artifact = {
    for crate in [
      "assign_position",
      "seal_event",
      "read",
      "admin",
      "controller",
      "generate_token",
      "authorizer",
    ] : crate => "${path.module}/../../../.artifacts/${crate}/bootstrap/bootstrap.zip"
  }

  common_tags = {
    Project     = "virtual-waiting-room"
    Environment = var.env
    ManagedBy   = "terraform"
  }

  # Where the authorizer sends an un-admitted visitor. The CloudFront
  # distribution created here is the waiting room, so derive the URL rather than
  # configuring it twice and letting the two drift apart.
  waiting_room_url = "https://${module.edge.distribution_domain_name}/"
}
