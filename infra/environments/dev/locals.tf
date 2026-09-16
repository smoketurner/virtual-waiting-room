locals {
  # `make build` writes each crate's zip to a fixed path under .artifacts, so
  # these are derived rather than configured: the build output location is not a
  # decision anyone makes, and a variable for it is one more thing that can be
  # set to the wrong value or, as happened, quietly not passed through at all.
  artifact = {
    for crate in [
      "assign_position",
      "open_event",
      "read",
      "admin",
      "controller",
      "generate_token",
    ] : crate => "${path.module}/../../../.artifacts/${crate}/bootstrap/bootstrap.zip"
  }

  common_tags = {
    Project     = "virtual-waiting-room"
    Environment = var.env
    ManagedBy   = "terraform"
  }

  # The waiting room's own URL. Derived from the distribution created here
  # rather than configured twice and left to drift.
  waiting_room_url = "https://${module.edge.viewer_domain_name}/"
}
