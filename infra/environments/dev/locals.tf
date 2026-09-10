locals {
  common_tags = {
    Project     = "virtual-waiting-room"
    Environment = var.env
    ManagedBy   = "terraform"
  }

  # Where the authorizer sends an un-admitted visitor. The CloudFront
  # distribution created here is the waiting room, so derive the URL rather than
  # configuring it twice and letting the two drift apart.
  waiting_room_url = "https://${module.edge[0].distribution_domain_name}/"
}
