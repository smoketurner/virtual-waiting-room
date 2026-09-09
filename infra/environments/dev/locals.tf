locals {
  common_tags = {
    Project     = "virtual-waiting-room"
    Environment = var.env
    ManagedBy   = "terraform"
  }
}
