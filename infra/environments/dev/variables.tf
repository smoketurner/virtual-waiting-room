variable "region" {
  description = "AWS region for the regional core resources (DynamoDB, SQS, Lambda, REST API)."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name in this deployment. Isolates a second deployment in the same account."
  type        = string
  default     = "vwr-dev"
}

variable "env" {
  description = "Deployment environment. Drives the REST API stage name and the Environment tag."
  type        = string
  default     = "dev"
}

variable "warm_throughput_write_units" {
  description = "Warm write units/s to pre-provision per table ahead of an event. 0 = no pre-warm; >= 4000 if set."
  type        = number
  default     = 0
}

variable "warm_throughput_read_units" {
  description = "Warm read units/s to pre-provision per table ahead of an event. 0 = no pre-warm; >= 12000 if set."
  type        = number
  default     = 0
}

variable "client_origin_domain_name" {
  description = "Domain name of the client's protected origin (the CloudFront default behaviour). Empty means the edge/CloudFront module is not created - it cannot exist without a real origin."
  type        = string
  default     = ""
}
