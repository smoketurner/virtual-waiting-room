variable "name_prefix" {
  description = "Prefix applied to every resource name in this deployment."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource in the module."
  type        = map(string)
  default     = {}
}
