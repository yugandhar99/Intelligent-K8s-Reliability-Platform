variable "namespace" {
  type        = string
  description = "A new, isolated lab namespace owned by this Terraform state."

  validation {
    condition     = can(regex("^stage7-tf-[a-z0-9]([a-z0-9-]{0,40}[a-z0-9])?$", var.namespace))
    error_message = "Use a dedicated stage7-tf-* namespace."
  }
}

variable "environment" {
  type        = string
  description = "Name recorded in the namespace metadata ConfigMap."

  validation {
    condition     = contains(["development", "testing", "production-style"], var.environment)
    error_message = "Choose development, testing, or production-style."
  }
}
