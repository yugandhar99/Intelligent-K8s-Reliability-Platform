variable "kubeconfig_path" {
  type        = string
  description = "Absolute path or path relative to this Terraform root for the dedicated Kind admin kubeconfig."
}

variable "identity_path" {
  type        = string
  description = "Absolute path or path relative to this Terraform root for the bootstrap Kind identity pin."
}

variable "namespace" {
  type        = string
  description = "Namespace to create in the dedicated Kind cluster."
  default     = "stage7-tf-validation"
}

variable "environment" {
  type        = string
  description = "Configuration label for the isolated environment."
  default     = "testing"
}
