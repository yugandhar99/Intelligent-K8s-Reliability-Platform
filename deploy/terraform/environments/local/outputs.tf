output "namespace" {
  description = "Namespace for a separate Helm release."
  value       = module.lab.namespace
}

output "observer_service_account" {
  description = "Infrastructure metadata reader identity."
  value       = module.lab.observer_service_account
}
