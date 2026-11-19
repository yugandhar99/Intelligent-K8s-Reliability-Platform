output "namespace" {
  description = "Namespace to pass to Helm. Helm must not create this namespace."
  value       = kubernetes_namespace_v1.lab.metadata[0].name
}

output "observer_service_account" {
  description = "Identity with read-only access to the Terraform metadata ConfigMap."
  value       = kubernetes_service_account_v1.observer.metadata[0].name
}
