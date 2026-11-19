terraform {
  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
  }
}

resource "kubernetes_namespace_v1" "lab" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/managed-by"       = "terraform"
      "pod-security.kubernetes.io/enforce" = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
      "pod-security.kubernetes.io/warn"    = "restricted"
    }
  }
}

resource "kubernetes_config_map_v1" "metadata" {
  metadata {
    name      = "terraform-lab-metadata"
    namespace = kubernetes_namespace_v1.lab.metadata[0].name
  }

  data = {
    environment = var.environment
    owner       = "terraform"
    execution   = "simulation"
  }
}

resource "kubernetes_service_account_v1" "observer" {
  metadata {
    name      = "terraform-lab-observer"
    namespace = kubernetes_namespace_v1.lab.metadata[0].name
  }

  automount_service_account_token = false
}

resource "kubernetes_role_v1" "observer" {
  metadata {
    name      = "terraform-lab-observer"
    namespace = kubernetes_namespace_v1.lab.metadata[0].name
  }

  rule {
    api_groups     = [""]
    resources      = ["configmaps"]
    resource_names = [kubernetes_config_map_v1.metadata.metadata[0].name]
    verbs          = ["get"]
  }
}

resource "kubernetes_role_binding_v1" "observer" {
  metadata {
    name      = "terraform-lab-observer"
    namespace = kubernetes_namespace_v1.lab.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.observer.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.observer.metadata[0].name
    namespace = kubernetes_namespace_v1.lab.metadata[0].name
  }
}
