terraform {
  required_version = ">= 1.11.0, < 2.0.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.38.0"
    }
  }
}

locals {
  expected_context = "kind-k8s-reliability-lab"
  kubeconfig       = yamldecode(file(var.kubeconfig_path))
  pin              = jsondecode(file(var.identity_path))
  context          = one(local.kubeconfig.contexts)
  cluster          = one(local.kubeconfig.clusters)
  identity         = one(local.kubeconfig.users)
  endpoint         = local.cluster.cluster.server
  ca               = base64decode(local.cluster.cluster["certificate-authority-data"])
  local_identity = (
    can(regex("^https://127[.]0[.]0[.]1:[0-9]{1,5}$", local.endpoint)) &&
    local.kubeconfig["current-context"] == local.expected_context &&
    local.context.name == local.expected_context &&
    local.context.context.cluster == local.expected_context &&
    local.cluster.name == local.expected_context &&
    local.context.context.user == local.identity.name &&
    toset(keys(local.cluster.cluster)) == toset(["server", "certificate-authority-data"]) &&
    toset(keys(local.identity.user)) == toset(["client-key-data", "client-certificate-data"]) &&
    local.pin.server == local.endpoint &&
    sha256(local.ca) == local.pin.ca_sha256
  )
}

provider "kubernetes" {
  # Invalid configurations only initialize a fail-closed loopback provider.
  insecure               = false
  host                   = local.local_identity ? local.endpoint : "https://127.0.0.1:1"
  cluster_ca_certificate = local.local_identity ? local.ca : null
  client_certificate     = local.local_identity ? base64decode(local.identity.user["client-certificate-data"]) : null
  client_key             = local.local_identity ? base64decode(local.identity.user["client-key-data"]) : null
}

resource "terraform_data" "local_identity" {
  lifecycle {
    precondition {
      condition     = local.local_identity
      error_message = "Credentials must match the pinned loopback k8s-reliability-lab cluster."
    }
  }
}

data "kubernetes_namespace_v1" "kube_system" {
  metadata {
    name = "kube-system"
  }

  lifecycle {
    precondition {
      condition     = local.local_identity
      error_message = "The local cluster identity must be verified before the provider reads namespaces."
    }

    postcondition {
      condition     = self.metadata[0].uid == local.pin.namespace_uid
      error_message = "The kube-system namespace UID does not match the dedicated Kind identity."
    }
  }
}

module "lab" {
  source      = "../../modules/lab-namespace"
  namespace   = var.namespace
  environment = var.environment

  depends_on = [terraform_data.local_identity, data.kubernetes_namespace_v1.kube_system]
}
