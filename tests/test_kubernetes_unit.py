from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from kubernetes.client.exceptions import ApiException

from app.kube_adapter import (
    LAB, NAMESPACE, OWNER, EvictionBlocked, KubernetesAdapter, KubeUnavailable, LabSafetyError,
)
from app.kube_planning import matches, requests


def adapter():
    instance = KubernetesAdapter.__new__(KubernetesAdapter)
    instance.pin = {"namespace_uid": "pinned"}
    instance.core = Mock()
    instance.core.read_namespace.return_value = SimpleNamespace(metadata=SimpleNamespace(uid="pinned"))
    return instance


def node(owner=None, cordoned=False):
    return {
        "metadata": {"uid": "node-id", "name": f"{LAB}-worker", "resourceVersion": "12",
                     "annotations": {OWNER: owner} if owner else {}},
        "spec": {"unschedulable": cordoned},
    }


def pod():
    return {
        "metadata": {"uid": "pod-id", "name": "web", "namespace": NAMESPACE, "resourceVersion": "14"},
        "spec": {"nodeName": f"{LAB}-worker"},
    }


def test_wrong_identity_refuses_mutation():
    kube = adapter()
    kube.core.read_namespace.return_value.metadata.uid = "different"
    with pytest.raises(LabSafetyError, match="UID"):
        kube.schedule(node(), "node-id", "operation", cordon=True)
    kube.core.patch_node.assert_not_called()


def test_patch_preserves_original_and_requires_ownership_and_version():
    kube = adapter()
    with pytest.raises(LabSafetyError):
        kube.schedule(node(cordoned=True), "node-id", "operation", cordon=True)
    with pytest.raises(LabSafetyError):
        kube.schedule(node(owner="someone-else", cordoned=True), "node-id", "operation", cordon=False)
    kube.core.patch_node.assert_not_called()
    kube.schedule(node(), "node-id", "operation", cordon=True)
    assert kube.core.patch_node.call_args.args[1] == {
        "metadata": {"resourceVersion": "12", "annotations": {OWNER: "operation"}},
        "spec": {"unschedulable": True},
    }
    kube.schedule(node(owner="operation", cordoned=True), "node-id", "operation", cordon=False)
    assert kube.core.patch_node.call_args.args[1]["metadata"]["annotations"] == {OWNER: None}
    assert kube.core.read_namespace.call_count == 2


def test_version_conflict_requires_reconciliation_without_blind_retry():
    kube = adapter()
    kube.core.patch_node.side_effect = ApiException(status=409)
    with pytest.raises(KubeUnavailable):
        kube.schedule(node(), "node-id", "operation", cordon=True)
    kube.core.patch_node.assert_called_once()


def test_eviction_uses_pdb_api_uid_and_grace_period_defaults():
    kube = adapter()
    kube.core.create_namespaced_pod_eviction.side_effect = ApiException(status=429)
    with pytest.raises(EvictionBlocked, match="429"):
        kube.evict(pod())
    name, namespace, request = kube.core.create_namespaced_pod_eviction.call_args.args
    assert (name, namespace, request.api_version, request.kind) == ("web", NAMESPACE, "policy/v1", "Eviction")
    assert request.delete_options.preconditions.uid == "pod-id"
    assert request.delete_options.preconditions.resource_version == "14"
    assert request.delete_options.grace_period_seconds is None
    kube.core.delete_namespaced_pod.assert_not_called()


def test_reads_retry_and_mutation_timeouts_reconcile():
    kube = adapter()
    kube.core.read_namespace.side_effect = [ApiException(status=503), kube.core.read_namespace.return_value]
    kube.core.patch_node.side_effect = TimeoutError
    with pytest.raises(KubeUnavailable, match="uncertain"):
        kube.schedule(node(), "node-id", "operation", cordon=True)
    assert kube.core.read_namespace.call_count == 2
    kube.core.patch_node.assert_called_once()


def test_capacity_counts_restartable_init_containers_and_overhead():
    def container(cpu, memory):
        return {"resources": {"requests": {"cpu": cpu, "memory": memory}}}

    demand = requests({"spec": {
        "containers": [container("500m", "32Mi"), container("200m", "16Mi")],
        "initContainers": [{**container("100m", "32Mi"), "restartPolicy": "Always"}, container("1", "16Mi")],
        "overhead": {"cpu": "100m", "memory": "16Mi"},
    }})
    assert str(demand[0]) == "1.200"
    assert demand[1:] == (96 * 1024**2, 1)


def test_empty_pdb_selector_matches_all_but_missing_selector_matches_none():
    assert matches({}, {"app": "web"})
    assert not matches(None, {"app": "web"})
    assert matches({"matchExpressions": [{"key": "app", "operator": "In", "values": ["web"]}]}, {"app": "web"})


@pytest.mark.parametrize(("namespace", "managed", "volume", "reason"), [
    ("kube-system", True, None, "protected namespace"),
    (NAMESPACE, False, None, "not managed"),
    (NAMESPACE, True, {"name": "local", "emptyDir": {}}, "storage"),
    (NAMESPACE, True, {"name": "claim", "persistentVolumeClaim": {"claimName": "data"}}, "storage"),
])
def test_drain_protects_system_unmanaged_and_storage_pods(namespace, managed, volume, reason):
    from app.kube_planning import Inventory

    target = pod()
    target["metadata"]["namespace"] = namespace
    target["spec"]["containers"] = [{"name": "web"}]
    if managed:
        target["metadata"]["ownerReferences"] = [{"kind": "ReplicaSet", "uid": "rs", "controller": True}]
    if volume:
        target["spec"]["volumes"] = [volume]
    snapshot = {
        "pods": [target], "nodes": [], "pdbs": [],
        "replica_sets": [{"metadata": {"uid": "rs", "ownerReferences": [
            {"kind": "Deployment", "uid": "deployment", "controller": True},
        ]}}],
        "deployments": [{"metadata": {"uid": "deployment", "namespace": namespace}}],
    }
    assert reason in Inventory(snapshot).eligible(target)[2]


def test_daemonset_residents_are_excluded_from_evictable_work():
    from app.kube_planning import Inventory

    daemon = pod()
    daemon["metadata"]["ownerReferences"] = [{"kind": "DaemonSet", "controller": True}]
    inventory = Inventory({"pods": [daemon], "nodes": [], "pdbs": [], "replica_sets": [], "deployments": []})
    assert not inventory.residents(f"{LAB}-worker")
