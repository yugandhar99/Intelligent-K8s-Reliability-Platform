"""Conservative scheduling forecasts and workload protection for the local lab."""

from decimal import Decimal

from kubernetes.utils.quantity import parse_quantity

from .kube_adapter import NAMESPACE, OWNER, WORKERS

RESOURCES = ("cpu", "memory", "pods")
SAFE_VOLUMES = {"configMap", "secret", "projected", "downwardAPI"}


def controller(resource: dict) -> dict:
    return next((ref for ref in resource["metadata"].get("ownerReferences", []) if ref.get("controller")), {})


def ready(resource: dict) -> bool:
    return not resource["metadata"].get("deletionTimestamp") and any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in resource.get("status", {}).get("conditions", [])
    )


def worker(node: dict) -> bool:
    meta = node["metadata"]
    return meta["name"] in WORKERS and not {
        "node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master",
    } & meta.get("labels", {}).keys()


def quantities(values: dict) -> tuple[Decimal, ...]:
    return tuple(parse_quantity(values.get(key, "0")) for key in RESOURCES)


def requests(pod: dict) -> tuple[Decimal, ...]:
    spec = pod["spec"]
    if spec.get("resources"):
        # Pod-level requests supersede container totals where specified.
        pod_level = spec["resources"].get("requests", {})
    else:
        pod_level = {}

    def container_values(container: dict) -> tuple[Decimal, ...]:
        return quantities(container.get("resources", {}).get("requests", {}))

    regular = list(map(container_values, spec["containers"]))
    sidecars = [Decimal(0)] * len(RESOURCES)
    peak = sidecars.copy()
    for init in spec.get("initContainers", []):
        combined = [prior + value for prior, value in zip(sidecars, container_values(init))]
        peak = list(map(max, zip(peak, combined)))
        if init.get("restartPolicy") == "Always":
            sidecars = combined
    overhead = quantities(spec.get("overhead", {}))
    return tuple(
        Decimal(1) if key == "pods" else (
            parse_quantity(pod_level[key]) if key in pod_level
            else max(peak[i], sidecars[i] + sum(values[i] for values in regular))
        ) + overhead[i]
        for i, key in enumerate(RESOURCES)
    )


def matches(selector: dict | None, labels: dict) -> bool:
    if selector is None or any(labels.get(key) != value for key, value in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression["key"], expression["operator"]
        values = expression.get("values", [])
        if not {
            "In": key in labels and labels[key] in values,
            "NotIn": key not in labels or labels[key] not in values,
            "Exists": key in labels,
            "DoesNotExist": key not in labels,
        }.get(operator, False):
            return False
    return True


class Inventory:
    def __init__(self, snapshot: dict) -> None:
        self.pods = snapshot["pods"]
        self.nodes = {node["metadata"]["name"]: node for node in snapshot["nodes"]}
        self.replica_sets = {item["metadata"]["uid"]: item for item in snapshot["replica_sets"]}
        self.deployments = {item["metadata"]["uid"]: item for item in snapshot["deployments"]}
        self.pdbs = snapshot["pdbs"]

    def deployment(self, pod: dict) -> dict | None:
        ref = controller(pod)
        replica_set = self.replica_sets.get(ref.get("uid")) if ref.get("kind") == "ReplicaSet" else None
        ref = controller(replica_set) if replica_set else {}
        return self.deployments.get(ref.get("uid")) if ref.get("kind") == "Deployment" else None

    def ready_pods(self, uid: str, *, exclude_node: str | None = None) -> set[str]:
        result = set()
        for pod in self.pods:
            location = pod["spec"].get("nodeName")
            if exclude_node is not None and (location == exclude_node or location not in self.nodes
                                             or not worker(self.nodes[location]) or not ready(self.nodes[location])):
                continue
            if ready(pod):
                deployment = self.deployment(pod)
                if deployment and deployment["metadata"]["uid"] == uid:
                    result.add(pod["metadata"]["uid"])
        return result

    def available(self, uid: str, replicas: int, generation: int) -> bool:
        deployment = self.deployments.get(uid)
        if not deployment or deployment["metadata"]["generation"] != generation:
            return False
        status = deployment.get("status", {})
        return (
            status.get("observedGeneration", 0) >= generation
            and status.get("availableReplicas", 0) >= replicas
            and len(self.ready_pods(uid)) >= replicas
        )

    def eligible(self, pod: dict) -> tuple[dict | None, list[dict], str | None]:
        meta, spec = pod["metadata"], pod["spec"]
        deployment = self.deployment(pod)
        budgets = [
            pdb for pdb in self.pdbs if pdb["metadata"]["namespace"] == meta["namespace"]
            and matches(pdb["spec"].get("selector"), meta.get("labels", {}))
        ]
        if meta["namespace"] != NAMESPACE or "kubernetes.io/config.mirror" in meta.get("annotations", {}):
            return deployment, budgets, "protected namespace or static pod"
        if not deployment or deployment["metadata"]["namespace"] != NAMESPACE:
            return deployment, budgets, "pod is not managed by a lab deployment"
        if any(set(volume) - {"name"} - SAFE_VOLUMES for volume in spec.get("volumes", [])):
            return deployment, budgets, "storage cannot be safely relocated"
        if (
            any(spec.get(key) for key in ("nodeSelector", "affinity", "topologySpreadConstraints",
                                          "resourceClaims", "schedulingGates", "priorityClassName",
                                          "runtimeClassName", "overhead", "hostNetwork"))
            or deployment["spec"]["template"]["spec"].get("nodeName")
            or spec.get("schedulerName", "default-scheduler") != "default-scheduler"
            or any(port.get("hostPort") for container in spec["containers"] + spec.get("initContainers", [])
                   for port in container.get("ports", []))
            or any(set(container.get("resources", {}).get("requests", {})) - {"cpu", "memory"}
                   for container in spec["containers"] + spec.get("initContainers", []))
        ):
            return deployment, budgets, "scheduling constraints exceed this forecast"
        if len(budgets) != 1 or budgets[0]["metadata"].get("deletionTimestamp"):
            return deployment, budgets, "exactly one active disruption budget is required"
        if not ready(pod):
            return deployment, budgets, "pod is not Ready"
        return deployment, budgets, None

    def residents(self, target: str) -> list[dict]:
        return [
            pod for pod in self.pods if pod["spec"].get("nodeName") == target
            and controller(pod).get("kind") != "DaemonSet"
            and pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
        ]

    def forecast(self, target: str, pods: list[dict]) -> tuple[dict, bool]:
        free = {}
        capacity = {}
        for name, node in sorted(self.nodes.items()):
            spec = node.get("spec", {})
            if (
                name == target or not worker(node) or not ready(node) or spec.get("unschedulable")
                or spec.get("taints") or OWNER in node["metadata"].get("annotations", {})
            ):
                continue
            total = quantities(node["status"]["allocatable"])
            used = [Decimal(0)] * len(RESOURCES)
            for pod in self.pods:
                if pod["spec"].get("nodeName") == name and pod.get("status", {}).get("phase") not in {"Failed", "Succeeded"}:
                    used = [a + b for a, b in zip(used, requests(pod))]
            free[name] = [max(Decimal(0), a - b) for a, b in zip(total, used)]
            capacity[name] = {
                label: dict(zip(RESOURCES, map(str, values)))
                for label, values in (("allocatable", total), ("requested", used), ("free", free[name]))
            }
        fits = bool(free)
        for demand in sorted(map(requests, pods), reverse=True):
            destination = next((name for name in free if all(a >= b for a, b in zip(free[name], demand))), None)
            if destination is None:
                fits = False
                break
            free[destination] = [a - b for a, b in zip(free[destination], demand)]
        for name, values in free.items():
            capacity[name]["forecast_free"] = dict(zip(RESOURCES, map(str, values)))
        return capacity, fits

    def plan(self, target: str) -> dict:
        node = self.nodes.get(target)
        plan = {"node": target, "node_uid": None, "pods": [], "capacity": {}, "problems": []}
        problems = plan["problems"]
        if not node or not worker(node):
            problems.append("Target must be a dedicated Kind worker")
            return plan
        plan["node_uid"] = node["metadata"]["uid"]
        if not ready(node) or node.get("spec", {}).get("unschedulable") or OWNER in node["metadata"].get("annotations", {}):
            problems.append("Target is not Ready and schedulable without a maintenance owner")
        residents = self.residents(target)
        for pod in sorted(residents, key=lambda item: item["metadata"]["name"]):
            meta = pod["metadata"]
            deployment, budgets, problem = self.eligible(pod)
            if problem:
                problems.append(f"{meta['namespace']}/{meta['name']}: {problem}")
                continue
            dep = deployment["metadata"]
            replicas = deployment["spec"].get("replicas", 1)
            if not self.available(dep["uid"], replicas, dep["generation"]):
                problems.append(f"{dep['name']}: application is not fully available")
            plan["pods"].append({
                "name": meta["name"], "uid": meta["uid"], "deployment": dep["uid"],
                "generation": dep["generation"], "replicas": replicas,
                "pdb": budgets[0]["metadata"]["name"], "pdb_uid": budgets[0]["metadata"]["uid"],
                "pdb_generation": budgets[0]["metadata"]["generation"],
                "disruptions_allowed": budgets[0].get("status", {}).get("disruptionsAllowed", 0),
            })
        plan["capacity"], fits = self.forecast(target, residents)
        if not fits:
            problems.append("Insufficient schedulable CPU, memory or pod capacity")
        return plan


def scope(plan: dict) -> tuple:
    return plan["node_uid"], [
        tuple(pod[key] for key in ("uid", "deployment", "generation", "replicas", "pdb_uid", "pdb_generation"))
        for pod in plan["pods"]
    ]
