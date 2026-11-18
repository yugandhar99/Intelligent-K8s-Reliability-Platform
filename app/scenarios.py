from .models import Cluster, DisruptionBudget, Node, Pod, Resources, Workload


def synthetic_cluster() -> Cluster:
    """Return a fresh portfolio scenario with no external data dependencies."""
    nodes = {
        name: Node(name=name, allocatable=Resources(cpu_millis=2200, memory_mib=3072))
        for name in ("cedar", "maple", "willow")
    }
    workloads = {
        "shop/catalog": Workload(namespace="shop", name="catalog", min_available=1),
        "shop/checkout": Workload(namespace="shop", name="checkout", min_available=1),
        "kube-system/router": Workload(
            namespace="kube-system", name="router", min_available=1
        ),
    }
    pods = {}
    for name, workload, node, cpu, memory in (
        ("catalog-1", "shop/catalog", "cedar", 650, 640),
        ("catalog-2", "shop/catalog", "maple", 650, 640),
        ("checkout-1", "shop/checkout", "cedar", 850, 1024),
        ("checkout-2", "shop/checkout", "willow", 850, 1024),
        ("router-1", "kube-system/router", "willow", 250, 256),
    ):
        pods[name] = Pod(
            name=name,
            workload=workload,
            node=node,
            requests=Resources(cpu_millis=cpu, memory_mib=memory),
        )
    return Cluster(
        nodes=nodes,
        workloads=workloads,
        pods=pods,
        budgets=[
            DisruptionBudget(
                name="catalog-availability",
                namespace="shop",
                workloads={"catalog"},
                min_available=1,
            ),
            DisruptionBudget(
                name="checkout-availability",
                namespace="shop",
                workloads={"checkout"},
                min_available=1,
            ),
        ],
    )
