"""Seeded synthetic inputs for scheduler regression tests and benchmarks."""

from random import Random
from typing import Literal

from .models import Cluster, DisruptionBudget, Node, Pod, Resources, Workload


ScenarioKind = Literal["mixed", "placement_pressure", "backtracking_pressure"]


def random_cluster(seed: int, *, kind: ScenarioKind = "mixed") -> Cluster:
    rng = Random(seed)
    if kind == "placement_pressure":
        # Generate complementary CPU/memory shapes. CPU-only best fit consumes
        # the memory-rich destination that the second pod needs.
        cpu = rng.randrange(8, 16) * 75
        memory = rng.randrange(8, 16) * 96
        capacities = {
            "target-0": (cpu * 3, memory * 3),
            "host-0": (cpu + 10, memory * 2),
            "host-1": (cpu + 80, memory),
            "anchor": (1, 1),
        }
        items = [
            ("target-0", cpu, memory),
            ("target-0", rng.randrange(1, cpu), memory * 2),
        ]
    elif kind == "backtracking_pressure":
        cpu = rng.randint(10, 90)
        memory = rng.randint(30, 130)
        # Each pod initially fits either host. The 6/5/3/2/2/2 shapes need
        # a different second or third placement from greedy best fit.
        capacities = {
            "target-0": (20 * cpu, 20 * memory),
            "host-0": (10 * cpu, 10 * memory),
            "host-1": (10 * cpu, 10 * memory),
            "anchor": (1, 1),
        }
        items = [("target-0", size * cpu, size * memory) for size in (6, 5, 3, 2, 2, 2)]
    else:
        capacities = {
            "target-0": (2400, 3000), "target-1": (2400, 3000),
            "host-0": (rng.randint(850, 2100), rng.randint(900, 2500)),
            "host-1": (rng.randint(850, 2100), rng.randint(900, 2500)),
            "anchor": (1, 1),
        }
        items = [
            (f"target-{rng.randrange(2)}", rng.randint(220, 550), rng.randint(275, 625))
            for _ in range(rng.randint(3, 4))
        ]
    nodes = {
        name: Node(
            name=name, allocatable=Resources(cpu_millis=cpu, memory_mib=memory),
            health_score=rng.randint(65, 100),
            labels={"zone": f"zone-{index % 2}"},
        )
        for index, (name, (cpu, memory)) in enumerate(capacities.items())
    }
    workloads = {}
    pods = {}
    budgets = []
    for index, (node, cpu, memory) in enumerate(items):
        name = f"service-{index}"
        key = f"generated/{name}"
        workloads[key] = Workload(namespace="generated", name=name, min_available=1)
        primary = Pod(
            name=f"{name}-primary", workload=key, node=node,
            requests=Resources(cpu_millis=cpu, memory_mib=memory),
            grace_seconds=rng.randint(1, 8), startup_seconds=rng.randint(2, 14),
        )
        replica = primary.model_copy(update={
            "name": f"{name}-replica", "node": "anchor",
            "requests": Resources(cpu_millis=0, memory_mib=0),
        })
        pods[primary.name] = primary
        pods[replica.name] = replica
        budgets.append(DisruptionBudget(
            name=f"{name}-budget", namespace="generated", workloads={name}, min_available=1,
        ))
    return Cluster(nodes=nodes, workloads=workloads, pods=pods, budgets=budgets)
