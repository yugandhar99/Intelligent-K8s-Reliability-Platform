"""Run with python -m benchmarks.compare_schedulers."""

from collections import Counter
from datetime import UTC, datetime
import json
from statistics import median
from time import perf_counter_ns

from app.batch_models import BatchPolicy, PolicySnapshot
from app.models import PolicySettings
from app.policy import MaintenancePolicy, SafetyError
from app.random_scenarios import ScenarioKind, random_cluster
from app.scheduler import build_plan
from app.simulator import simulate_maintenance


def compare(cases_per_group: int = 100) -> dict:
    if cases_per_group < 1:
        raise ValueError("cases_per_group must be positive")
    policy = MaintenancePolicy(PolicySettings(
        protected_namespaces=set(), termination_timeout_seconds=30, readiness_timeout_seconds=60,
    ))
    settings = PolicySnapshot(settings=BatchPolicy())
    now = datetime(2026, 1, 1, tzinfo=UTC)
    results = {}
    groups: tuple[ScenarioKind, ...] = ("mixed", "placement_pressure", "backtracking_pressure")
    for group in groups:
        outcomes: Counter[str] = Counter()
        greedy_times = []
        search_times = []
        search_steps = []
        for seed in range(cases_per_group):
            cluster = random_cluster(seed, kind=group)
            start = perf_counter_ns()
            greedy_input = cluster.model_copy(deep=True)
            try:
                simulate_maintenance(greedy_input, "target-0", policy)
            except SafetyError:
                greedy = False
            else:
                greedy = True
            greedy_times.append((perf_counter_ns() - start) / 1_000_000)

            start = perf_counter_ns()
            plan = build_plan(cluster, ["target-0"], policy, settings, now)
            search_times.append((perf_counter_ns() - start) / 1_000_000)
            search_steps.append(plan.search_steps)
            outcomes[f"greedy_{'safe' if greedy else 'blocked'}_search_{'safe' if plan.safe else 'blocked'}"] += 1

        results[group] = {
            "scenarios": cases_per_group,
            "greedy_safe": sum(count for key, count in outcomes.items() if key.startswith("greedy_safe")),
            "search_safe": sum(count for key, count in outcomes.items() if key.endswith("search_safe")),
            "paired_outcomes": dict(sorted(outcomes.items())),
            "greedy_median_ms": round(median(greedy_times), 4),
            "search_median_ms": round(median(search_times), 4),
            "search_max_steps": max(search_steps),
        }
    return results


if __name__ == "__main__":
    print(json.dumps(compare(), indent=2))
