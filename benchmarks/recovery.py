"""Measure durable job and recovery storage costs on fresh synthetic databases."""

from pathlib import Path
from statistics import median, quantiles
from tempfile import TemporaryDirectory
from time import perf_counter

from app.batch_models import BatchPolicy
from app.batch_service import BatchService
from app.disaster_recovery import copy_database
from app.jobs import JobService
from app.models import PolicySettings
from app.policy import MaintenancePolicy
from app.service import MaintenanceService
from app.store import Store


def measure(root: Path) -> dict[str, float]:
    store = Store(root / "cluster.sqlite3")
    store.initialize()
    policy = MaintenancePolicy(PolicySettings(
        protected_namespaces={"kube-system", "platform-security"},
        termination_timeout_seconds=30, readiness_timeout_seconds=60,
    ))
    single = MaintenanceService(store, policy)
    batch = BatchService(store, policy, BatchPolicy())
    batch.initialize()
    jobs = JobService(single, batch)
    jobs.initialize()
    plan = batch.create_plan(["cedar", "maple"])
    start = perf_counter()
    job = jobs.submit("batch", plan.id)
    for _ in range(2):
        lease = jobs.claim("initial")
        assert lease and jobs.run(lease)
    first_wave = perf_counter()
    assert jobs.claim("initial")
    jobs.release("initial")
    restart = perf_counter()
    replacement = JobService(single, batch)
    lease = replacement.claim("replacement")
    assert lease and replacement.run(lease)
    completed = perf_counter()
    assert replacement.status(job.id).checkpoint == 2
    backup = root / "backup.sqlite3"
    backup_start = perf_counter()
    copy_database(store.path, backup)
    saved = perf_counter()
    copy_database(backup, root / "restored.sqlite3", restore=True)
    restored = perf_counter()
    return {
        "enqueue through first wave": first_wave - start,
        "reconcile and finish second wave": completed - restart,
        "verified backup": saved - backup_start,
        "verified restore": restored - saved,
    }


def main() -> None:
    with TemporaryDirectory(prefix="maintenance-benchmark-") as directory:
        results = []
        for index in range(30):
            root = Path(directory) / str(index)
            root.mkdir()
            results.append(measure(root))
    print("30 fresh databases, milliseconds")
    for name in results[0]:
        times = [sample[name] * 1000 for sample in results]
        print(f"{name}: median {median(times):.2f}, p95 {quantiles(times, n=100)[94]:.2f}")


if __name__ == "__main__":
    main()
