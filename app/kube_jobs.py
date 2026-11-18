"""Live maintenance state machine. Kubernetes is authoritative after interruptions."""

import json
from uuid import uuid4

from .jobs import Job
from .kube_adapter import EvictionBlocked, KubeUnavailable, LabSafetyError, OWNER
from .kube_planning import Inventory, ready, scope, worker
from .models import Problem
from .service import RecordNotFound, logger
from .store import MaintenanceConflict


class LiveMaintenance:
    def __init__(self, jobs, adapter, deadline: float = 120) -> None:
        self.jobs = jobs
        self.adapter = adapter
        self.deadline = deadline

    def initialize(self) -> None:
        with self.jobs.store.transaction(write=True) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS live_plans (
                id TEXT PRIMARY KEY, payload TEXT NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS live_checkpoints (
                job_id TEXT PRIMARY KEY REFERENCES jobs(id), payload TEXT NOT NULL
            )""")

    def plan(self, node: str) -> dict:
        plan = Inventory(self.adapter.discover()).plan(node)
        plan.update(id=uuid4().hex, cluster_uid=self.adapter.pin["namespace_uid"], created=self.jobs.clock())
        with self.jobs.store.transaction(write=True) as db:
            db.execute("INSERT INTO live_plans VALUES (?, ?)", (plan["id"], json.dumps(plan)))
        return plan

    def approve(self, plan_id: str, approver: str) -> Job:
        with self.jobs.store.transaction(write=True) as db:
            row = db.execute("SELECT payload FROM live_plans WHERE id = ?", (plan_id,)).fetchone()
            if not row:
                raise RecordNotFound("Live plan not found")
            plan = json.loads(row[0])
            existing = db.execute("SELECT payload FROM jobs WHERE kind = 'kubernetes' AND plan_id = ?",
                                  (plan_id,)).fetchone()
            if existing:
                return Job.model_validate_json(existing[0])
            if (plan["problems"] or self.jobs.clock() - plan["created"] > 300
                    or plan["cluster_uid"] != self.adapter.pin["namespace_uid"]):
                raise MaintenanceConflict("Approval requires a fresh, eligible plan for the pinned cluster")
            if db.execute("""SELECT 1 FROM jobs WHERE kind = 'kubernetes' AND
                json_extract(payload, '$.state') IN ('queued', 'running', 'retrying')""").fetchone():
                raise MaintenanceConflict("Another live maintenance job is active")
            job = Job(id=uuid4().hex, kind="kubernetes", plan_id=plan_id, nodes=[plan["node"]],
                      total_steps=len(plan["pods"]) + 2)
            db.execute("INSERT INTO jobs(id, kind, plan_id, payload) VALUES (?, ?, ?, ?)",
                       (job.id, job.kind, plan_id, job.model_dump_json()))
            run = {"plan": plan, "phase": "verify", "index": 0, "problem": None,
                   "deadline": self.jobs.clock() + self.deadline}
            db.execute("INSERT INTO live_checkpoints VALUES (?, ?)", (job.id, json.dumps(run)))
            self.audit(db, job, "live.approved", approver=approver, node=plan["node"], plan=plan_id)
            return job

    def audit(self, db, job: Job, event: str, **fields) -> None:
        self.jobs.event(db, job, event, **fields)
        logger.info(event, extra={"fields": {"job": job.id, **fields}})

    def notify(self, db, job: Job, plan: dict, reason: str) -> None:
        try:
            self.adapter.event(plan["node"], plan["node_uid"], reason, f"Maintenance job {job.id}: {reason}")
        except (KubeUnavailable, LabSafetyError) as error:
            self.audit(db, job, "live.event_delivery_failed", reason=reason, error=str(error))

    def step(self, db, job: Job) -> None:
        payload, = db.execute("SELECT payload FROM live_checkpoints WHERE job_id = ?", (job.id,)).fetchone()
        run = json.loads(payload)
        phase = run["phase"]
        try:
            if run["plan"]["cluster_uid"] != self.adapter.pin["namespace_uid"]:
                raise LabSafetyError("The job belongs to a different Kind cluster")
            if phase != "cleanup" and self.jobs.clock() >= run["deadline"]:
                self.fail(run, "operation_deadline", "Deadline reached before drain verification")
            elif phase == "cleanup":
                self.cleanup(db, job, run)
                return
            else:
                self.advance(db, job, run)
            self.jobs.ready(db, job)
            if run["phase"] == "wait" or run.get("pending_disruption"):
                job.next_attempt = self.jobs.clock() + 0.5
        except EvictionBlocked as error:
            self.fail(run, "eviction_blocked", str(error))
            self.audit(db, job, "live.eviction_blocked", pod=run["plan"]["pods"][run["index"]]["name"])
            self.notify(db, job, run["plan"], "EvictionBlocked")
            self.jobs.ready(db, job)
        except KubeUnavailable as error:
            job.retries += 1
            job.state = "retrying"
            job.next_attempt = self.jobs.clock() + min(10, 0.5 * 2 ** min(job.retries, 5))
            self.audit(db, job, "live.reconcile_scheduled", phase=phase, attempt=job.retries, reason=str(error))
        except LabSafetyError as error:
            if phase == "cleanup":
                self.jobs.finish(db, job, "failed", Problem(code="ownership_or_identity_changed", message=str(error)))
                return
            self.fail(run, "live_safety_check", str(error))
            self.audit(db, job, "live.safety_stop", phase=phase, reason=str(error))
            self.jobs.ready(db, job)
        self.jobs.save(db, job)
        db.execute("UPDATE live_checkpoints SET payload = ? WHERE job_id = ?", (json.dumps(run), job.id))

    @staticmethod
    def fail(run: dict, code: str, message: str) -> None:
        run["problem"] = {"code": code, "message": message}
        run["phase"] = "cleanup"

    @staticmethod
    def workloads_available(inventory: Inventory, plan: dict) -> bool:
        return all(inventory.available(item["deployment"], item["replicas"], item["generation"])
                   for item in plan["pods"])

    @staticmethod
    def check_worker(node: dict, plan: dict) -> tuple[str | None, bool]:
        meta = node["metadata"]
        if not worker(node) or meta["uid"] != plan["node_uid"]:
            raise LabSafetyError("Target worker identity or role changed")
        return meta.get("annotations", {}).get(OWNER), node.get("spec", {}).get("unschedulable", False)

    def advance(self, db, job: Job, run: dict) -> None:
        plan, phase = run["plan"], run["phase"]
        target = plan["node"]
        inventory = Inventory(self.adapter.discover())
        if phase == "verify":
            current = inventory.plan(target)
            if current["problems"] or scope(current) != scope(plan):
                raise LabSafetyError("Approved workload scope or safety checks changed before cordoning")
            run["phase"] = "cordon"
            run["cordon_intent"] = True
            self.audit(db, job, "live.cordon_intent", node=target)
            return

        node = inventory.nodes.get(target)
        if node is None:
            raise LabSafetyError("Target worker disappeared")
        owner, unschedulable = self.check_worker(node, plan)
        if phase == "cordon":
            if owner is None and not unschedulable:
                current = inventory.plan(target)
                if current["problems"] or scope(current) != scope(plan):
                    raise LabSafetyError("Approved workload scope or safety checks changed before cordoning")
                self.adapter.schedule(node, plan["node_uid"], job.id, cordon=True)
            elif owner != job.id or not unschedulable:
                raise LabSafetyError("Another actor changed target scheduling ownership")
            run["phase"] = "drain"
            job.checkpoint += 1
            self.audit(db, job, "live.node_cordoned", node=target)
            self.notify(db, job, plan, "NodeCordoned")
            return

        if owner != job.id or not unschedulable or not ready(node):
            raise LabSafetyError("The job no longer has a Ready target with an owned cordon")
        remaining = plan["pods"][run["index"]:]
        residents = {pod["metadata"]["uid"]: pod for pod in inventory.residents(target)}
        if residents.keys() - {item["uid"] for item in remaining}:
            raise LabSafetyError("Unapproved pods appeared on the target")
        if not remaining:
            if not self.workloads_available(inventory, plan):
                raise LabSafetyError("Application availability changed before drain completion")
            self.audit(db, job, "live.maintenance_simulated", reboot=True, os_patch=True)
            self.notify(db, job, plan, "DrainVerified")
            run["phase"] = "cleanup"
            return

        item = remaining[0]
        pod = residents.get(item["uid"])
        dep, replicas, generation = item["deployment"], item["replicas"], item["generation"]
        if phase == "wait" or (phase == "evict" and
                              (pod is None or pod["metadata"].get("deletionTimestamp"))):
            new_ready = inventory.ready_pods(dep, exclude_node=target) - set(run["baseline"])
            if pod is None and new_ready and inventory.available(dep, replicas, generation):
                run.pop("pending_disruption", None)
                run["phase"] = "drain"
                run["index"] += 1
                job.checkpoint += 1
                self.audit(db, job, "live.replacement_ready", pod=item["name"],
                           replacement=sorted(new_ready)[0])
            else:
                run["phase"] = "wait"
            return

        if pod is None:
            raise LabSafetyError("An approved pod disappeared before eviction intent")
        deployment, budgets, problem = inventory.eligible(pod)
        if phase == "evict" and any(
            budget["metadata"]["uid"] == item["pdb_uid"]
            and item["name"] in budget.get("status", {}).get("disruptedPods", {}) for budget in budgets
        ):
            run["pending_disruption"] = True
            return
        run.pop("pending_disruption", None)
        if (problem or deployment["metadata"]["uid"] != dep or budgets[0]["metadata"]["uid"] != item["pdb_uid"]
                or budgets[0]["metadata"]["generation"] != item["pdb_generation"]
                or not self.workloads_available(inventory, plan)):
            raise LabSafetyError("Workload ownership, budget or availability changed before eviction")
        if not inventory.forecast(target, list(residents.values()))[1]:
            raise LabSafetyError("Remaining schedulable capacity is insufficient")
        if phase == "drain":
            run["baseline"] = sorted(inventory.ready_pods(dep))
            run["phase"] = "evict"
            self.audit(db, job, "live.eviction_intent", pod=item["name"], pod_uid=item["uid"])
        else:
            self.adapter.evict(pod)
            run["phase"] = "wait"
            self.audit(db, job, "live.eviction_accepted", pod=item["name"], pod_uid=item["uid"])

    def cleanup(self, db, job: Job, run: dict) -> None:
        plan = run["plan"]
        if run.get("cordon_intent"):
            node = self.adapter.node(plan["node"])
            owner, unschedulable = self.check_worker(node, plan)
            if owner == job.id:
                self.adapter.schedule(node, plan["node_uid"], job.id, cordon=False)
            elif owner is not None or unschedulable:
                raise LabSafetyError("Target scheduling state changed externally; cordon was left untouched")
            self.audit(db, job, "live.scheduling_restored", node=plan["node"])
            self.notify(db, job, plan, "SchedulingRestored")
        if run["problem"]:
            self.jobs.finish(db, job, "failed", Problem.model_validate(run["problem"]))
        else:
            job.checkpoint += 1
            self.jobs.finish(db, job, "succeeded")
