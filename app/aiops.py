"""Durable incident lifecycle and deterministic handoff to the maintenance engine."""

from dataclasses import dataclass
from hmac import compare_digest
import json
from secrets import token_urlsafe
from time import time
from uuid import uuid4

from .aiops_providers import AIService, Brief
from .aiops_signals import (
    RULES, RUNBOOKS, Observation, capacity_forecast, demo_observations, digest, explain,
    kube_observations, observation,
)
from .jobs import TERMINAL, JobService
from .service import RecordNotFound, logger
from .store import MaintenanceConflict
from .telemetry import traced

APPROVAL_SECONDS = 300
EVIDENCE_LIMIT = 12


class ApprovalRefused(MaintenanceConflict):
    pass


@dataclass(frozen=True)
class Candidate:
    target: str
    contract: str
    plan: dict


class IncidentService:
    def __init__(self, jobs: JobService, ai: AIService, clock=time) -> None:
        self.jobs, self.ai, self.clock = jobs, ai, clock
        self.store = jobs.store
        self.live = jobs.live
        self.pool = self.live.adapter.pin["namespace_uid"] if self.live else "simulation"

    def initialize(self) -> None:
        with self.store.transaction(write=True) as db:
            statements = (
                """CREATE TABLE IF NOT EXISTS ai_incidents (
                    id TEXT PRIMARY KEY, pool TEXT NOT NULL, group_key TEXT NOT NULL,
                    opened REAL NOT NULL, latest REAL NOT NULL, revision INTEGER NOT NULL,
                    resolved REAL, disposition TEXT
                )""",
                """CREATE UNIQUE INDEX IF NOT EXISTS ai_open_incident
                    ON ai_incidents(pool, group_key) WHERE resolved IS NULL""",
                """CREATE TABLE IF NOT EXISTS ai_seen (
                    pool TEXT NOT NULL, id TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(pool, id)
                )""",
                """CREATE TABLE IF NOT EXISTS ai_evidence (
                    incident_id TEXT NOT NULL REFERENCES ai_incidents(id),
                    id TEXT NOT NULL, observed REAL NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(incident_id, id)
                )""",
                """CREATE TABLE IF NOT EXISTS ai_capacity (
                    pool TEXT NOT NULL, minute INTEGER NOT NULL, cpu INTEGER NOT NULL, memory INTEGER NOT NULL,
                    PRIMARY KEY(pool, minute)
                )""",
                """CREATE TABLE IF NOT EXISTS ai_recommendations (
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES ai_incidents(id),
                    revision INTEGER NOT NULL, contract TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL,
                    payload TEXT NOT NULL
                )""",
                """CREATE INDEX IF NOT EXISTS ai_recommendations_lookup
                    ON ai_recommendations(incident_id, revision, created)""",
                """CREATE TABLE IF NOT EXISTS ai_remediations (
                    id TEXT PRIMARY KEY REFERENCES ai_recommendations(id),
                    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id), actor TEXT NOT NULL, timestamp REAL NOT NULL
                )""",
                """CREATE TABLE IF NOT EXISTS ai_incident_audit (
                    sequence INTEGER PRIMARY KEY, incident_id TEXT NOT NULL REFERENCES ai_incidents(id),
                    timestamp REAL NOT NULL, event TEXT NOT NULL, actor TEXT NOT NULL, detail TEXT NOT NULL
                )""",
                """CREATE INDEX IF NOT EXISTS ai_audit_lookup ON ai_incident_audit(incident_id, sequence)""",
            )
            for statement in statements:
                db.execute(statement)
            self.ai.initialize(db)

    def audit(self, db, incident: str, event: str, actor: str = "system", **detail) -> None:
        logger.info(event, extra={"fields": {"incident_id": incident,
                                              **{key: detail[key] for key in ("job", "recommendation", "signal")
                                                 if key in detail}}})
        db.execute("INSERT INTO ai_incident_audit(incident_id,timestamp,event,actor,detail) VALUES (?,?,?,?,?)",
                   (incident, self.clock(), event, actor, json.dumps(detail)))

    def ingest(self, db, observations: list[Observation]) -> dict:
        opened, changed = 0, set()
        for item in observations:
            seen = db.execute("SELECT count FROM ai_seen WHERE pool=? AND id=?", (self.pool, item.id)).fetchone()
            if seen and seen[0] >= item.count:
                continue
            row = db.execute("SELECT id FROM ai_incidents WHERE pool=? AND group_key=? AND resolved IS NULL",
                             (self.pool, item.group)).fetchone()
            if row:
                incident, = row
            else:
                incident = uuid4().hex
                db.execute("INSERT INTO ai_incidents VALUES (?,?,?,?,?,0,NULL,NULL)",
                           (incident, self.pool, item.group, item.timestamp, item.timestamp))
                self.audit(db, incident, "incident.opened")
                opened += 1
            db.execute("""INSERT INTO ai_seen VALUES (?,?,?)
                ON CONFLICT(pool,id) DO UPDATE SET count=excluded.count""", (self.pool, item.id, item.count))
            db.execute("""INSERT INTO ai_evidence VALUES (?,?,?,?)
                ON CONFLICT(incident_id,id) DO UPDATE SET observed=excluded.observed,payload=excluded.payload""",
                       (incident, item.id, item.timestamp, item.model_dump_json()))
            db.execute("UPDATE ai_incidents SET latest=MAX(latest,?), revision=revision+1 WHERE id=?",
                       (item.timestamp, incident))
            self.audit(db, incident, "evidence.recorded", signal=item.signal, observation=item.id, count=item.count)
            changed.add(incident)
        return {"opened": opened, "updated": len(changed), "incidents": sorted(changed)}

    def demo(self) -> dict:
        if self.live:
            raise MaintenanceConflict("Synthetic incident generation is available in simulation mode")
        with self.store.transaction(write=True) as db:
            return self.ingest(db, demo_observations(self.clock()))

    def engine_evidence(self, db, now: float) -> list[Observation]:
        rows = db.execute("""SELECT history.sequence, history.timestamp, jobs.payload, history.details, run.payload
            FROM job_history history
            JOIN jobs ON jobs.id=history.job_id JOIN live_checkpoints run ON run.job_id=jobs.id
            WHERE history.timestamp>=? AND history.event='live.eviction_blocked'
            AND json_extract(run.payload, '$.plan.cluster_uid')=?
            ORDER BY history.sequence""", (now - 3600, self.pool))
        findings = []
        for sequence, occurred, job, details, checkpoint in rows:
            node = json.loads(job)["nodes"][0]
            findings.append(observation("engine", "eviction_failed", str(sequence), node, occurred, node=node))
            pod = json.loads(details).get("pod")
            if any(item["name"] == pod and item["disruptions_allowed"] == 0
                   for item in json.loads(checkpoint)["plan"]["pods"]):
                findings.append(observation("engine", "pdb", str(sequence), node, occurred, node=node))
        return findings

    @traced("aiops.collect")
    def collect(self) -> dict:
        now = self.clock()
        findings = []
        if self.live:
            from .kube_planning import Inventory, quantities, ready, requests, worker

            snapshot = self.live.adapter.discover()
            findings = kube_observations(self.live.adapter.events(), snapshot, now)
            inventory = Inventory(snapshot)
            workers = {name: node for name, node in inventory.nodes.items()
                       if worker(node) and ready(node) and not node["spec"].get("unschedulable") and not node["spec"].get("taints")}
            total = [quantities(node["status"]["allocatable"]) for node in workers.values()]
            used = [requests(pod) for pod in inventory.pods if pod["spec"].get("nodeName") in workers
                    and pod.get("status", {}).get("phase") not in {"Failed", "Succeeded"}]
        else:
            cluster = self.jobs.single.cluster()
            workers = {name: node for name, node in cluster.nodes.items() if node.ready and node.schedulable}
            total = [(node.allocatable.cpu_millis, node.allocatable.memory_mib) for node in workers.values()]
            used = [(pod.requests.cpu_millis, pod.requests.memory_mib)
                    for pod in cluster.pods.values() if pod.node in workers]
        capacity = [sum(row[index] for row in total) for index in range(2)]
        utilization = [round(10_000 * sum(row[index] for row in used) / capacity[index])
                       if capacity[index] else 10_000 for index in range(2)]
        with self.store.transaction(write=True) as db:
            if self.live:
                findings.extend(self.engine_evidence(db, now))
            db.execute("INSERT OR IGNORE INTO ai_capacity VALUES (?,?,?,?)",
                       (self.pool, int(now // 60), *utilization))
            forecast = self.forecast(db)
            for resource in forecast["warnings"]:
                stats = forecast["resources"][resource]
                findings.append(observation("forecast", "capacity", f"{resource}:{int(now // 3600)}", resource, now,
                                            current=round(100 * stats["current_percent"]),
                                            projected=round(100 * stats["projected_30m_percent"])))
            return {**self.ingest(db, findings), "observations": len(findings), "forecast": forecast}

    def forecast(self, db) -> dict:
        samples = db.execute("""SELECT minute*60.0,cpu,memory FROM ai_capacity WHERE pool=? AND minute>=?
            ORDER BY minute""", (self.pool, int(self.clock() // 60) - 59)).fetchall()
        return capacity_forecast(samples)

    def require(self, db, incident: str):
        row = db.execute("""SELECT opened,latest,revision,resolved,disposition FROM ai_incidents
            WHERE id=? AND pool=?""", (incident, self.pool)).fetchone()
        if not row:
            raise RecordNotFound("Incident not found")
        return row

    def evidence(self, db, incident: str) -> list[Observation]:
        return [Observation.model_validate_json(row[0]) for row in db.execute(
            "SELECT payload FROM ai_evidence WHERE incident_id=? ORDER BY observed DESC,id LIMIT ?",
            (incident, EVIDENCE_LIMIT),
        )]

    def describe(self, db, incident: str) -> dict:
        opened, latest, revision, resolved, disposition = self.require(db, incident)
        evidence = self.evidence(db, incident)
        total, = db.execute("SELECT COUNT(*) FROM ai_evidence WHERE incident_id=?", (incident,)).fetchone()
        return {"id": incident, "status": "resolved" if resolved is not None else "open",
                "opened_at": opened, "last_observed_at": latest, "revision": revision,
                "resolved_at": resolved, "disposition": disposition, "subject": evidence[0].subject,
                "namespace": evidence[0].namespace, "evidence_count": total,
                "evidence": [item.model_dump() for item in evidence], **explain(evidence)}

    def incident(self, incident: str) -> dict:
        with self.store.transaction() as db:
            return self.describe(db, incident)

    def incidents(self, limit: int, status: str) -> list[dict]:
        clause = {"all": "1", "open": "resolved IS NULL", "resolved": "resolved IS NOT NULL"}[status]
        with self.store.transaction() as db:
            rows = db.execute(f"SELECT id FROM ai_incidents WHERE pool=? AND {clause} ORDER BY latest DESC,id LIMIT ?",
                              (self.pool, limit)).fetchall()
            return [self.describe(db, row[0]) for row in rows]

    def current_plan(self, node: str, db=None) -> dict:
        if self.live:
            from .kube_planning import Inventory

            return Inventory(self.live.adapter.discover()).plan(node)
        cluster = self.store.snapshot(db) if db else self.jobs.single.cluster()
        return self.jobs.single.plan_for(cluster, node).model_dump(mode="json", exclude={"id"})

    def validate_plan(self, target: str, plan: dict) -> Candidate:
        if self.live:
            from .kube_planning import disruption_available, scope

            if plan["problems"] or not plan["node_uid"] or not disruption_available(plan):
                raise ApprovalRefused("Current live plan is blocked by deterministic maintenance checks")
            contract = digest(self.pool, target, scope(plan))
        else:
            if not plan["safe"]:
                raise ApprovalRefused("Current simulation plan is blocked by deterministic maintenance checks")
            contract = digest(self.pool, target, plan)
        return Candidate(target, contract, plan)

    def target(self, evidence: list[Observation]) -> str | None:
        # Only the engine's own eviction history can nominate a live target.
        source = "engine" if self.live else "demo"
        targets = {item.node for item in evidence if item.source == source and item.signal == "eviction_failed"
                   and item.node and item.timestamp >= self.clock() - 3600}
        return next(iter(targets)) if len(targets) == 1 else None

    def risk(self, node: str, plan: dict, forecast: dict, evidence: list[Observation]) -> dict:
        blocked = bool(plan["problems"])
        if self.live:
            from .kube_planning import disruption_available

            blocked = blocked or not disruption_available(plan)
        components = {
            "failed_preflight": 60 if blocked else 0,
            "low_pool_headroom": 15 * len(forecast["warnings"]),
            "target_health_signal": 30 if any(item.node == node and item.signal in {"node_unready", "pressure"}
                                              for item in evidence) else 0,
            "eviction_history": 15 if any(item.node == node and item.signal == "eviction_failed"
                                        for item in evidence) else 0,
        }
        return {"node": node, "score": min(100, sum(components.values())), "blocked": blocked,
                "components": components, "forecast": forecast}

    def assess(self, node: str) -> dict:
        plan = self.current_plan(node)
        with self.store.transaction() as db:
            forecast = self.forecast(db)
            rows = db.execute("""SELECT evidence.payload FROM ai_evidence evidence
                JOIN ai_incidents incident ON incident.id=evidence.incident_id WHERE incident.pool=?
                AND incident.resolved IS NULL AND evidence.observed>=?
                AND json_extract(evidence.payload,'$.node')=?""", (self.pool, self.clock() - 3600, node))
            evidence = [Observation.model_validate_json(row[0]) for row in rows]
        return self.risk(node, plan, forecast, evidence)

    def recommendations(self, incident: str) -> list[dict]:
        with self.store.transaction() as db:
            self.require(db, incident)
            rows = db.execute("SELECT payload FROM ai_recommendations WHERE incident_id=? ORDER BY created DESC,rowid DESC LIMIT 100",
                              (incident,))
            return [json.loads(row[0]) for row in rows]

    @traced("aiops.recommend", argument=0, identifier="incident_id")
    def recommend(self, incident: str) -> dict:
        with self.store.transaction() as db:
            _, _, revision, resolved, _ = self.require(db, incident)
            evidence = self.evidence(db, incident)
            forecast = self.forecast(db)
        if resolved is not None:
            raise MaintenanceConflict("The incident is already resolved")
        target = self.target(evidence)
        candidate = None
        risk = None
        if target:
            plan = self.current_plan(target)
            risk = self.risk(target, plan, forecast, evidence)
            try:
                candidate = self.validate_plan(target, plan)
            except ApprovalRefused:
                pass
        recommendation_id = uuid4().hex
        contract = candidate.contract if candidate else ""
        now = self.clock()
        with self.store.transaction() as db:
            previous = db.execute("""SELECT payload FROM ai_recommendations WHERE incident_id=? AND revision=?
                ORDER BY created DESC,rowid DESC LIMIT 1""", (incident, revision)).fetchone()
        if previous:
            cached = json.loads(previous[0])
            if cached["contract"] == contract and cached["expires_at"] > now and cached["risk"] == risk:
                return cached
            brief, provider, fallback = Brief.model_validate(cached["brief"]), cached["provider"], cached["fallback"]
        else:
            brief, provider, fallback = self.ai.classify(recommendation_id, evidence)
        now = self.clock()
        cited = [evidence[index] for index in brief.evidence]
        diagnosis = explain(cited, brief.lead_signal)
        payload = {
            "id": recommendation_id, "incident_id": incident, "revision": revision, "provider": provider,
            "fallback": fallback, "brief": brief.model_dump(), "contract": contract, "risk": risk,
            "summary": f"{RULES[brief.lead_signal][0]}. See cited observations and suggested runbooks.",
            "confidence": diagnosis["confidence"], "uncertainty": diagnosis["uncertainty"],
            "evidence": [item.id for item in cited],
            "observations": [item.model_dump() for item in evidence],
            "runbooks": [{"id": name, "instruction": RUNBOOKS[name]} for name in brief.runbooks],
            "expires_at": now + APPROVAL_SECONDS,
            "action": {"type": "drain_worker", "node": candidate.target, "mode": "kubernetes" if self.live else "simulation",
                       "reviewed_plan": candidate.plan, "nonce": token_urlsafe(24)} if candidate else None,
        }
        with self.store.transaction(write=True) as db:
            self.require(db, incident)
            db.execute("INSERT INTO ai_recommendations VALUES (?,?,?,?,?,?,?)",
                       (recommendation_id, incident, revision, contract, now, payload["expires_at"], json.dumps(payload)))
            self.audit(db, incident, "recommendation.generated", recommendation=recommendation_id, provider=provider)
        return payload

    @staticmethod
    def authorize_recommendation(db, recommendation: dict, incident_row, nonce: str, now: float) -> None:
        action = recommendation["action"]
        if not action or action["type"] != "drain_worker":
            raise ApprovalRefused("Recommendation has no allowlisted executable action")
        if not compare_digest(nonce.encode(), action["nonce"].encode()):
            raise ApprovalRefused("Approval nonce did not match")
        if db.execute("SELECT 1 FROM ai_remediations WHERE id=?", (recommendation["id"],)).fetchone():
            raise ApprovalRefused("Approval was already consumed")
        if recommendation["expires_at"] <= now:
            raise ApprovalRefused("Approval expired")
        if incident_row[3] is not None or incident_row[2] != recommendation["revision"]:
            raise ApprovalRefused("Incident changed since the recommendation was issued")

    def get_recommendation(self, db, recommendation: str) -> dict:
        row = db.execute("""SELECT rec.payload FROM ai_recommendations rec JOIN ai_incidents incident
            ON incident.id=rec.incident_id WHERE rec.id=? AND incident.pool=?""", (recommendation, self.pool)).fetchone()
        if row is None:
            raise RecordNotFound("Recommendation not found")
        return json.loads(row[0])

    def revalidate_live(self, incident: str, recommendation: str, actor: str, node: str) -> dict:
        from .kube_adapter import KubeUnavailable, LabSafetyError

        try:
            return self.current_plan(node)
        except (KubeUnavailable, LabSafetyError) as error:
            with self.store.transaction(write=True) as db:
                self.audit(db, incident, "approval.revalidation_failed", actor,
                           recommendation=recommendation, reason=type(error).__name__)
            raise

    def approve(self, recommendation_id: str, actor: str, nonce: str) -> dict:
        incident = None
        try:
            with self.store.transaction() as db:
                recommendation = self.get_recommendation(db, recommendation_id)
                incident = recommendation["incident_id"]
                self.authorize_recommendation(db, recommendation, self.require(db, incident), nonce, self.clock())
            node = recommendation["action"]["node"]
            fresh = self.revalidate_live(incident, recommendation_id, actor, node) if self.live else None
            with self.store.transaction(write=True) as db:
                current = self.require(db, incident)
                self.authorize_recommendation(db, recommendation, current, nonce, self.clock())
                if recommendation["action"]["mode"] != ("kubernetes" if self.live else "simulation"):
                    raise ApprovalRefused("Recommendation mode differs from the active cluster")
                if self.target(self.evidence(db, incident)) != node:
                    raise ApprovalRefused("Current trusted evidence does not authorize this target")
                if db.execute("""SELECT 1 FROM ai_remediations rem JOIN ai_recommendations rec ON rec.id=rem.id
                    WHERE rec.incident_id=? AND rec.revision=? AND rec.contract=?""",
                              (incident, recommendation["revision"], recommendation["contract"])).fetchone():
                    raise ApprovalRefused("This incident revision and plan were already approved")
                if db.execute("""SELECT 1 FROM ai_remediations rem JOIN ai_recommendations rec ON rec.id=rem.id
                    JOIN jobs ON jobs.id=rem.job_id WHERE rec.incident_id=? AND
                    json_extract(jobs.payload,'$.state') NOT IN ('succeeded','failed','rejected','cancelled')""",
                              (incident,)).fetchone():
                    raise ApprovalRefused("An approved remediation for this incident is still active")
                candidate = self.validate_plan(node, fresh if self.live else self.current_plan(node, db))
                if not compare_digest(candidate.contract, recommendation["contract"]):
                    raise ApprovalRefused("Cluster state changed since the recommendation was issued")
                plan_id = uuid4().hex
                if self.live:
                    live_plan = {**candidate.plan, "id": plan_id, "created": self.clock(), "cluster_uid": self.pool}
                    db.execute("INSERT INTO live_plans VALUES (?,?)", (plan_id, json.dumps(live_plan)))
                    job = self.live.approve_in(db, plan_id, actor, require_disruption=True)
                else:
                    plan = {**candidate.plan, "id": plan_id}
                    db.execute("INSERT INTO plans VALUES (?,?)", (plan_id, json.dumps(plan)))
                    job = self.jobs.submit_in(db, "single", plan_id)
                db.execute("INSERT INTO ai_remediations VALUES (?,?,?,?)",
                           (recommendation_id, job.id, actor, self.clock()))
                self.audit(db, incident, "approval.consumed", actor, recommendation=recommendation_id, job=job.id)
            return {"id": recommendation_id, "approved_by": actor, "job": job.model_dump()}
        except MaintenanceConflict as error:
            if incident:
                with self.store.transaction(write=True) as db:
                    self.audit(db, incident, "approval.refused", actor, recommendation=recommendation_id, reason=str(error))
            raise

    def remediation(self, recommendation: str) -> dict:
        with self.store.transaction() as db:
            self.get_recommendation(db, recommendation)
            row = db.execute("SELECT job_id, actor, timestamp FROM ai_remediations WHERE id=?",
                             (recommendation,)).fetchone()
            if not row:
                raise RecordNotFound("Approved remediation not found")
            job_id, actor, approved = row
            job = self.jobs.get(db, job_id)
        return {"id": recommendation, "approved_by": actor, "approved_at": approved, "job": job.model_dump(),
                "history": [event.model_dump() for event in self.jobs.history(job_id)]}

    def resolve(self, incident: str, actor: str, disposition: str) -> dict:
        with self.store.transaction(write=True) as db:
            if self.require(db, incident)[3] is not None:
                raise MaintenanceConflict("Incident already resolved")
            rows = db.execute("""SELECT rem.job_id FROM ai_remediations rem JOIN ai_recommendations rec
                ON rec.id=rem.id WHERE rec.incident_id=?""", (incident,)).fetchall()
            if any(self.jobs.get(db, row[0]).state not in TERMINAL for row in rows):
                raise MaintenanceConflict("Wait for active remediation to finish")
            db.execute("UPDATE ai_incidents SET resolved=?, disposition=? WHERE id=?",
                       (self.clock(), disposition, incident))
            self.audit(db, incident, "incident.resolved", actor, disposition=disposition)
            return self.describe(db, incident)

    @staticmethod
    def audit_rows(rows) -> list[dict]:
        return [{"sequence": seq, "timestamp": stamp, "event": event, "actor": actor, "detail": json.loads(detail)}
                for seq, stamp, event, actor, detail in rows]

    def history(self, incident: str, after: int, limit: int) -> list[dict]:
        with self.store.transaction() as db:
            self.require(db, incident)
            return self.audit_rows(db.execute("""SELECT sequence,timestamp,event,actor,detail
                FROM ai_incident_audit WHERE incident_id=? AND sequence>? ORDER BY sequence LIMIT ?""",
                (incident, after, limit)))

    def report(self, incident: str) -> dict:
        with self.store.transaction() as db:
            summary = self.describe(db, incident)
            if summary["status"] != "resolved":
                raise MaintenanceConflict("Resolve the incident before generating its post-incident report")
            jobs = db.execute("""SELECT rem.id,rem.job_id,rem.actor FROM ai_remediations rem JOIN ai_recommendations rec
                ON rec.id=rem.id WHERE rec.incident_id=? ORDER BY rem.timestamp""", (incident,)).fetchall()
            remediations = [{"recommendation": rec, "job": job, "approved_by": actor,
                             "outcome": self.jobs.get(db, job).state} for rec, job, actor in jobs]
            recommendations, = db.execute("SELECT COUNT(*) FROM ai_recommendations WHERE incident_id=?",
                                          (incident,)).fetchone()
            timeline = self.audit_rows(db.execute("""SELECT sequence,timestamp,event,actor,detail
                FROM ai_incident_audit WHERE incident_id=? ORDER BY sequence DESC LIMIT 100""", (incident,)))
        return {"incident": summary, "timeline": list(reversed(timeline)), "duration_seconds": max(0, summary["resolved_at"] - summary["opened_at"]),
                "recommendation_count": recommendations, "remediations": remediations,
                "follow_up_runbooks": [{"id": name, "instruction": RUNBOOKS[name]} for name in summary["runbooks"]]}
