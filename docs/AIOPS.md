# Incident analysis and approved remediation

Stage 5 adds incident collection, evidence-based diagnosis, capacity forecasting, and a human approval workflow. Simulation and the deterministic mock provider are the defaults. All live testing uses the dedicated `k8s-reliability-lab` cluster.

## Run the demo

Install the project requirements as described in the [README](../README.md). Choose a fresh simulation database so the example starts from the original topology. Configure a bearer secret for each operator who can approve remediation or close incidents.

```bash
export MAINTENANCE_MODE=simulation
export MAINTENANCE_DB=data/aiops-demo.sqlite3
export AIOPS_PROVIDER=mock
export AIOPS_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export AIOPS_OPERATOR_TOKENS="$(python3 -c 'import json,os; print(json.dumps({"local-operator":os.environ["AIOPS_TOKEN"]}))')"
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

In another shell with access to the same bearer secret, seed the demo and review the incidents. The seven original observations cover restart loops, OOM terminations, pending pods, failed scheduling, a restrictive disruption budget, node memory pressure, and repeated eviction failures. They correlate into five incidents. Repeating the demo request keeps the same episodes.

```bash
api=http://127.0.0.1:8000
curl -fsS -X POST "$api/aiops/demo"
curl -fsS "$api/aiops/incidents" | python3 -m json.tool
curl -fsS "$api/aiops/risk?node=cedar" | python3 -m json.tool
curl -fsS -X POST "$api/aiops/incidents/<incident-id>/recommendations" | python3 -m json.tool
```

For the Cedar eviction incident, review the recommendation's evidence, uncertainty, risk components, and the dry-run plan in `action.reviewed_plan`. The simulation starts a durable maintenance job only when an authenticated operator approves that recommendation's nonce. For advisory recommendations, the `action` field is null and the runbook instructions guide manual investigation.

```bash
curl -fsS -X POST "$api/aiops/recommendations/<recommendation-id>/approve" \
  -H "Authorization: Bearer $AIOPS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"approved":true,"nonce":"<nonce-from-reviewed-recommendation>"}'
curl -fsS "$api/aiops/remediations/<recommendation-id>"
curl -fsS -X POST "$api/aiops/incidents/<incident-id>/resolve" \
  -H "Authorization: Bearer $AIOPS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"disposition":"mitigated"}'
curl -fsS "$api/aiops/incidents/<incident-id>/report" | python3 -m json.tool
```

## Endpoints

| Method and path | Purpose |
| --- | --- |
| `POST /aiops/collect` | Collect current observations and sample the schedulable pool |
| `POST /aiops/demo` | Seed repeatable synthetic incidents in simulation |
| `GET /aiops/incidents` | List incidents by status |
| `GET /aiops/incidents/{id}` | Read supporting observations, a hypothesis, confidence and uncertainty |
| `GET /aiops/incidents/{id}/history` | Read the paginated evidence, recommendation, approval and resolution audit |
| `GET /aiops/incidents/{id}/recommendations` | Read historical recommendations, including reviewed plans |
| `POST /aiops/incidents/{id}/recommendations` | Generate or reuse a structured advisory brief and evaluate an executable candidate independently |
| `POST /aiops/recommendations/{id}/approve` | Authenticate the approver, consume the nonce and queue eligible remediation |
| `GET /aiops/remediations/{id}` | Read the authenticated identity, linked job and maintenance history |
| `POST /aiops/incidents/{id}/resolve` | Record an authenticated operator's resolution disposition |
| `GET /aiops/incidents/{id}/report` | Read the resolved incident timeline, duration, remediation outcomes and follow-up runbooks |
| `GET /aiops/risk?node=…` | Recalculate the target's maintenance risk and include the current capacity forecast |
| `GET /aiops/usage` | Read UTC-day token usage, charged reservations and spending limit |

## Collection, correlation and risk

- Live collection starts every 30 seconds by default. Set `AIOPS_COLLECTION_SECONDS=0` to use only manual collection. The restricted Kubernetes adapter lists Events and reads Pod, Deployment, node and PDB state through the existing Kind identity pin. It promotes a bounded set of Event reasons and conditions into typed observations. Pod-to-Deployment correlation uses owner UIDs. Unschedulable pods can be corroborated by comparing their resource requests to observed node allocatable limits.
- SQLite deduplicates Event UIDs, condition identities and series counts within the current cluster identity. It groups signals by workload or node, namespace and failure family. Crash and OOM signals can share a workload incident, just as Pending and FailedScheduling can share a placement incident. A resolved episode stays closed when an earlier Event is replayed. New observations and increased occurrence counts can open a new episode. Unchanged evidence reuses an existing AI classification.
- Diagnosis uses documented signal weights in the code and raises confidence for corroborating observations and repeated eviction failures. The 0–90 confidence score ranks supporting evidence and represents a heuristic. Recommendations state explicitly when the initiating change remains uncertain.
- Capacity samples use requests divided by allocatable CPU and memory across Ready, schedulable workers. The forecast uses the latest hour of minute samples. A rising series needs three observations, at least two minutes of history, and at least five percentage points of growth before it is extrapolated 30 minutes forward. A current or projected rate of 85% raises a warning. The response exposes the sample count and latest sample timestamp. Maintenance risk combines failed preflight, low capacity headroom, observed target health, and eviction history using additive weights capped at 100.

## Provider integrations and budget

The default mock provider emits the same structured brief for identical inputs and requires no key. For an optional external provider, configure a model that supports text replies from the listed API, the appropriate credential, and conservative per-token costs.

| `AIOPS_PROVIDER` | Credential variable | API reference |
| --- | --- | --- |
| `openai` | `OPENAI_API_KEY` | [Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create) |
| `anthropic` | `ANTHROPIC_API_KEY` | [Messages](https://platform.claude.com/docs/en/api/messages) |
| `gemini` | `GEMINI_API_KEY` | [Generate Content](https://aistudio.google.com/api-reference/generate-content/generate-content) |

Set `AIOPS_MODEL`, `AIOPS_INPUT_MICROUSD_PER_TOKEN`, `AIOPS_OUTPUT_MICROUSD_PER_TOKEN`, and `AIOPS_DAILY_LIMIT_MICROUSD`. Rates are positive integers rounded up to microdollars per token. For example, a configured rate of `3` corresponds to USD $3 per million tokens. The daily limit defaults to zero, and external recommendation requests require an operator bearer secret.

The service reserves an input ceiling and a 512-token output allowance in SQLite before every external attempt. Confirmed usage settles the reservation at the configured rates. An indeterminate response retains its full reservation for the UTC day. Provider-rated usage can differ from the local estimate. Usage above the reserved token ceiling is recorded and opens the circuit. Provider requests use bounded I/O timeouts and at most two attempts for transient failures. The circuit persists across restarts, opens after two failures and allows a single recovery probe after 60 seconds. Blocked, unavailable, or invalid responses fall back to the mock. Different app workers must share the same database and budget settings to share this limit.

## Security and handoff

The provider input is a positive projection of redacted data. It contains only platform-defined signal codes, source categories, frequency categories, runbook IDs and evidence indices. Free-text events, log messages, labels, annotations, names, timestamps, raw snapshots, approval secrets and credential values stay out of provider prompts. Provider credentials travel in authentication headers to fixed HTTPS endpoints. Responses are schema-checked and validated against the cited observations, then rendered with local text templates. The provider interface exposes classification only.

The executable allowlist contains `drain_worker` only. In live mode, only a real eviction failure from the existing engine's local audit can nominate a target. Kubernetes Events alone can provide advisory evidence. The candidate then needs a fresh eligible plan, current PDB status with positive disruption slack, and matching worker and workload identity. The actual Eviction API enforces PDBs. Restarting workloads, changing budgets, running shell commands and patching the OS stay outside the AIOps action surface.

Each recommendation binds eligible work to its incident revision, plan scope, mode, nonce and five-minute expiration. Approval identity comes from a server-side mapping of unique per-operator bearer secrets. The request supplies an explicit boolean and the nonce. On approval, the platform re-reads the cluster, independently rebuilds the plan, and rejects stale evidence, new scope, or safety blockers. It commits approval, replay protection and enqueueing together in SQLite. The existing engine revalidates before cordoning and evicting, verifies availability and restores only its own cordon. Incident and job histories retain the authenticated operator and outcomes.

This is a local portfolio service. Bind the API to loopback and protect SQLite, kubeconfigs and environment secrets as operator-owned files. The local bearer mapping provides distinct identities. Organizations would need their own identity integration and access controls before exposing an API of this kind.

## Validate on the dedicated Kind lab

Use the [Kind setup guide](KUBERNETES.md) and run bootstrap again to apply Stage 5 Event-list RBAC and refresh the lab-only token. Use a fresh application database for the demonstration and keep `AIOPS_PROVIDER=mock`.

```bash
python3 -m pytest -q
RUN_KIND_TESTS=1 python3 -m pytest -q -s
```

The opt-in integration test creates a container that deliberately exits and a Deployment whose CPU request exceeds the lab's node capacity. It verifies real CrashLoopBackOff and scheduling signals, correlation, and advisory runbooks, then removes those fixtures. It also exercises a real restrictive PDB through the existing engine. The test rejects advisory-only approval and a previously safe recommendation after the PDB changes. A fixture operator updates the demo PDB separately, after which a fresh authenticated approval queues a normal Stage 4 drain. The integration fixture restores the original budget and replica settings.

### Measured results

The complete suite passed **232 tests in 59.96 seconds**. That includes the 134 existing tests and 98 new cases, of which one exercised the full AIOps flow in Kind. Across both stages, six tests used the dedicated cluster.

| Check | Outcome |
| --- | --- |
| Synthetic signals | Seven original observations produced five incidents. A repeated collection opened zero additional incidents. |
| Real workload symptoms | CrashLoopBackOff and the unschedulable Deployment were detected and correlated in 2.74 s after applying the fixtures. The scheduling incident cited the CPU request exceeding allocatable CPU. |
| Restrictive PDB and drift | Kubernetes rejected the initial eviction with HTTP 429. AIOps refused approval for the advisory-only result and refused a stale approval after the previously eligible budget changed. |
| Authenticated retry | A fresh approval queued the existing live maintenance job. The drain, readiness verification and cordon restoration completed in 2.62 s from submission. The post-incident report recorded the successful job. |
| Provider safety | Mocked HTTP tests covered OpenAI, Anthropic and Gemini envelopes. The suite verified prompt injection resistance, output rejection, timeouts, retry, circuit recovery, and concurrent budget enforcement. |

## Limitations

Collection is polling with a one-hour lookback for Events and new engine failures. Kubernetes Event retention and aggregation can omit short-lived details. Confidence and extrapolation are rule-based, and the forecast uses requested resources as its capacity signal. Reports record operator resolution, observed outcomes and open questions. External provider HTTP formats and failure modes are tested with local mocked transports. The project demo and live Kubernetes tests use the deterministic provider.
