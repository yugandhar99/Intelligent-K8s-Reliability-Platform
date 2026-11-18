# Kubernetes maintenance in the Kind lab

Stage 4 supports a single live worker maintenance job at a time with persistent checkpoints. It uses the official Kubernetes Python client and targets only the local `k8s-reliability-lab` cluster. Kind v0.31.0 and Kubernetes v1.35.0 were used for validation.

## Set up the dedicated cluster

Install Docker, Kind, kubectl and the project requirements. Run these commands from the project root, with Docker running.

```bash
mkdir -p data
kind create cluster --name k8s-reliability-lab --config kubernetes/kind.yaml \
  --kubeconfig data/kind-admin.yaml --wait 180s
python3 kubernetes/bootstrap.py
kubectl --kubeconfig data/kind-admin.yaml --context kind-k8s-reliability-lab get nodes
kubectl --kubeconfig data/kind-admin.yaml --context kind-k8s-reliability-lab \
  -n reliability-lab get deployments,pods,pdbs
```

The cluster has one control-plane and three workers. Bootstrap obtains the Kubernetes endpoint directly from Kind. It installs the lab namespace, service account, RBAC, and demo workloads, then writes a separate token-only kubeconfig and identity pin in the ignored `data/` directory. The service-account token requests a two-hour lifetime. Refresh it before an expired token blocks recovery. The bootstrap script reapplies the manifests, including the demo's original replica settings. Run it between maintenance jobs.

Set an approval token independently of the Kubernetes token and keep the API bound to loopback.

```bash
export MAINTENANCE_MODE=kubernetes
export LAB_KUBECONFIG="$PWD/data/kind-live.yaml"
export LAB_IDENTITY="$PWD/data/kind-identity.json"
export LAB_APPROVAL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Live mode defaults to `data/live.sqlite3`. Keep that file and reuse it for all workers and restarts against this lab. With `MAINTENANCE_MODE` unset, the API starts in simulation mode with `data/cluster.sqlite3`.

## Plan, approve, and follow a drain

| Endpoint | Behavior |
| --- | --- |
| `GET /kubernetes/cluster` | Reads the current nodes, pods, Deployments, ReplicaSets, namespaces and PDBs |
| `POST /kubernetes/maintenance/plans` | Stores a read-only Kubernetes forecast for a worker |
| `POST /kubernetes/maintenance/execute` | Requires the local operator token and explicit approval of an eligible plan ID |
| `GET /maintenance/jobs/{id}` | Shows state, checkpoint, retry count and terminal problem |
| `GET /maintenance/jobs/{id}/history` | Returns ordered durable audit entries |
| `GET /maintenance/workers` | Reports SQLite worker lease health |
| `POST /maintenance/recovery` | Requeues expired leases for reconciliation by a live worker |

Generate a plan using a worker identified in discovery. Review its `problems`, approved pod list, PDB status and per-worker capacity before submitting approval.

```bash
api=http://127.0.0.1:8000
curl -fsS "$api/kubernetes/cluster"
curl -fsS "$api/kubernetes/maintenance/plans" -H 'Content-Type: application/json' \
  -d '{"node":"k8s-reliability-lab-worker"}' | python3 -m json.tool
```

Use the reviewed plan's ID and your operator label in the following request. Plans expire after five minutes. An approval replay for the same plan returns the existing job.

```bash
curl -fsS "$api/kubernetes/maintenance/execute" \
  -H "Authorization: Bearer $LAB_APPROVAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"plan_id":"<reviewed-plan-id>","approver":"<operator>","approved":true}'
curl -fsS "$api/maintenance/jobs/<job-id>/history" | python3 -m json.tool
kubectl --kubeconfig data/kind-admin.yaml --context kind-k8s-reliability-lab \
  -n reliability-lab get events --sort-by=.lastTimestamp
```

## Architecture and security boundary

- **Target identity.** The adapter accepts a single context for the dedicated Kind name, an HTTPS endpoint at `127.0.0.1`, a pinned certificate authority, and token-only credentials. It checks the pinned `kube-system` namespace UID through TLS before every node patch, eviction, or Event creation. It does not load the current context from a user kubeconfig or use in-cluster credentials.
- **Authorization.** The service account can read the discovery resources across the lab, patch only the three named workers, and create pod evictions and Events only in `reliability-lab`. The app additionally verifies worker role and UID, namespace, Deployment ownership, budget coverage, readiness and relocation constraints. DaemonSet pods stay on the target. System, static and unmanaged pods and unsafe storage stop a drain. The approval token is a local shared secret. The operator label is self-reported.
- **Forecast.** The planner uses actual allocatable CPU, memory and pod slots minus bound pod requests. It accounts for init containers, sidecars and overhead and forecasts whether the target pods fit on other Ready, schedulable workers. CPU is in cores and memory in bytes. Unsupported scheduling and storage constraints fail closed. The PDB snapshot is advisory. The Eviction API makes the admission decision.
- **Execution.** The worker revalidates the approved node and pod UIDs and workload revisions. It records intent, claims the cordon with an ownership annotation and a resource-version compare-and-swap, and verifies continued ownership before each eviction. Each `policy/v1` eviction carries pod UID and resource-version preconditions and uses configured pod termination grace. The worker waits for the original pod to leave, a newly Ready replacement on another worker, and the Deployment's desired availability before proceeding. The final drain check precedes the simulated reboot and OS patch.
- **Durability.** SQLite persists approval, operation ownership, worker leases, fenced checkpoints, and audit history. A worker holds the SQLite writer transaction while applying one live step so another worker using the same file cannot reclaim the lease mid-step. Node cordons and eviction intents are persisted before the corresponding side effects. On retry or restart the job observes the node annotation, pod UID, termination state, PDB status and replacement availability in Kubernetes. Cleanup patches only an observed, owned cordon at its current resource version. Kubernetes Events are best effort and reference the lab service account, with node, node UID and job ID in the message. The durable audit is available through the jobs API.
- **Failure handling.** HTTP 429 from eviction stops the drain and triggers cordon cleanup. Transient API errors and ambiguous mutation responses trigger bounded backoff and a fresh read. The drain has a two-minute default deadline. Cleanup retries transient failures after that deadline. An identity mismatch, lost ownership or permanent permission error leaves the scheduling state untouched and records a failure for operator inspection. A completed eviction is irreversible. A failed job can leave completed replacements in place.

## Validate

The default test run executes simulator and adapter unit tests and skips the live scenarios. With the dedicated lab bootstrapped and no other maintenance running, run the live suite serially.

```bash
python3 -m pytest -q
RUN_KIND_TESTS=1 python3 -m pytest -q -s tests/test_kubernetes_live.py
```

The live suite uses the restricted adapter for maintenance. It uses the dedicated admin kubeconfig for fixture setup, deliberately changing scheduling or ownership in test cases, and probing the two demo Services through the Kubernetes API proxy. The third demo Deployment is scaled to three replicas during the restrictive-PDB test and returned to zero afterwards.

### Measured results

The combined suite ran against Kind v0.31.0, Kubernetes v1.35.0 and Kubernetes Python client v35.0.0. **134 tests passed in 45.50 seconds**, including all 117 existing tests, 12 new unit cases, and five live tests.

| Live scenario | Observed result |
| --- | --- |
| Successful worker drain | Four pods replaced on other workers and scheduling restored in 9.01 s from approval to completion. The minimum sampled Ready count was two of three for each Deployment. The HTTP probe succeeded 13/13 times for `web` and 13/13 for `api` (100% sampled availability). |
| Restrictive PDB | Kubernetes returned HTTP 429. The protected pod remained in place and the job restored scheduling in 1.31 s. |
| Concurrent approval and insufficient capacity | A second simultaneous approval and a competing worker claim were blocked. The capacity forecast rejected a drain while every alternative worker was cordoned. |
| Stale cordon ownership | The job failed safely. The external owner and cordon were preserved. |
| Interrupted eviction | The test stopped the worker after the real eviction request succeeded and before its SQLite checkpoint committed. A new worker reconciled the live state, observed the replacement without repeating the eviction, and finished the drain in 9.13 s after restart. |

## Known limitations

This stage runs one live job per SQLite database. Use one database for the lab. Independent SQLite databases do not share a maintenance lock. The simulator's batch and pause, resume and cancel workflows remain simulator features.

Discovery is a series of fresh API reads. The capacity forecast is a conservative estimate and does not reserve capacity or model every scheduler feature. It is based on resource requests rather than runtime utilization, and metrics-server is not installed. The job verifies Kubernetes readiness and Deployment availability. The integration test separately observes sampled HTTP success through the API proxy. Samples do not measure downtime between probes or external client availability.
