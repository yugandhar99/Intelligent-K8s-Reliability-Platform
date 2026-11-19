# Intelligent Kubernetes Mainten ance & Reliability Platform

[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Kind-326CE5?logo=kubernetes&logoColor=white)](docs/KUBERNETES.md)
[![Terraform and Helm](https://img.shields.io/badge/Terraform%20%2B%20Helm-deployment-623CE4?logo=terraform&logoColor=white)](docs/DEVSECOPS.md)
[![Prometheus and Grafana](https://img.shields.io/badge/Prometheus%20%2B%20Grafana-observability-E6522C?logo=prometheus&logoColor=white)](docs/OBSERVABILITY.md)
[![MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Rehearse Kubernetes node maintenance, prove a drain is safe, and recover interrupted operations with an audit trail.**

Draining a node can disrupt services if replacement capacity is thin, replicas are unhealthy, or a worker loses ownership mid-operation. This independent portfolio project combines a maintenance simulator, a guarded Kubernetes adapter, AIOps, and operational telemetry. Simulation is the default. Live operations target a dedicated local Kind cluster.

## Architecture

```mermaid
flowchart LR
    O["Operator"] --> API["FastAPI"]
    API --> P["Safety policy and dry-run planner"]
    P --> Q["SQLite jobs, leases and audit"]
    Q --> W["Fenced maintenance worker"]
    W --> S["Simulator"]
    W --> K["Pinned local Kind adapter"]
    K --> I["Event and inventory collection"]
    I --> A["Incident correlation and AIOps"]
    A --> API
    API -. metrics .-> M["Prometheus"]
    W -. traces .-> J["Jaeger"]
    M --> G["Grafana"]
    M --> L["Alertmanager"]
```

The platform uses Python, FastAPI, Pydantic, SQLite, the Kubernetes Python client, OpenTelemetry, Prometheus, Grafana, Alertmanager, Jaeger, Docker Compose, Helm, Terraform, and GitHub Actions. [Architecture and design tradeoffs](docs/ARCHITECTURE.md)

## Maintenance and AIOps workflows

### Kubernetes maintenance

```mermaid
flowchart LR
    D["Discover capacity, workloads and PDBs"] --> P["Review dry-run plan"]
    P --> A["Approve the exact plan"]
    A --> C["Revalidate and claim the cordon"]
    C --> E["Evict through Kubernetes"]
    E --> V["Verify replacement readiness"]
    V --> R["Restore scheduling"]
    E -->|PDB rejects| S["Record refusal and restore the owned cordon"]
```

The simulator supports single-node and multi-node rehearsals. Live maintenance processes one worker at a time, preserves externally owned cordons, and verifies the cluster identity before each mutation. [Kind setup and live workflow](docs/KUBERNETES.md)

### AIOps and human approval

```mermaid
flowchart LR
    T["Typed events and state"] --> I["Correlated incident"]
    I --> A["Classification and runbook"]
    I --> S["Independent safety evaluation"]
    A --> O["Operator review"]
    S --> O
    O --> H["Authenticated one-time approval"]
    H --> F["Fresh state and plan verification"]
    F --> J["Durable maintenance job"]
```

The deterministic mock runs without API keys. Optional OpenAI, Anthropic, and Gemini adapters receive only typed evidence and are subject to a spending limit. The only executable AIOps recommendation is an allowlisted worker drain. [AIOps and approval controls](docs/AIOPS.md)

### Disaster recovery

```mermaid
flowchart LR
    DB["Committed SQLite state"] --> B["Online backup and integrity checks"]
    B --> R["Restore to a new database path"]
    R --> F["Expire stale leases and fence old workers"]
    F --> W["Reconcile checkpoints and current state"]
    W --> J["Resume or safely stop the job"]
```

Simulation checkpoints are transactional. Live recovery reconciles Kubernetes state and restores scheduling only when ownership is still valid. Completed live evictions remain in effect. [Backup, restore, and recovery evidence](docs/RECOVERY.md)

### DevSecOps pipeline

```mermaid
flowchart LR
    C["Commit or pull request"] --> T["Locked dependencies and regression tests"]
    C --> S["CodeQL, secret and vulnerability scans"]
    C --> K["Docker build, Helm, Terraform and Kind checks"]
    T --> A["CI evidence artifacts"]
    S --> A
    K --> A
    A --> M["Published GitHub release and manual authorization"]
    M --> G["Protected GHCR workflow"]
```

CI definitions and security gates are checked locally. GitHub-hosted CI and CodeQL execution are unverified. [Deployment and CI guide](docs/DEVSECOPS.md)

## Operational dashboards

Authentic captures from the running local Grafana stack. The Cluster Health dashboard shows the controlled readiness failure and recovery.

<table>
  <tr>
    <td><a href="docs/screenshots/cluster-health.png"><img src="docs/screenshots/cluster-health.png" alt="Kubernetes Cluster Health dashboard" width="480"></a></td>
    <td><a href="docs/screenshots/maintenance-reliability.png"><img src="docs/screenshots/maintenance-reliability.png" alt="Maintenance Operations and Reliability dashboard" width="480"></a></td>
  </tr>
  <tr>
    <td><a href="docs/screenshots/aiops-performance.png"><img src="docs/screenshots/aiops-performance.png" alt="AIOps and AI Provider Performance dashboard" width="480"></a></td>
    <td><a href="docs/screenshots/slo-error-budget.png"><img src="docs/screenshots/slo-error-budget.png" alt="SLO and Error Budget Monitoring dashboard" width="480"></a></td>
  </tr>
</table>

## Measured results

| Local validation | Measured result |
| --- | --- |
| Regression suite | **265 passed** in 94.28 s, including six Kind tests |
| 100 concurrent simulator reads | **285.05 requests/s**, P50 266.10 ms, P95 320.96 ms, P99 327.31 ms, zero errors |
| 20 concurrent submissions of one plan | **One durable job**, zero request errors |
| Kind drain | **Four pods** replaced in **11.82 s**, with **24/24 sampled HTTP probes** successful |

[Benchmark method, concurrency checks, and validation results](docs/PERFORMANCE.md)

## Run the demo

Use Python 3.12 and install the locked dependencies. The commands below start the simulator with a dedicated local database.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements-ci.lock
MAINTENANCE_DB=data/rehearsal.sqlite3 python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

In a second terminal, activate the environment, check the worker, run a simulated maintenance job, and seed original incident scenarios.

```bash
source .venv/bin/activate
curl -fsS http://127.0.0.1:8000/ready
python deploy/smoke.py
curl -fsS -X POST http://127.0.0.1:8000/aiops/demo
curl -fsS http://127.0.0.1:8000/aiops/incidents
python -m pytest -q
```

Open the FastAPI docs at `http://127.0.0.1:8000/docs`. To run the containerized simulator with observability, set a local Grafana password and start the Compose stack.

```bash
export OBS_GRAFANA_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose -f compose.monitoring.yaml -f compose.platform.yaml up --build -d --wait
```

## Repository map

| Path | Purpose |
| --- | --- |
| [app/](app/) | API, maintenance, durable recovery, Kind adapter, AIOps, telemetry |
| [tests/](tests/) and [benchmarks/](benchmarks/) | Regression, Kind integration, and reproducible benchmarks |
| [kubernetes/](kubernetes/) | Dedicated Kind setup, restricted RBAC, and demo workloads |
| [deploy/](deploy/) | Helm chart, Terraform example, and validation tooling |
| [monitoring/](monitoring/) | Prometheus rules, Alertmanager, and Grafana provisioning |
| [.github/workflows/](.github/workflows/) | CI, CodeQL, and manually authorized GHCR workflow |
| [docs/](docs/) | Design, operational guides, screenshots, and release evidence |

## Security and limitations

- Live Kubernetes mutations require local identity validation and an approved safety plan. Reboot and OS patch steps are simulated.
- Deployments use a single SQLite writer and a local filesystem lock. Recreate upgrades can cause short interruptions.
- Provider prompts omit credentials, raw logs and identifiers. The paid-provider paths were validated through mocked HTTP responses. The lab reports requested resources and exposes runtime utilization when metrics-server supplies it.
- Images and dependencies are pinned, and the build runs as non-root. Unresolved Debian base-image findings and the distinction between local validation and hosted CI are documented in the security review.

[Simulator guide](docs/SIMULATION.md) · [Observability](docs/OBSERVABILITY.md) · [Security review](docs/SECURITY_REVIEW.md) · [Changelog](CHANGELOG.md) · [v1.0.0 release notes](docs/RELEASE_v1.0.0.md) · [Publication commands](docs/PUBLICATION.md) · [Third-party notices](THIRD_PARTY_NOTICES.md)
