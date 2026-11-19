# Observability, monitoring and alerting

Stage 6 instruments the FastAPI server, simulation and live maintenance, durable workers, the Kubernetes adapter, and AIOps. The simulation remains the default. The local stack provides Prometheus, Grafana, Alertmanager, a local webhook receiver, and Jaeger. All live reads and maintenance use the dedicated `k8s-reliability-lab` credentials and identity pin.

## Start the local stack

Install the Python dependencies and start Docker. Re-run the [Kind bootstrap](KUBERNETES.md) to refresh credentials and grant the read-only node metrics permission.

```bash
python3 -m pip install -r requirements.txt
python3 kubernetes/bootstrap.py
export OBS_GRAFANA_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose -f compose.monitoring.yaml up -d
```

The Compose targets use Docker's `host-gateway` alias. You can override it with `OBS_SCRAPE_HOST` for a different Docker network setup. The app API binds to loopback. Its separate Prometheus-only HTTP listener binds to an address reachable by the Docker bridge. Use a private host interface or host firewall if the host has an externally reachable interface.

In one terminal, start the simulation:

```bash
export OBS_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
MAINTENANCE_MODE=simulation MAINTENANCE_DB=data/obs-simulation.sqlite3 \
  OBS_METRICS_ADDRESS=0.0.0.0 OBS_METRICS_PORT=9108 \
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --no-access-log
```

In another terminal, configure `LAB_APPROVAL_TOKEN` as in the Kind guide and start live mode:

```bash
export OBS_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
MAINTENANCE_MODE=kubernetes MAINTENANCE_DB=data/obs-kubernetes.sqlite3 \
  LAB_KUBECONFIG=data/kind-live.yaml LAB_IDENTITY=data/kind-identity.json \
  OBS_METRICS_ADDRESS=0.0.0.0 OBS_METRICS_PORT=9109 \
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --no-access-log
```

The standalone application also serves `GET /metrics` on its normal API port. Set `OBS_METRICS_PORT` to enable the optional dedicated listener. The two app instances use distinct SQLite databases. Use one instrumented server process per mode in this setup. Grafana uses `admin` and the configured password on its first startup. Keep that password when reusing the Grafana volume. Grafana, Prometheus, Alertmanager, the receiver, and Jaeger publish locally on ports 3000, 9090, 9093, 9095, and 16686 respectively.

## Data and instrumentation

Prometheus scrapes both app modes every five seconds. Job status, durable operation outcomes, incidents, AIOps approvals, and UTC-day provider accounting come from SQLite. Request, provider, eviction, and worker counters cover the current process. A separate poller reads the Kind inventory every five seconds by default (`OBS_CLUSTER_POLL_SECONDS`). Scrapes use the last successful snapshot and emit freshness signals alongside it.

Every application series is labeled `mode=simulation` or `mode=kubernetes`. HTTP route labels use FastAPI route templates or `unmatched`. Other labels use fixed method classes, status classes, resource names, workflow names, and outcome values. Request paths, pod names, worker IDs, operation IDs, incident IDs, operator names, prompts, model identifiers, headers, and credentials stay out of labels. OpenTelemetry records HTTP, maintenance, Kubernetes, recovery, and AI spans. JSON logs carry trace and span IDs plus local operation or incident IDs when available. Background worker spans correlate with admission through the durable operation ID. Setting the OTLP endpoint enables export. Traces stay local with the supplied Jaeger endpoint.

| Metric family (prefix `reliability_`) | Interpretation |
| --- | --- |
| `http_requests_total`, `http_request_duration_seconds` | Request count by class and wall-time histogram for p50, p95 and p99 |
| `jobs`, `job_duration_seconds`, `sync_maintenance_duration_seconds`, `oldest_active_job_seconds` | Durable state counts, admission-to-terminal wall time, synchronous simulation wall time, and active backlog age |
| `workers_healthy`, `worker_heartbeats_total`, `worker_latest_heartbeat_timestamp_seconds`, `leases_overdue`, `expired_leases_total`, `recovery_duration_seconds` | Healthy lease count, heartbeat activity, overdue leases and recovery classifications |
| `eviction_attempts_total`, `eviction_retries_total`, `kubernetes_request_duration_seconds` | Actual Kubernetes attempt outcomes, eviction retry decisions, and API latency. Simulator evictions are labeled separately |
| `cluster_nodes`, `cluster_pressure_nodes`, `cluster_resource_ratio`, `runtime_metrics_available` | Readiness, pressure and worker resource fractions. `source=requests` uses allocatable resources. `source=usage` requires metrics-server samples for all workers |
| `application_replicas`, `pdb_violations`, `pdb_zero_slack` | Ready and desired replicas, observed PDB healthy-replica shortfalls, and budgets permitting zero disruptions |
| `incidents`, `incident_duration_seconds` | Open or resolved counts and time from first observation to detection or resolution |
| `ai_attempt_duration_seconds`, `ai_output_rejections_total`, `ai_tokens_today`, `ai_cost_today_dollars` | Provider calls, latency, and transport availability, invalid output, token accounting, and estimated spend including outstanding reservations |
| `approval_rejections_total`, `live_approval_duration_seconds`, `aiops_approval_duration_seconds` | Authentication, validation and safety rejection classes, plus time from plan or recommendation to approval |

## Dashboards

- **Kubernetes Cluster Health** shows worker readiness, replicas, capacity fractions and disruption budgets. [Screenshot](screenshots/cluster-health.png)
- **Maintenance Operations and Reliability** shows durable job states, latency, eviction outcomes and worker leases. [Screenshot](screenshots/maintenance-reliability.png)
- **AIOps and AI Provider Performance** shows open and resolved incidents, providers, approvals, tokens and accounted cost. [Screenshot](screenshots/aiops-performance.png)
- **SLO and Error Budget Monitoring** shows the rolling SLIs, remaining budget, throughput and latency distributions. [Screenshot](screenshots/slo-error-budget.png)

The dashboards are provisioned from JSON. Regenerate those definitions with `python3 monitoring/dashboards.py`. The mode selector filters mixed-mode panels. The four cluster-health cards show live Kind measurements. To reproduce the screenshots locally, install Playwright and Chromium, then run `python3 monitoring/capture.py --base http://127.0.0.1:3000 --output docs/screenshots`. The capture script uses the configured Grafana password.

## SLIs and SLOs

The recording rules use a rolling one-hour rate. An SLI remains absent until its denominator is positive. API health and readiness checks, scrape requests, and unmatched routes are excluded from API availability. Target outages are monitored separately by Prometheus `up`.

| Objective | Good events divided by eligible events | Environment variable | Default |
| --- | --- | --- | --- |
| API availability | Recognized API requests below HTTP 500 divided by all eligible requests | `OBS_SLO_API` | 99.5% |
| Maintenance reliability | Successful runs divided by terminal durable jobs and synchronous runs, excluding cancellations | `OBS_SLO_MAINTENANCE` | 95% |
| Recovery performance | Expired leases reconciled within the deadline divided by observed expired leases | `OBS_SLO_RECOVERY` | 99% |

Recovery measures elapsed time from persisted lease expiry through fencing and reconciliation. The deadline defaults to 10 seconds and uses `OBS_RECOVERY_DEADLINE_SECONDS`. Live job phases continue through the existing engine after the worker reconciles the lease. Recovery SLI timing covers lease recovery, and the operation-duration histogram covers the resulting workload lifecycle.

**Remaining budget** is `1 - (1 - observed SLI) / (1 - configured SLO)`. A negative result means the observed bad-event rate exceeds the allowance. Histogram quantiles estimate percentiles from bucket boundaries. API throughput, error rate and provider availability use separate five-minute rules.

## Alerts and controlled demonstration

Prometheus evaluates rules for scrape outages, node degradation, stale inventory, PDB shortfalls, prolonged maintenance, worker failure, API errors, eviction failures, resource pressure, AI spending, and exhausted error budgets. Alertmanager groups by alert name and mode, waits five seconds before the first notification, groups updates every ten seconds, and repeats unchanged alerts hourly. The webhook delivers firing and resolved notifications locally. Inspect the current alerts at the Alertmanager UI and deliveries at `http://127.0.0.1:9095/notifications`.

Use a fresh simulation database. Configure the AIOps operator mapping as in the [AIOps guide](AIOPS.md) and export that operator's bearer secret as `OBS_OPERATOR_TOKEN`. The demo builds its own simulator activity, signs off a valid action, deliberately rejects two invalid approvals, and injects an expired lease into the specified simulation database. The real worker then recovers that lease.

```bash
PYTHONPATH=. python3 monitoring/demo.py simulation --database data/obs-simulation.sqlite3
PYTHONPATH=. python3 monitoring/demo.py traffic --seconds 180
```

The optional `monitoring/demo.py live-drain --approve-live` uses the existing approval API and `OBS_LAB_TOKEN`. For the controlled alert, run the following command while both app modes and the monitoring stack are available.

```bash
PYTHONPATH=. python3 monitoring/kind_failure.py
docker compose -f compose.monitoring.yaml exec -T --workdir /etc/prometheus \
  prometheus promtool test rules rules.test.yml
RUN_KIND_TESTS=1 python3 -m pytest -q
```

The failure script verifies the dedicated Kind identity, installs an isolated two-replica workload, temporarily moves one HTTP readiness file, and restores it. It observes the real PDB shortfall, checks the alert and notification fingerprint, and removes its own fixture after validation. The script leaves the existing demo workloads intact.

## Validation results

The full suite passed **243 tests in 66.36 seconds**, including all 232 prior tests and six tests that used Kind. Eleven new Python cases covered route cardinality, secret exclusion, tracing, metrics, and configuration validation. Promtool passed the SLI arithmetic, zero-denominator, alert timing and resolution fixtures. Alertmanager configuration validation also passed.

| Live check | Observed result |
| --- | --- |
| Prometheus | Four scrape targets were up (simulator, Kind, Prometheus and Alertmanager). All 22 recording and alerting rules were healthy. |
| Grafana | The Prometheus data source was healthy and all four provisioned dashboards were captured in Chromium. |
| Jaeger | Both app modes exported traces. Observed operations included HTTP requests, maintenance worker steps, worker recovery, Kubernetes calls, and mock AI provider calls. |
| Kind maintenance | An approved drain replaced four pods and completed in **13.45 seconds** in the demonstration. |
| Kind alert and recovery | A real readiness failure raised the live PDB shortfall from zero to one. The metric was observed after **10.29 seconds** and the firing webhook after **30.00 seconds**. |
| Deduplication | The same alert stayed active for a further 25 seconds with one firing notification. Restoring readiness cleared the shortfall and produced one resolved webhook after **14.67 seconds**. Both deliveries shared the same group and fingerprint. |
| Resource telemetry | The lab reported worker request fractions and `runtime_metrics_available=0`. The request fractions shown in Grafana use observed allocatable capacity and pod requests. |

The recorded demo showed a simulated expired lease recovery, two resolved simulation incidents, and four accepted live eviction requests. The SLI plots are based on this short local run and its small sample sizes. See the [Prometheus alerting rule reference](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) and [Grafana provisioning reference](https://grafana.com/docs/grafana/latest/administration/provisioning/).

## Limitations

The dashboards and notification receiver are local. The receiver keeps the last 500 notifications in memory, and Jaeger uses in-memory storage. Durable metrics derive from the retained SQLite history, so scrape cost grows with history size. Counters and latency from process-local activity reset with a process restart. Sustained deployment would need retention, authenticated scraping and trace ingress, and monitoring of the monitoring stack itself.
