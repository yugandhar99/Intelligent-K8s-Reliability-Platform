"""Generate the provisioned Grafana dashboard definitions."""

import json
from pathlib import Path

OUT = Path(__file__).with_name("grafana") / "dashboards" 
DATASOURCE = {"type": "prometheus", "uid": "reliability-prometheus"}
LATENCY = "reliability_http_request_duration_seconds_bucket"
DURATION = "reliability_job_duration_seconds_bucket"

def metric(query, legend=""):
    return (query, legend)

def labelled(name, label, *, mode=True):
    return metric(name + ('{mode=~"$mode"}' if mode else ""), "{{mode}} " + label)

def dashboard(uid, title, description, cards, charts):
    panels = []
    for index, (label, expression, unit) in enumerate(cards):
        panels.append(panel(index + 1, label, [metric(expression)], unit, index * 6, 0, 6, 5, stat=True))
    for index, (label, expressions, unit, notes) in enumerate(charts):
        panels.append(panel(index + 5, label, expressions, unit, (index % 2) * 12,
                            5 + (index // 2) * 9, 12, 9, description=notes))
    model = {
        "uid": uid, "title": title, "description": description, "schemaVersion": 41,
        "version": 1, "editable": False, "tags": ["Reliability", "Local lab"],
        "refresh": "10s", "time": {"from": "now-10m", "to": "now"},
        "templating": {"list": [{
            "name": "mode", "label": "Measurement mode", "type": "custom",
            "query": "simulation,kubernetes", "includeAll": True, "allValue": "simulation|kubernetes",
            "current": {"text": "All", "value": "$__all", "selected": True},
        }]}, "panels": panels,
    }
    (OUT / f"{uid}.json").write_text(json.dumps(model, indent=2) + "\n")

def panel(number, title, expressions, unit, x, y, w, h, *, stat=False, description=""):
    return {
        "id": number, "title": title, "description": description, "type": "stat" if stat else "timeseries",
        "datasource": DATASOURCE, "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + index), "expr": query, "legendFormat": legend,
                     "instant": stat, "range": not stat, "datasource": DATASOURCE}
                    for index, (query, legend) in enumerate(expressions)],
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic" if not stat else "fixed",
                                                               **({"fixedColor": "semi-dark-blue"} if stat else {})},
                                      **({} if stat else {"custom": {"lineWidth": 2, "fillOpacity": 16,
                                                                       "showPoints": "auto"}})}, "overrides": []},
        "options": ({"colorMode": "value", "graphMode": "none", "textMode": "value",
                     "reduceOptions": {"calcs": ["lastNotNull"]}} if stat else
                    {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                     "tooltip": {"mode": "multi", "sort": "desc"}}),
    }

def main():
    dashboard("cluster-health", "Kubernetes Cluster Health",
              "Live Kind health and simulator state with explicit measurement mode.", [
        ("Live ready workers", 'sum(reliability_cluster_nodes{role="worker",state="ready",mode="kubernetes"})', "short"),
        ("Live ready application replicas", 'sum(reliability_application_replicas{state="ready",mode="kubernetes"})', "short"),
        ("Live PDB violations", 'sum(reliability_pdb_violations{mode="kubernetes"})', "short"),
        ("Live runtime samples", 'max(reliability_runtime_metrics_available{mode="kubernetes"})', "bool_yes_no"),
    ], [
        ("Nodes by readiness", [labelled("reliability_cluster_nodes", "{{role}} {{state}}")], "short",
         "Worker and control-plane groups have bounded series."),
        ("Replica readiness", [labelled("reliability_application_replicas", "{{state}}")], "short",
         "Live counts come from the lab Deployment status."),
        ("Worker resource fractions", [labelled("reliability_cluster_resource_ratio", "{{resource}} {{source}}")], "percentunit",
         "Runtime usage appears when metrics-server reports every worker. Requests use allocatable capacity."),
        ("Disruption safety", [labelled("reliability_pdb_violations", "violations"),
                               labelled("reliability_pdb_zero_slack", "zero slack")], "short",
         "A violation means observed healthy replicas are below the budget minimum."),
    ])
    dashboard("maintenance-reliability", "Maintenance Operations and Reliability",
              "Work queue, execution latency, evictions and worker recovery.", [
        ("Active jobs", 'sum(reliability_jobs{mode=~"$mode",state=~"queued|running|retrying"})', "short"),
        ("Completed jobs", 'sum(reliability_jobs{mode=~"$mode",state="succeeded"})', "short"),
        ("Healthy workers", 'sum(reliability_workers_healthy{mode=~"$mode"})', "short"),
        ("Expired leases", 'sum(reliability_expired_leases_total{mode=~"$mode"})', "short"),
    ], [
        ("Durable job state", [metric('sum by (mode,state) (reliability_jobs{mode=~"$mode",state!="cancelled"})', "{{mode}} {{state}}")],
         "short", "Counts are reconstructed from SQLite."),
        ("Job duration p95", [metric(f'histogram_quantile(0.95,sum by (mode,le)(rate({DURATION}{{mode=~"$mode"}}[5m])))',
                                    "{{mode}} p95")], "s", "Wall time from queue admission to a terminal state."),
        ("Eviction outcomes and retries", [
            metric('sum by (mode,outcome) (reliability_eviction_attempts_total{mode=~"$mode"})', "{{mode}} {{outcome}}"),
            labelled("reliability_eviction_retries_total", "retry decisions")], "short",
         "A blocked live eviction is a PDB response and contributes to the alert signal."),
        ("Worker leases and backlog", [labelled("reliability_workers_healthy", "healthy workers"),
                                        labelled("reliability_leases_overdue", "overdue leases")],
         "short", "Worker counts expire when a heartbeat stops updating its lease."),
    ])
    dashboard("aiops-performance", "AIOps and AI Provider Performance",
              "Incident lifecycle, provider behavior, spending and human decisions.", [
        ("Open incidents", 'sum(reliability_incidents{mode=~"$mode",state="open"})', "short"),
        ("Resolved incidents", 'sum(reliability_incidents{mode=~"$mode",state="resolved"})', "short"),
        ("AI accounted cost today", 'sum(reliability_ai_cost_today_dollars{mode=~"$mode"})', "currencyUSD"),
        ("Rejected approvals", 'sum(reliability_approval_rejections_total{mode=~"$mode"})', "short"),
    ], [
        ("Provider p95 latency", [metric('histogram_quantile(0.95,sum by (mode,provider,le)(rate(reliability_ai_attempt_duration_seconds_bucket{mode=~"$mode"}[5m])))',
                                        "{{mode}} {{provider}}")], "s", "Provider call latency includes failed attempts."),
        ("Provider calls and failures", [
            metric('sum by (mode,provider,outcome)(reliability_ai_attempt_duration_seconds_count{mode=~"$mode",provider="mock"})',
                   "{{mode}} {{provider}} {{outcome}}"),
            metric('sum by (mode,provider,outcome)(reliability_ai_attempt_duration_seconds_count{mode=~"$mode",provider!="mock"}) > 0',
                   "{{mode}} {{provider}} {{outcome}}")], "short", "Mock is local. Optional external provider calls appear after use."),
        ("Incident and approval p95", [
            metric('histogram_quantile(0.95,sum by (mode,phase,le)(rate(reliability_incident_duration_seconds_bucket{mode=~"$mode"}[5m])))',
                   "{{mode}} {{phase}}"),
            metric('histogram_quantile(0.95,sum by (mode,le)(rate(reliability_aiops_approval_duration_seconds_bucket{mode=~"$mode"}[5m])))',
                   "{{mode}} approval")], "s", "Incident durations start at the first observed symptom."),
        ("UTC-day token ledger", [labelled("reliability_ai_tokens_today", "{{provider}} {{direction}}")],
         "short", "The default mock provider incurs zero billable tokens."),
    ])
    dashboard("slo-error-budget", "SLO and Error Budget Monitoring",
              "Observed objectives, error budgets, API traffic and latency.", [
        ("API availability", 'min(reliability:sli_ratio:rate1h{mode=~"$mode",slo="api"})', "percentunit"),
        ("Maintenance reliability", 'min(reliability:sli_ratio:rate1h{mode=~"$mode",slo="maintenance"})', "percentunit"),
        ("Recovery within deadline", 'min(reliability:sli_ratio:rate1h{mode=~"$mode",slo="recovery"})', "percentunit"),
        ("API latency p95", 'max(reliability:api_latency_p95_seconds:rate5m{mode=~"$mode"})', "s"),
    ], [
        ("Observed SLIs", [labelled("reliability:sli_ratio:rate1h", "{{slo}}")], "percentunit",
         "Ratios are absent when the evaluation window has zero observed events."),
        ("Error budget remaining", [labelled("reliability:error_budget_remaining_ratio:rate1h", "{{slo}}")], "percentunit",
         "A negative value shows that observed bad events exceeded the configured allowance."),
        ("API requests and failures per second", [
            metric('sum by (mode) (rate(reliability_http_requests_total{mode=~"$mode"}[5m]))', "{{mode}} all"),
            metric('sum by (mode) (rate(reliability_http_requests_total{mode=~"$mode",status_class="5xx"}[5m]))',
                   "{{mode}} 5xx")], "reqps", "Routes are recorded as stable FastAPI templates."),
        ("API latency percentiles", [
            metric(f'histogram_quantile({percent},sum by (mode,le)(rate({LATENCY}{{mode=~"$mode"}}[5m])))',
                   f"{{{{mode}}}} p{round(percent*100)}") for percent in (0.50, 0.95, 0.99)],
         "s", "Percentiles are estimated from Prometheus histogram buckets."),
    ])

if __name__ == "__main__":
    main()
