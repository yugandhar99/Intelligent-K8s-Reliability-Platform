"""Verify a real readiness failure and Alertmanager notification pair in the lab."""

from base64 import b64decode 
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from time import monotonic, sleep, time
from urllib.parse import urlencode
from urllib.request import urlopen

import yaml

from app.kube_adapter import LAB, KubernetesAdapter

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "kubernetes" / "observability-demo.yaml"
ALERT = "ApplicationDisruptionBudgetViolation"
PROBE = "/usr/share/nginx/html/observability-healthz"


def get(base, path):
    with urlopen(base + path, timeout=5) as response:
        return json.load(response)


def poll(check, timeout=90):
    end = monotonic() + timeout
    while monotonic() < end:
        if result := check():
            return result
        sleep(2)
    raise RuntimeError("Timed out waiting for observed monitoring state")


def prom(expression):
    return get("http://127.0.0.1:9090", "/api/v1/query?" + urlencode({"query": expression}))["data"]["result"]


def violations(value):
    return any(float(item["value"][1]) == value for item in prom('reliability_pdb_violations{mode="kubernetes"}'))


def notifications(start):
    return [item for item in get("http://127.0.0.1:9095", "/notifications")
            if item["group_labels"].get("alertname") == ALERT and item["received_at"] >= start]


def active():
    return any(item["labels"].get("alertname") == ALERT
               for item in get("http://127.0.0.1:9093", "/api/v2/alerts"))


def main():
    data = ROOT / "data"
    admin_config = data / "kind-admin.yaml"
    adapter = KubernetesAdapter(data / "kind-live.yaml", data / "kind-identity.json")
    [cluster] = yaml.safe_load(admin_config.read_text())["clusters"]
    ca = sha256(b64decode(cluster["cluster"]["certificate-authority-data"])).hexdigest()
    if (cluster["name"] != f"kind-{LAB}" or ca != adapter.pin["ca_sha256"]
            or cluster["cluster"]["server"] != adapter.pin["server"]):
        raise RuntimeError("Admin credentials do not match the dedicated pinned lab")

    def admin(*args):
        adapter.validate_identity()
        return subprocess.check_output(["kubectl", "--kubeconfig", str(admin_config), "--context", f"kind-{LAB}",
                                        "--request-timeout=10s", "-n", "reliability-lab", *args], text=True, timeout=70)

    existing = admin("get", "deployment", "observability-probe", "--ignore-not-found", "-o", "name")
    if existing.strip():
        raise RuntimeError("The isolated observability fixture is already present")
    admin("apply", "-f", str(MANIFEST))
    try:
        admin("rollout", "status", "deployment/observability-probe", "--timeout=60s")
        poll(lambda: violations(0) and not active())
        pods = json.loads(admin("get", "pods", "-l", "app=observability-probe", "-o", "json"))["items"]
        name = sorted(pod["metadata"]["name"] for pod in pods)[0]
        start = time()
        admin("exec", name, "--", "mv", PROBE, PROBE + ".offline")
        try:
            poll(lambda: violations(1))
            observed = time()
            firing = poll(lambda: active() and next((item for item in notifications(start)
                                                     if item["status"] == "firing"), None))
            sleep(25)
            assert active(), "Alert should still be firing for the same active violation"
            assert len(notifications(start)) == 1, "Alertmanager emitted a duplicate notification"
        finally:
            recovered = time()
            admin("exec", name, "--", "mv", PROBE + ".offline", PROBE)
        poll(lambda: violations(0))
        resolved = poll(lambda: not active() and next((item for item in notifications(start)
                                                       if item["status"] == "resolved"), None))
        assert len(notifications(start)) == 2
        assert firing["group_key"] == resolved["group_key"]
        assert firing["alerts"][0]["fingerprint"] == resolved["alerts"][0]["fingerprint"]
        report = {"violation_observed_seconds": round(observed - start, 2),
                  "firing_notification_seconds": round(firing["received_at"] - start, 2),
                  "resolution_notification_seconds": round(resolved["received_at"] - recovered, 2),
                  "firing_notifications": 1, "resolved_notifications": 1,
                  "alert": ALERT, "deduplicated_for_seconds": 25, "start": start, "end": time()}
        print(json.dumps(report, indent=2), flush=True)
    finally:
        admin("delete", "-f", str(MANIFEST), "--ignore-not-found=true", "--wait=true", "--timeout=45s")
        adapter.close()


if __name__ == "__main__":
    main()
