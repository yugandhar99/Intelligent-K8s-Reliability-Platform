"""Check deployment contracts without contacting a remote Kubernetes cluster."""

import json
import re
import shutil
import subprocess
import sys
from base64 import b64encode
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from time import monotonic, sleep

import pytest
import yaml
from fastapi.testclient import TestClient

from app.jobs import JobService
from app.main import create_app
from app.process_lock import exclusive_database
from deploy import release_gate, verify_lab

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy/helm/reliability"
HELM = shutil.which("helm") or next(
    (str(p) for p in (ROOT / "data/bin/helm",) if p.is_file()), None
)


def render(*args, okay=True):
    if not HELM:
        pytest.skip("Helm rendering requires the local Helm executable")
    result = subprocess.run(
        [HELM, "template", "isolation", str(CHART), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    if not okay:
        assert result.returncode != 0
        return result.stderr
    assert result.returncode == 0, result.stderr
    return {item["kind"]: item for item in yaml.safe_load_all(result.stdout) if item}


def test_chart_uses_recreate_and_one_restricted_writer():
    manifests = render("-f", str(CHART / "values-testing.yaml"))
    deployment = manifests["Deployment"]["spec"]
    assert deployment["replicas"] == 1 and deployment["strategy"] == {
        "type": "Recreate"
    }
    pod = deployment["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    assert pod["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }
    assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/ready"
    assert manifests["PersistentVolumeClaim"]["spec"]["accessModes"] == [
        "ReadWriteOnce"
    ]
    assert manifests["ConfigMap"]["data"]["MAINTENANCE_EXCLUSIVE_DB"] == "1"
    assert manifests["ConfigMap"]["data"]["MAINTENANCE_MODE"] == "simulation"
    assert manifests["Service"]["spec"]["type"] == "ClusterIP"
    assert manifests["NetworkPolicy"]["spec"]["egress"] == []
    assert manifests["PodDisruptionBudget"]["spec"]["minAvailable"] == 1
    assert manifests["Role"]["rules"][0]["verbs"] == ["get"]
    assert manifests["RoleBinding"]["subjects"][0]["name"].endswith("-config-reader")
    assert "Namespace" not in manifests


def test_chart_rejects_an_unsafe_replica_override():
    assert "replicaCount" in render("--set", "replicaCount=2", okay=False)


def test_production_chart_requires_an_immutable_image():
    profile = ["-f", str(CHART / "values-production.yaml")]
    assert "image.digest is required" in render(*profile, okay=False)
    digest = "sha256:" + "a" * 64
    manifest = render(*profile, "--set-string", f"image.digest={digest}")
    image = manifest["Deployment"]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image.endswith("@" + digest)
    assert (
        manifest["PersistentVolumeClaim"]["metadata"]["annotations"][
            "helm.sh/resource-policy"
        ]
        == "keep"
    )


def test_database_lock_rejects_a_second_process_and_releases(tmp_path):
    file = tmp_path / "cluster.sqlite3"
    code = """import sys
from pathlib import Path
from app.process_lock import exclusive_database

with exclusive_database(Path(sys.argv[1]), enabled=True):
    print("lock acquired")
"""

    def child():
        return subprocess.run(
            [sys.executable, "-c", code, str(file)],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )

    with exclusive_database(file, enabled=True):
        result = child()
        assert (
            result.returncode != 0
            and "Another application process owns" in result.stderr
        )
    result = child()
    assert result.returncode == 0 and result.stdout.strip() == "lock acquired"


def test_readiness_fails_when_a_worker_lease_is_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(JobService, "workers", lambda self: [])
    with TestClient(create_app(tmp_path / "readiness.sqlite3")) as api:
        assert api.get("/health").status_code == 200
        assert api.get("/ready").status_code == 503
        assert api.get("/ready").json() == {"status": "waiting", "mode": "simulation"}


def test_readiness_succeeds_with_the_real_worker(tmp_path):
    with TestClient(create_app(tmp_path / "readiness.sqlite3")) as api:
        deadline = monotonic() + 4
        while monotonic() < deadline:
            response = api.get("/ready")
            if response.status_code == 200:
                break
            sleep(0.05)
        assert response.json() == {"status": "ready", "mode": "simulation"}


def identity():
    cert = b"fixture-ca-without-credentials"
    endpoint = "https://127.0.0.1:6443"
    context = verify_lab.CONTEXT
    cfg = {
        "current-context": context,
        "contexts": [
            {"name": context, "context": {"cluster": context, "user": "lab-admin"}}
        ],
        "clusters": [
            {
                "name": context,
                "cluster": {
                    "certificate-authority-data": b64encode(cert).decode(),
                    "server": endpoint,
                },
            }
        ],
        "users": [
            {
                "name": "lab-admin",
                "user": {"client-certificate-data": "", "client-key-data": ""},
            }
        ],
    }
    pin = {
        "server": endpoint,
        "ca_sha256": sha256(cert).hexdigest(),
        "namespace_uid": "1" * 36,
    }
    return cfg, pin


@pytest.mark.parametrize(
    "change", ["cloud", "proxy", "context", "other_ca", "plugin", "multiple"]
)
def test_local_guard_refuses_remote_or_altered_credentials(change):
    cfg, pin = identity()
    verify_lab.validate_config(cfg, pin)
    if change == "cloud":
        cfg["clusters"][0]["cluster"]["server"] = pin["server"] = (
            "https://remote.example.test"
        )
    elif change == "proxy":
        cfg["clusters"][0]["cluster"]["proxy-url"] = "https://remote.example.test"
    elif change == "context":
        cfg["current-context"] = "production"
    elif change == "other_ca":
        pin["ca_sha256"] = "a" * 64
    elif change == "plugin":
        cfg["users"][0]["user"]["exec"] = {"command": "external-plugin"}
    elif change == "multiple":
        cfg["clusters"].append({})
    with pytest.raises(ValueError):
        verify_lab.validate_config(cfg, pin)


@pytest.mark.parametrize(
    "tag", ["v1.2", "refs/tags/v1.2.3", "v1.2.3;echo", "v1.2.3-rc.1"]
)
def test_release_gate_rejects_non_stable_or_shell_like_tags(tag, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail(
            "Invalid release identifiers must fail before Git or GitHub operations"
        )

    monkeypatch.setattr(release_gate.subprocess, "check_output", forbidden)
    assert not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag)
    with pytest.raises(ValueError, match="stable"):
        release_gate.verify_release(tag, "a" * 40, "owner/repo", "fixture")


@pytest.mark.parametrize(
    ("changed", "error"),
    [
        ("head", "authorized checkout"),
        ("draft", "published stable"),
        ("prerelease", "published stable"),
        ("unpublished", "published stable"),
    ],
)
def test_release_gate_checks_local_tag_and_published_release(
    changed, error, monkeypatch
):
    head = "a" * 40
    tag = "v1.2.3"
    release = {
        "tag_name": tag,
        "draft": False,
        "prerelease": False,
        "published_at": "2026-01-01",
    }
    monkeypatch.setattr(
        release_gate.subprocess, "check_output", lambda *args, **kwargs: head
    )
    monkeypatch.setattr(
        release_gate,
        "urlopen",
        lambda *args, **kwargs: BytesIO(json.dumps(release).encode()),
    )
    assert (
        release_gate.verify_release(tag, head, "Owner/Repo", "fixture")
        == "ghcr.io/owner/repo:v1.2.3"
    )
    if changed == "head":
        head = "b" * 40
        monkeypatch.setattr(
            release_gate.subprocess, "check_output", lambda *args, **kwargs: "a" * 40
        )
    elif changed == "unpublished":
        release["published_at"] = None
    else:
        release[changed] = True
    with pytest.raises(ValueError, match=error):
        release_gate.verify_release(tag, head, "Owner/Repo", "fixture")


def test_ci_permissions_manual_publish_and_immutable_action_pins():
    workflows = {}
    for file in (ROOT / ".github/workflows").glob("*.yml"):
        workflow = yaml.load(file.read_text(), Loader=yaml.BaseLoader)
        workflows[file.stem] = workflow
        assert workflow["permissions"] == {"contents": "read"}
        for name, job in workflow["jobs"].items():
            assert "packages" not in job.get("permissions", {}) or name == "publish"
            for step in job["steps"]:
                if "uses" in step:
                    assert re.fullmatch(r".+@[0-9a-f]{40}", step["uses"])
    assert list(workflows["publish"]["on"]) == ["workflow_dispatch"]
    job = workflows["publish"]["jobs"]["publish"]
    assert job["environment"] == "ghcr-release"
    assert "inputs.confirm == 'publish'" in job["if"]


def test_runtime_and_ci_locks_agree_and_cover_all_pins():
    def lock(file: str):
        contents = (ROOT / file).read_text().replace("\\\n", " ")
        items = re.findall(r"(?m)^([a-zA-Z0-9_.-]+)==([^\s]+)\s+([^\n]+)", contents)
        assert items and all(
            re.search(r"--hash=sha256:[0-9a-f]{64}", hashes) for _, _, hashes in items
        )
        return {name: version for name, version, _ in items}

    runtime = lock("requirements.lock")
    ci = lock("requirements-ci.lock")
    assert "pytest" in ci and "pytest" not in runtime
    assert all(ci[name] == version for name, version in runtime.items())
