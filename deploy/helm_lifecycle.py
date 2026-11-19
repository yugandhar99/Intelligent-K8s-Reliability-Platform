"""Exercise an isolated Helm release in the verified local Terraform namespace."""

import argparse
import json
import re
import subprocess
from time import monotonic

from verify_lab import CONTEXT, ROOT, verify

CHART = ROOT / "deploy/helm/reliability"
RELEASE = "stage7-smoke"
NAME = f"{RELEASE}-reliability"


def lifecycle(namespace: str) -> dict:
    if not re.fullmatch(r"stage7-tf-[a-z0-9]([a-z0-9-]{0,40}[a-z0-9])?", namespace):
        raise ValueError("Use an isolated stage7-tf-* namespace")
    config = str(ROOT / "data/kind-admin.yaml")
    values = str(CHART / "values-testing.yaml")

    def kube(*args: str) -> str:
        verify()
        return subprocess.check_output(
            [
                "kubectl",
                "--kubeconfig",
                config,
                "--context",
                CONTEXT,
                "--namespace",
                namespace,
                *args,
            ],
            text=True,
            timeout=240,
        )

    def helm(*args: str) -> str:
        verify()
        return subprocess.check_output(
            [
                "helm",
                "--kubeconfig",
                config,
                "--kube-context",
                CONTEXT,
                "--namespace",
                namespace,
                *args,
            ],
            text=True,
            timeout=240,
        )

    def pod_uid() -> str:
        pods = json.loads(
            kube(
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=api-worker",
                "-o",
                "json",
            )
        )
        (pod,) = [
            pod for pod in pods["items"] if not pod["metadata"].get("deletionTimestamp")
        ]
        return pod["metadata"]["uid"]

    def probe(slo: str, job: str = "") -> dict:
        code = (ROOT / "deploy/smoke.py").read_text()
        return json.loads(
            kube(
                "exec",
                f"deployment/{NAME}",
                "--",
                "python",
                "-c",
                code,
                "--api-slo",
                slo,
                "--existing-job",
                job,
            )
        )

    def assert_pvc(uid: str) -> None:
        assert kube("get", "pvc", NAME, "-o", "jsonpath={.metadata.uid}") == uid

    metadata = json.loads(kube("get", "namespace", namespace, "-o", "json"))["metadata"]
    assert metadata["labels"]["app.kubernetes.io/managed-by"] == "terraform"
    assert not json.loads(
        helm("list", "--all", "--filter", f"^{RELEASE}$", "-o", "json")
    )
    assert not kube("get", "pvc", NAME, "--ignore-not-found", "-o", "name")
    measurements = {}
    start = monotonic()
    helm("install", RELEASE, str(CHART), "-f", values, "--atomic", "--timeout", "180s")
    measurements["install_seconds"] = round(monotonic() - start, 2)
    helm("test", RELEASE, "--timeout", "60s")
    first = probe("0.99")
    first_uid = pod_uid()
    pvc_uid = kube("get", "pvc", NAME, "-o", "jsonpath={.metadata.uid}")
    start = monotonic()
    helm(
        "upgrade",
        RELEASE,
        str(CHART),
        "-f",
        values,
        "--atomic",
        "--timeout",
        "180s",
        "--set-string",
        "observability.apiSlo=0.97",
    )
    measurements["upgrade_seconds"] = round(monotonic() - start, 2)
    second = pod_uid()
    assert second != first_uid
    probe("0.97", first["job"])
    assert_pvc(pvc_uid)
    start = monotonic()
    helm("rollback", RELEASE, "1", "--wait", "--timeout", "180s")
    measurements["rollback_seconds"] = round(monotonic() - start, 2)
    assert pod_uid() != second
    probe("0.99", first["job"])
    assert_pvc(pvc_uid)
    helm("test", RELEASE, "--timeout", "60s")
    history = json.loads(helm("history", RELEASE, "-o", "json"))
    assert [revision["revision"] for revision in history] == [1, 2, 3]
    start = monotonic()
    helm("uninstall", RELEASE, "--wait", "--timeout", "180s")
    measurements["uninstall_seconds"] = round(monotonic() - start, 2)
    assert not kube(
        "get",
        "deployment,svc,pvc",
        "--field-selector",
        f"metadata.name={NAME}",
        "-o",
        "name",
    )
    assert (
        kube(
            "get", "configmap", "terraform-lab-metadata", "-o", "jsonpath={.data.owner}"
        )
        == "terraform"
    )
    return {**measurements, "job_persisted": True, "revisions": 3}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    print(json.dumps(lifecycle(args.namespace), indent=2))
