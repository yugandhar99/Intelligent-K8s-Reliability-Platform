"""Fail closed unless the local admin kubeconfig still matches the dedicated Kind pin."""

import json
import re
import subprocess
from base64 import b64decode
from hashlib import sha256
from pathlib import Path

import yaml

CONTEXT = "kind-k8s-reliability-lab"
ROOT = Path(__file__).resolve().parents[1]


def validate_config(config: dict, pin: dict) -> None:
    try:
        [context] = config["contexts"]
        [cluster] = config["clusters"]
        [user] = config["users"]
        server = cluster["cluster"]["server"]
        ca = b64decode(cluster["cluster"]["certificate-authority-data"], validate=True)
        matches = (
            config["current-context"]
            == context["name"]
            == context["context"]["cluster"]
            == cluster["name"]
            == CONTEXT
            and context["context"]["user"] == user["name"]
            and set(cluster["cluster"]) == {"server", "certificate-authority-data"}
            and set(user["user"]) == {"client-certificate-data", "client-key-data"}
            and re.fullmatch(r"https://127\.0\.0\.1:[0-9]{1,5}", server)
            and server == pin["server"]
            and sha256(ca).hexdigest() == pin["ca_sha256"]
            and re.fullmatch(r"[a-f0-9-]{36}", pin["namespace_uid"])
        )
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError("Invalid dedicated lab credentials or identity pin") from error
    if not matches:
        raise ValueError("Expected the pinned HTTPS loopback Kind cluster")


def verify():
    admin = ROOT / "data" / "kind-admin.yaml"
    pin = json.loads((ROOT / "data" / "kind-identity.json").read_text())
    validate_config(yaml.safe_load(admin.read_text()), pin)
    uid = subprocess.check_output(
        (
            "kubectl",
            "--kubeconfig",
            str(admin),
            "--context",
            CONTEXT,
            "--request-timeout=6s",
            "get",
            "namespace",
            "kube-system",
            "-o",
            "jsonpath={.metadata.uid}",
        ),
        text=True,
        timeout=12,
    )
    if uid != pin["namespace_uid"]:
        raise ValueError("The live kube-system namespace UID differs from the lab pin")


if __name__ == "__main__":
    verify()
    print("Dedicated local Kind identity verified")
