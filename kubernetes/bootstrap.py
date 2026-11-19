"""Provision credentials and demo workloads on the dedicated local Kind cluster."""

from base64 import b64decode 
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import urlsplit

import yaml

LAB = "k8s-reliability-lab"
CONTEXT = f"kind-{LAB}"
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
ADMIN = DATA / "kind-admin.yaml"


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True, cwd=ROOT)


def kubectl(*args: str) -> str:
    return command("kubectl", "--kubeconfig", str(ADMIN), "--context", CONTEXT, *args)


def main() -> None:
    os.umask(0o077)
    DATA.mkdir(exist_ok=True)
    kind = os.environ.get("KIND_BIN", "kind")
    # Obtain the admin endpoint directly from Kind, never from a user's default kubeconfig.
    raw = command(kind, "get", "kubeconfig", "--name", LAB)
    config = yaml.safe_load(raw)
    [cluster] = config["clusters"]
    server = urlsplit(cluster["cluster"]["server"])
    if (
        config["current-context"] != CONTEXT or cluster["name"] != CONTEXT
        or server.scheme != "https" or server.hostname != "127.0.0.1"
        or server.netloc != f"127.0.0.1:{server.port}" or not server.port
        or server.path or server.query or server.fragment
    ):
        raise RuntimeError("Expected the dedicated loopback Kind endpoint")
    ADMIN.write_text(raw)
    ADMIN.chmod(0o600)
    identity = {
        "namespace_uid": kubectl("get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"),
        "server": cluster["cluster"]["server"],
        "ca_sha256": sha256(b64decode(cluster["cluster"]["certificate-authority-data"])).hexdigest(),
    }
    if not identity["namespace_uid"]:
        raise RuntimeError("Missing Kind namespace UID")
    kubectl("apply", "-f", "kubernetes/rbac.yaml")
    kubectl("apply", "-f", "kubernetes/workloads.yaml")
    token = kubectl("-n", "reliability-lab", "create", "token", "maintenance", "--duration=2h").strip()
    [context] = config["contexts"]
    [user] = config["users"]
    config["users"] = [{"name": user["name"], "user": {"token": token}}]
    context["context"]["namespace"] = "reliability-lab"
    for name, content in (("kind-identity.json", json.dumps(identity)), ("kind-live.yaml", yaml.safe_dump(config))):
        path = DATA / name
        path.write_text(content)
        path.chmod(0o600)
    print("Dedicated lab credentials and identity pin written under data/")


if __name__ == "__main__":
    main()
