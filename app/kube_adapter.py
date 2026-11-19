"""Bounded Kubernetes API calls against an explicitly pinned local Kind lab."""

from base64 import b64decode
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from urllib.parse import urlsplit
from uuid import uuid4

from kubernetes import client
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import HTTPError
import yaml

from .telemetry import kube_request

LAB = "k8s-reliability-lab"
NAMESPACE = "reliability-lab"
WORKERS = frozenset(f"{LAB}-worker{suffix}" for suffix in ("", "2", "3"))
OWNER = "reliability-lab/maintenance-owner"
TIMEOUT = (2, 4)
RETRYABLE = {0, 408, 429, 500, 502, 503, 504}


class LabSafetyError(Exception):
    pass


class KubeUnavailable(Exception):
    pass


class EvictionBlocked(Exception):
    pass


class KubernetesAdapter:
    def __init__(self, kubeconfig: Path, identity: Path) -> None:
        self.pin = json.loads(identity.read_text())
        config = yaml.safe_load(kubeconfig.read_text())
        context_name = f"kind-{LAB}"
        try:
            [context] = config["contexts"]
            [cluster] = config["clusters"]
            [user] = config["users"]
            endpoints = context["context"]
            connection = cluster["cluster"]
            credentials = user["user"]
            server = urlsplit(connection["server"])
            ca = b64decode(connection["certificate-authority-data"], validate=True)
            local = (
                config["current-context"] == context["name"] == endpoints["cluster"] == cluster["name"]
                == context_name
                and endpoints["user"] == user["name"]
                and set(connection) == {"server", "certificate-authority-data"}
                and set(credentials) == {"token"} and bool(credentials["token"])
                and server.scheme == "https" and server.hostname == "127.0.0.1" and server.port
                and server.netloc == f"127.0.0.1:{server.port}"
                and not (server.path or server.query or server.fragment)
                and self.pin["server"] == connection["server"]
                and self.pin["ca_sha256"] == sha256(ca).hexdigest()
                and bool(self.pin["namespace_uid"])
            )
        except (KeyError, TypeError, ValueError) as error:
            raise LabSafetyError("Invalid dedicated Kind credentials or identity pin") from error
        if not local:
            raise LabSafetyError("Credentials do not match the pinned loopback Kind lab")
        self.security = TemporaryDirectory(prefix="reliability-ca-")
        ca_file = Path(self.security.name) / "ca.crt"
        ca_file.write_bytes(ca)
        settings = client.Configuration()
        settings.host = self.pin["server"]
        settings.ssl_ca_cert = str(ca_file)
        settings.verify_ssl = True
        settings.proxy = None
        settings.retries = 0
        settings.api_key["authorization"] = credentials["token"]
        settings.api_key_prefix["authorization"] = "Bearer"
        self.client = client.ApiClient(settings)
        self.core = client.CoreV1Api(self.client)
        self.apps = client.AppsV1Api(self.client)
        self.policy = client.PolicyV1Api(self.client)
        self.metrics = client.CustomObjectsApi(self.client)

    def close(self) -> None:
        self.client.close()
        self.security.cleanup()

    @staticmethod
    def read(method, *args, **kwargs):
        for attempt in range(3):
            try:
                with kube_request("read"):
                    return method(*args, _request_timeout=TIMEOUT, **kwargs)
            except ApiException as error:
                if error.status == 404:
                    raise LabSafetyError("A required Kubernetes resource disappeared") from error
                if error.status not in RETRYABLE and error.status is not None:
                    raise LabSafetyError(f"Kubernetes read rejected with HTTP {error.status}") from error
            except (HTTPError, OSError):
                pass
            if attempt < 2:
                sleep(0.1 * (attempt + 1))
        raise KubeUnavailable("Kubernetes read retries exhausted")

    def validate_identity(self) -> None:
        namespace = self.read(self.core.read_namespace, "kube-system")
        if namespace.metadata.uid != self.pin["namespace_uid"]:
            raise LabSafetyError("Kind namespace UID differs from the bootstrap pin")

    def discover(self) -> dict:
        self.validate_identity()
        methods = {
            "nodes": self.core.list_node,
            "pods": self.core.list_pod_for_all_namespaces,
            "deployments": self.apps.list_deployment_for_all_namespaces,
            "replica_sets": self.apps.list_replica_set_for_all_namespaces,
            "namespaces": self.core.list_namespace,
            "pdbs": self.policy.list_pod_disruption_budget_for_all_namespaces,
        }
        return {
            name: self.client.sanitize_for_serialization(self.read(method).items)
            for name, method in methods.items()
        }

    def node_usage(self) -> dict | None:
        try:
            with kube_request("metrics"):
                rows = self.metrics.list_cluster_custom_object(
                    "metrics.k8s.io", "v1beta1", "nodes", _request_timeout=TIMEOUT,
                )["items"]
        except (ApiException, HTTPError, OSError):
            return None
        return {row["metadata"]["name"]: row["usage"] for row in rows}

    def events(self) -> list[dict]:
        self.validate_identity()
        events = []
        continuation = None
        for _ in range(20):
            page = self.read(self.core.list_event_for_all_namespaces, limit=250, _continue=continuation)
            events.extend(self.client.sanitize_for_serialization(page.items))
            continuation = page.metadata._continue
            if not continuation:
                return events
        raise KubeUnavailable("Kubernetes Event listing exceeded the local collection limit")

    def node(self, name: str) -> dict:
        if name not in WORKERS:
            raise LabSafetyError("Target is outside the dedicated Kind worker set")
        return self.client.sanitize_for_serialization(self.read(self.core.read_node, name))

    def mutate(self, method, *args, eviction: bool = False) -> None:
        self.validate_identity()
        try:
            with kube_request("evict" if eviction else "write", eviction=eviction):
                method(*args, _request_timeout=TIMEOUT)
        except ApiException as error:
            if error.status == 429 and eviction:
                raise EvictionBlocked("Eviction API returned HTTP 429") from error
            if error.status in RETRYABLE | {404, 409, 422, None}:
                raise KubeUnavailable(f"Kubernetes mutation requires reconciliation (HTTP {error.status})") from error
            raise LabSafetyError(f"Kubernetes mutation rejected with HTTP {error.status}") from error
        except (HTTPError, OSError) as error:
            raise KubeUnavailable("Kubernetes mutation response uncertain") from error

    def schedule(self, node: dict, uid: str, operation: str, *, cordon: bool) -> None:
        meta = node["metadata"]
        labels = meta.get("labels", {})
        if (meta["name"] not in WORKERS or meta["uid"] != uid or
                {"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master"} & labels.keys()):
            raise LabSafetyError("Worker identity or role changed")
        owner = meta.get("annotations", {}).get(OWNER)
        unschedulable = node.get("spec", {}).get("unschedulable", False)
        if cordon and (owner is not None or unschedulable):
            raise LabSafetyError("Target already has a cordon or maintenance owner")
        if not cordon and (owner != operation or not unschedulable):
            raise LabSafetyError("Cannot restore scheduling because cordon ownership or state changed")
        # A merge patch with resourceVersion is a compare-and-swap on the observed node.
        body = {
            "metadata": {
                "resourceVersion": meta["resourceVersion"],
                "annotations": {OWNER: operation if cordon else None},
            },
            "spec": {"unschedulable": cordon},
        }
        self.mutate(self.core.patch_node, meta["name"], body)

    def evict(self, pod: dict) -> None:
        meta = pod["metadata"]
        if meta["namespace"] != NAMESPACE or pod["spec"].get("nodeName") not in WORKERS:
            raise LabSafetyError("Pod is outside the approved Kind eviction scope")
        eviction = client.V1Eviction(
            api_version="policy/v1", kind="Eviction",
            metadata=client.V1ObjectMeta(name=meta["name"], namespace=meta["namespace"]),
            delete_options=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                uid=meta["uid"], resource_version=meta["resourceVersion"],
            )),
        )
        self.mutate(self.core.create_namespaced_pod_eviction, meta["name"], meta["namespace"], eviction,
                    eviction=True)

    def event(self, node: str, uid: str, reason: str, message: str) -> None:
        from datetime import datetime, timezone

        event = client.CoreV1Event(
            metadata=client.V1ObjectMeta(name=f"maintenance-{uuid4().hex}", namespace=NAMESPACE),
            involved_object=client.V1ObjectReference(
                api_version="v1", kind="ServiceAccount", name="maintenance", namespace=NAMESPACE,
            ),
            reason=reason, message=f"{message}; node={node} uid={uid}", type="Normal",
            source=client.V1EventSource(component="reliability-lab"),
            first_timestamp=datetime.now(timezone.utc), last_timestamp=datetime.now(timezone.utc), count=1,
        )
        self.mutate(self.core.create_namespaced_event, NAMESPACE, event)
