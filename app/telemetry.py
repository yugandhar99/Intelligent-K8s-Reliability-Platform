"""Bounded metric labels and explicit tracing across application entry points."""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import re
from time import monotonic

from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

import logging

logger = logging.getLogger("maintenance")

CURRENT = ContextVar("maintenance_telemetry", default=None)
IDENTITIES = ContextVar("maintenance_log_ids", default={})
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
WALL_BUCKETS = (0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600)
PROVIDERS = ("mock", "openai", "anthropic", "gemini")
RESULTS = ("ok", "blocked", "uncertain", "error")


class Telemetry:
    def __init__(self, mode, objectives, recovery_deadline, *, endpoint=None, processor=None):
        self.mode = mode
        self.recovery_deadline = recovery_deadline
        self.registry = CollectorRegistry()
        self.provider = TracerProvider(resource=Resource.create({
            "service.name": f"reliability-{mode}", "deployment.environment.name": "local",
        }), shutdown_on_exit=False)
        if endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            self.provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, timeout=3)))
        if processor:
            self.provider.add_span_processor(processor)
        self.tracer = self.provider.get_tracer("reliability")

        def counter(name, help_text, *labels):
            return Counter(f"reliability_{name}", help_text, ("mode", *labels), registry=self.registry)

        def histogram(name, help_text, labels, buckets=LATENCY_BUCKETS):
            return Histogram(f"reliability_{name}_seconds", help_text, ("mode", *labels),
                             buckets=buckets, registry=self.registry)

        self.http = counter("http_requests", "Completed API requests", "method", "route", "status_class")
        self.http_time = histogram("http_request_duration", "API request wall time", ("method", "route", "status_class"))
        self.sync = histogram("sync_maintenance_duration", "Synchronous simulation maintenance wall time",
                              ("workflow", "outcome"), WALL_BUCKETS)
        self.heartbeat = counter("worker_heartbeats", "Successful worker heartbeat writes")
        self.leases = counter("expired_leases", "Expired leases observed for reconciliation")
        self.recovery = histogram("recovery_duration", "Seconds from lease expiry to reconciliation",
                                  ("outcome",), WALL_BUCKETS)
        self.evictions = counter("eviction_attempts", "Observed eviction requests or simulated attempts", "outcome")
        self.eviction_retry = counter("eviction_retries", "Eviction retry decisions")
        self.kube_time = histogram("kubernetes_request_duration", "Kubernetes API attempt wall time",
                                   ("verb", "outcome"))
        self.ai_time = histogram("ai_attempt_duration", "AI provider attempts including failed calls",
                                 ("provider", "outcome"))
        self.ai_rejected = counter("ai_output_rejections", "Provider output rejected by deterministic validation", "provider")
        self.approval_rejected = counter("approval_rejections", "Rejected approval requests", "workflow", "reason")
        self.approval_time = histogram("live_approval_duration", "Elapsed from live plan creation to human approval",
                                       (), WALL_BUCKETS)
        self.objectives = Gauge("reliability_slo_target_ratio", "Configured good-event fraction",
                                ("mode", "slo"), registry=self.registry)
        for objective, target in objectives.items():
            self.objectives.labels(mode, objective).set(target)
        for result in RESULTS:
            self.evictions.labels(mode, result)
        for provider in PROVIDERS:
            for outcome in ("ok", "error"):
                self.ai_time.labels(mode, provider, outcome)
        self.heartbeat.labels(mode)
        self.leases.labels(mode)
        self.eviction_retry.labels(mode)
        for outcome in ("good", "late", "failed"):
            self.recovery.labels(mode, outcome)
        for workflow in ("single", "batch"):
            for outcome in ("succeeded", "failed", "rejected"):
                self.sync.labels("simulation", workflow, outcome)

    @contextmanager
    def bind(self):
        token = CURRENT.set(self)
        try:
            yield
        finally:
            CURRENT.reset(token)


@contextmanager
def span(name, *, attributes=None, origin=None, kind=SpanKind.INTERNAL, **identifiers):
    telemetry = CURRENT.get()
    if telemetry is None:
        yield None
        return
    token = IDENTITIES.set({**IDENTITIES.get(), **identifiers})
    try:
        with telemetry.tracer.start_as_current_span(name, context=origin, kind=kind, attributes={
            **(attributes or {}), **identifiers,
        }, record_exception=False, set_status_on_exception=False) as current:
            try:
                yield current
            except BaseException:
                current.set_status(Status(StatusCode.ERROR))
                raise
    finally:
        IDENTITIES.reset(token)


def traced(name, *, argument=None, identifier=None):
    """Instrument synchronous domain operations with an optional local identifier."""
    def decorate(function):
        @wraps(function)
        def wrapped(self, *args, **kwargs):
            value = args[argument] if identifier and len(args) > argument else None
            ids = {identifier: value} if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value) else {}
            with span(name, **ids):
                return function(self, *args, **kwargs)
        return wrapped
    return decorate


def trace_fields():
    context = trace.get_current_span().get_span_context()
    return {**IDENTITIES.get(), **({
        "trace_id": format(context.trace_id, "032x"), "span_id": format(context.span_id, "016x"),
    } if context.is_valid else {})}


def record_eviction(mode, outcome="ok", *, retry=False):
    if telemetry := CURRENT.get():
        telemetry.evictions.labels(mode, outcome).inc()
        if retry:
            telemetry.eviction_retry.labels(mode).inc()


class HTTPMonitoring:
    def __init__(self, app, telemetry):
        self.app, self.telemetry = app, telemetry

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/metrics":
            await self.app(scope, receive, send)
            return
        method = scope["method"]
        method = method if method in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} else "OTHER"
        headers = {key.decode(): value.decode("latin-1") for key, value in scope.get("headers", [])
                   if key == b"traceparent"}
        status = 500

        async def track(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        started = monotonic()
        telemetry = self.telemetry
        with telemetry.bind(), span("http.request", kind=SpanKind.SERVER,
                                    origin=propagate.extract(headers), attributes={"http.request.method": method}) as current:
            try:
                await self.app(scope, receive, track)
            finally:
                route = getattr(scope.get("route"), "path", "unmatched")
                status_class = f"{status // 100}xx" if 100 <= status < 600 else "other"
                values = (telemetry.mode, method, route, status_class)
                telemetry.http.labels(*values).inc()
                telemetry.http_time.labels(*values).observe(monotonic() - started)
                if current:
                    current.update_name(f"{method} {route}")
                    current.set_attribute("http.route", route)
                    current.set_attribute("http.response.status_code", status)
                    if status >= 500:
                        current.set_status(Status(StatusCode.ERROR))
                workflow = {"/kubernetes/maintenance/execute": "kubernetes",
                            "/aiops/recommendations/{recommendation}/approve": "aiops"}.get(route)
                if workflow and status >= 400:
                    reason = ("auth" if status in {401, 403} else "validation" if status == 422
                              else "safety" if status < 500 else "unavailable")
                    telemetry.approval_rejected.labels(telemetry.mode, workflow, reason).inc()
                if route not in {"/health", "unmatched"}:
                    logger.info("http.completed", extra={"fields": {"route": route, "status": status, "method": method}})


def sync_operation(workflow):
    def decorate(function):
        @wraps(function)
        def wrapped(self, *args, **kwargs):
            telemetry = CURRENT.get()
            if telemetry is None:
                return function(self, *args, **kwargs)
            started = monotonic()
            outcome = "failed"
            with span("maintenance.execute", attributes={"maintenance.workflow": workflow}) as current:
                try:
                    result = function(self, *args, **kwargs)
                    current.set_attribute("operation_id", result.id)
                    outcome = result.state.value
                    return result
                finally:
                    telemetry.sync.labels("simulation", workflow, outcome).observe(monotonic() - started)
        return wrapped
    return decorate


@contextmanager
def kube_request(verb, *, eviction=False):
    telemetry = CURRENT.get()
    if telemetry is None:
        yield
        return
    started = monotonic()
    outcome = "ok"
    with span("kubernetes.request", kind=SpanKind.CLIENT, attributes={"kubernetes.verb": verb}) as current:
        try:
            yield
        except Exception as error:
            status = getattr(error, "status", None)
            outcome = ("blocked" if eviction and status == 429 else
                       "uncertain" if status in {None, 0, 408, 409, 500, 502, 503, 504} else "error")
            raise
        finally:
            current.set_attribute("kubernetes.outcome", outcome)
            telemetry.kube_time.labels("kubernetes", verb, outcome).observe(monotonic() - started)
            if eviction:
                record_eviction("kubernetes", outcome)


def simulated_eviction_event(event, details):
    if event == "pod.evicted":
        record_eviction("simulation")
    elif event == "api.retry_scheduled" and details.get("action") == "eviction":
        record_eviction("simulation", "uncertain", retry=True)
