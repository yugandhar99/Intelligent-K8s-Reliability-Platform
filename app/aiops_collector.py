"""Periodically poll the dedicated lab without delaying job execution."""

from contextlib import nullcontext
import sqlite3
from threading import Event, Thread

from .aiops import IncidentService
from .kube_adapter import KubeUnavailable, LabSafetyError
from .service import logger


class EventCollector:
    def __init__(self, service: IncidentService, interval: float, *, telemetry=None) -> None:
        self.service, self.interval = service, interval
        self.telemetry = telemetry
        self.stop = Event()
        self.thread = Thread(target=self.run, name="aiops-event-collector", daemon=True)

    def run(self) -> None:
        with self.telemetry.bind() if self.telemetry else nullcontext():
            self.poll()

    def poll(self) -> None:
        while not self.stop.wait(self.interval):
            try:
                self.service.collect()
            except (KubeUnavailable, LabSafetyError, sqlite3.OperationalError) as error:
                logger.warning("aiops.collection_failed", extra={"fields": {"error": type(error).__name__}})

    def close(self) -> None:
        self.stop.set()
        self.thread.join()
