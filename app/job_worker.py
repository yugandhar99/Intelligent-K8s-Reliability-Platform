"""Run fenced maintenance steps on a background thread."""

from contextlib import nullcontext
import sqlite3
from threading import Event, Thread
from uuid import uuid4

from .jobs import JobService
from .service import logger
from .telemetry import span


class JobWorker:
    def __init__(self, service: JobService, interval: float = 0.1, *, telemetry=None) -> None:
        self.service = service
        self.telemetry = telemetry
        self.interval = interval
        self.id = uuid4().hex
        self.stop = Event()
        self.thread = Thread(target=self.run, name="maintenance-worker", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        self.thread.join()

    def run(self) -> None:
        with self.telemetry.bind() if self.telemetry else nullcontext():
            self.process()

    def process(self) -> None:
        try:
            while not self.stop.is_set():
                try:
                    self.service.heartbeat(self.id)
                    lease = self.service.claim(self.id)
                    if self.telemetry:
                        self.telemetry.heartbeat.labels(self.telemetry.mode).inc()
                    if lease:
                        with span("maintenance.worker.step", operation_id=lease.job_id):
                            self.service.run(lease)
                except sqlite3.OperationalError:
                    logger.warning("worker.storage_unavailable", extra={"fields": {"worker": self.id}})
                self.stop.wait(self.interval)
        finally:
            try:
                self.service.release(self.id)
            except sqlite3.OperationalError:
                logger.warning("worker.release_failed", extra={"fields": {"worker": self.id}})
