"""Run fenced maintenance steps on a background thread."""

import sqlite3
from threading import Event, Thread
from uuid import uuid4

from .jobs import JobService
from .service import logger


class JobWorker:
    def __init__(self, service: JobService, interval: float = 0.1) -> None:
        self.service = service
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
        try:
            while not self.stop.is_set():
                try:
                    self.service.heartbeat(self.id)
                    lease = self.service.claim(self.id)
                    if lease:
                        self.service.run(lease)
                except sqlite3.OperationalError:
                    logger.warning("worker.storage_unavailable", extra={"fields": {"worker": self.id}})
                self.stop.wait(self.interval)
        finally:
            try:
                self.service.release(self.id)
            except sqlite3.OperationalError:
                logger.warning("worker.release_failed", extra={"fields": {"worker": self.id}})
