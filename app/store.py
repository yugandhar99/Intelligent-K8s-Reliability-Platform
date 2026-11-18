from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import sqlite3

from .models import Cluster
from .scenarios import synthetic_cluster


class MaintenanceConflict(Exception):
    pass


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def transaction(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=10)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS cluster (id INTEGER PRIMARY KEY CHECK(id = 1), payload TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS plans (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, plan_id TEXT NOT NULL UNIQUE REFERENCES plans(id), payload TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS audit (sequence INTEGER PRIMARY KEY, operation_id TEXT NOT NULL REFERENCES operations(id), payload TEXT NOT NULL)")
            connection.execute("CREATE INDEX IF NOT EXISTS audit_operation ON audit(operation_id, sequence)")
            connection.execute(
                "INSERT OR IGNORE INTO cluster VALUES (1, ?)",
                (synthetic_cluster().model_dump_json(),),
            )

    @staticmethod
    def snapshot(connection: sqlite3.Connection) -> Cluster:
        row = connection.execute("SELECT payload FROM cluster WHERE id = 1").fetchone()
        return Cluster.model_validate_json(row[0])

    @staticmethod
    def check_synchronous_write(db: sqlite3.Connection, kind: str, plan_id: str) -> None:
        if db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'").fetchone():
            if db.execute("SELECT 1 FROM jobs WHERE kind = ? AND plan_id = ?", (kind, plan_id)).fetchone():
                raise MaintenanceConflict("This plan already has a durable job")
            if db.execute("SELECT 1 FROM job_owner").fetchone():
                raise MaintenanceConflict("A durable job owns the simulated cluster")
