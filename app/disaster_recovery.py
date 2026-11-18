"""Copy and verify a simulator database without replacing an existing file."""

import argparse
from contextlib import closing
import os
from pathlib import Path
import sqlite3
from tempfile import NamedTemporaryFile

from .store import Store


def verify(db: sqlite3.Connection) -> None:
    if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
        raise ValueError("SQLite integrity check failed")
    if db.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("SQLite foreign key check failed")
    Store.snapshot(db)


def copy_database(source: Path, destination: Path, *, restore: bool = False) -> None:
    """Publish a verified backup or restore to a new path on the local filesystem."""
    for candidate in (destination, *(Path(f"{destination}{suffix}") for suffix in ("-wal", "-shm", "-journal"))):
        if os.path.lexists(candidate):
            raise FileExistsError(candidate)
    with NamedTemporaryFile(dir=destination.parent, prefix=".maintenance-", delete=False) as temporary:
        scratch = Path(temporary.name)
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as original:
            with closing(sqlite3.connect(scratch)) as copy:
                original.backup(copy)
                verify(copy)
                if restore and copy.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
                ).fetchone():
                    with copy:
                        copy.execute("UPDATE job_workers SET expires = 0")
                        copy.execute("UPDATE jobs SET expiry = 0 WHERE worker IS NOT NULL")
                    verify(copy)
        with scratch.open("rb") as artifact:
            os.fsync(artifact.fileno())
        os.link(scratch, destination)
        directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        scratch.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("backup", "restore"))
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        copy_database(args.source, args.destination, restore=args.action == "restore")
    except (OSError, sqlite3.Error, ValueError) as error:
        parser.exit(1, f"{args.action} failed: {error}\n")
    print(f"Verified {args.action} at {args.destination}")


if __name__ == "__main__":
    main()
