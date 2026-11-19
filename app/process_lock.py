"""Optionally reserve exclusive application ownership of a local SQLite file."""

from contextlib import contextmanager
from pathlib import Path


@contextmanager
def exclusive_database(path: Path, *, enabled: bool):
    if not enabled:
        yield
        return

    import fcntl

    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keep the inode on disk so subsequent processes contend on the same lock.
    with path.with_name(f"{path.name}.owner.lock").open("a+b") as descriptor:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                "Another application process owns this SQLite database"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
