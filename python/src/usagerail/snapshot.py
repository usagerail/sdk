"""Saving the key snapshot to disk, only in long-running processes. Same as fileSnapshotStore in the Node SDK's node.ts.

The snapshot is written to the local temp directory with mode 600. The temp directory is shared by every user on the machine, and others can put files and symlinks in it;
reading a snapshot someone else planted would mean trusting their key hashes. So:
- files live in a subdirectory only the current user can enter (0700), checked before every use: a real directory, owned by us, no access for anyone else;
- read: open without following symlinks, check owner and mode on that same file descriptor, then read from it, so nothing can be swapped in between;
- write: create a randomly named file exclusively (O_EXCL), then rename it over the old one, so a process killed mid-write never leaves an unreadable snapshot.
File names tell apps apart by a hash of the appKey; the appKey itself never appears in a file name.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import secrets
import stat
import tempfile
from typing import Any

# Windows has no O_NOFOLLOW
_NO_FOLLOW = getattr(os, "O_NOFOLLOW", 0)


class FileSnapshotStore:
    def __init__(self, app_key: str, directory: str | None = None) -> None:
        self.directory = directory or _default_directory()
        name = hashlib.sha256(app_key.encode()).hexdigest()[:16]
        self.file = os.path.join(self.directory, f"snapshot-{name}.json")

    def load(self) -> dict[str, Any] | None:
        try:
            _assert_private_directory(self.directory)
        except FileNotFoundError:
            return None
        try:
            descriptor = os.open(self.file, os.O_RDONLY | _NO_FOLLOW)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as handle:
            stats = os.fstat(handle.fileno())
            if not stat.S_ISREG(stats.st_mode) or not _is_private(stats):
                raise PermissionError(f"{self.file} is not a private file owned by this user; ignoring it")
            snapshot = json.loads(handle.read())
        return snapshot if _is_snapshot(snapshot) else None

    def save(self, snapshot: dict[str, Any]) -> None:
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        _assert_private_directory(self.directory)
        temporary = os.path.join(self.directory, f".snapshot-{secrets.token_hex(8)}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NO_FOLLOW, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, separators=(",", ":"))
            os.replace(temporary, self.file)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError as error:
                if error.errno != errno.ENOENT:
                    raise
            raise


def _default_directory() -> str:
    owner = os.getuid() if hasattr(os, "getuid") else "snapshots"
    return os.path.join(tempfile.gettempdir(), f"usagerail-{owner}")


def _assert_private_directory(directory: str) -> None:
    stats = os.lstat(directory)
    if not stat.S_ISDIR(stats.st_mode) or not _is_private(stats):
        raise PermissionError(f"{directory} must be a directory that only this user can access")


def _is_private(stats: os.stat_result) -> bool:
    """Owned by the current user, with no permissions for group or others. Windows has no uid or Unix mode bits, so it isn't checked"""
    if not hasattr(os, "getuid"):
        return True
    return stats.st_uid == os.getuid() and stats.st_mode & 0o077 == 0


def _is_snapshot(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("saved_at"), str) and isinstance(value.get("keys"), list) and isinstance(value.get("consumers"), list)
