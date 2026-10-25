"""A few things the three framework adapters share."""

from __future__ import annotations

from typing import Any

from .engine import Engine, create_engine
from .snapshot import FileSnapshotStore


def engine_for(app_key: str, auth: str, snapshot: bool, options: dict[str, Any]) -> Engine[Any]:
    """A key snapshot only makes sense for apps using our keys; by default it's saved to the local temp directory"""
    store = FileSnapshotStore(app_key) if snapshot and auth == "api-key" and app_key else None
    return create_engine(app_key=app_key, auth=auth, snapshot_store=store, **options)
