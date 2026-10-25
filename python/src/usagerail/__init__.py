"""UsageRail's Python SDK: API keys, rate limits, quotas and usage metering inside your own API server.

FastAPI / Starlette use usagerail.fastapi, Flask uses usagerail.flask, Django uses usagerail.django.
For another framework, use create_engine: get a decision from begin() and call end() when the request finishes.
"""

from .engine import (
    MAX_TRACK_UNITS,
    Admission,
    ConsumerIdentity,
    Decision,
    Engine,
    Pending,
    Rejection,
    RejectionCode,
    RequestHandle,
    create_engine,
    read_api_key,
    rejection_body,
)
from .key_store import SnapshotStore
from .snapshot import FileSnapshotStore
from .transport import HttpTransport, Transport, TransportError

__all__ = [
    "MAX_TRACK_UNITS",
    "Admission",
    "ConsumerIdentity",
    "Decision",
    "Engine",
    "FileSnapshotStore",
    "HttpTransport",
    "Pending",
    "Rejection",
    "RejectionCode",
    "RequestHandle",
    "SnapshotStore",
    "Transport",
    "TransportError",
    "create_engine",
    "read_api_key",
    "rejection_body",
]
