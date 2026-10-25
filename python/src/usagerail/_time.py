"""Conversion between ISO 8601 times and millisecond timestamps, formatted like JavaScript's Date (which the server and the Node SDK use)."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_iso_ms(value: object) -> float | None:
    """Returns None if it can't be parsed. No time zone means UTC (the server always includes Z)"""
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp() * 1000


def iso_from_ms(ms: float) -> str:
    """2026-10-20T12:00:10.000Z, same as new Date(ms).toISOString()"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
