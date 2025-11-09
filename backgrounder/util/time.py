from __future__ import annotations

from datetime import UTC, datetime, timedelta

ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"


def now_utc() -> datetime:
    """Get current UTC time with timezone."""
    return datetime.now(UTC)


def to_utc(dt: datetime | None) -> datetime | None:
    """Convert datetime to UTC, handling naive datetimes."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(dt: datetime | None) -> str | None:
    """Convert datetime to ISO format string."""
    if dt is None:
        return None
    return to_utc(dt).isoformat()


def parse_iso(s: str | None) -> datetime | None:
    """Parse ISO format string to datetime."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return to_utc(dt)
    except (ValueError, TypeError):
        return None


def time_since(dt: datetime) -> timedelta:
    """Get time elapsed since datetime."""
    return now_utc() - to_utc(dt)


def is_stale(dt: datetime, max_age: timedelta) -> bool:
    """Check if datetime is older than max_age."""
    return time_since(dt) > max_age
