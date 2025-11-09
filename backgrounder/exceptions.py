from __future__ import annotations


class BackgrounderError(Exception):
    """Base exception for backgrounder library."""

    pass


class CancelledError(BackgrounderError):
    """Raised when job is cancelled."""

    pass


class TimeoutError(BackgrounderError):
    """Raised when job exceeds timeout."""

    pass


class ResourceError(BackgrounderError):
    """Raised when resource allocation fails."""

    pass


class RunnerError(BackgrounderError):
    """Raised when task runner fails."""

    pass
