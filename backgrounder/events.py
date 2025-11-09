from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from .models import Stage


@dataclass
class ProgressEvent:
    """Event emitted during job processing."""

    etype: Literal["progress", "status", "error", "assets", "heartbeat"]
    job_id: str
    stage: Stage
    percent: int
    data: dict[str, Any] | None = None
    timestamp: str | None = None
