from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, NewType

from .util.time import now_utc

TaskId = NewType("TaskId", str)

TASK_ID_PATTERN = r"^[a-z0-9][a-z0-9._:-]{0,127}$"


class JobStatus(str, Enum):
    """Job lifecycle states."""

    queued = "queued"
    running = "running"
    done = "done"
    failed = "failed"
    cancel_requested = "cancel_requested"
    canceled = "canceled"
    timeout = "timeout"

    def is_terminal(self) -> bool:
        """Check if status is terminal (no further processing)."""
        return self in (self.done, self.failed, self.canceled, self.timeout)

    def is_active(self) -> bool:
        """Check if job is actively processing."""
        return self in (self.queued, self.running, self.cancel_requested)


class Stage(str, Enum):
    """Processing stages."""

    idle = "idle"
    fetch = "fetch"
    preprocess = "preprocess"
    infer = "infer"
    postprocess = "postprocess"
    write = "write"
    cleanup = "cleanup"


@dataclass
class Progress:
    """Job progress tracking."""

    percent: int = 0
    stage: Stage = Stage.idle
    processed_items: int = 0
    total_items: int = 0
    message: str | None = None

    def update(
        self,
        percent: int | None = None,
        stage: Stage | None = None,
        processed: int | None = None,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        """Update progress fields."""
        if percent is not None:
            self.percent = max(0, min(100, percent))
        if stage is not None:
            self.stage = stage
        if processed is not None:
            self.processed_items = processed
        if total is not None:
            self.total_items = total
        if message is not None:
            self.message = message


@dataclass
class ErrorInfo:
    """Error details."""

    code: str
    message: str
    stage: Stage | None = None
    details: dict[str, Any] = field(default_factory=dict)
    traceback: str | None = None


@dataclass
class Assets:
    """Output assets from job execution."""

    cog: dict[str, Any] | None = None
    geojson: dict[str, Any] | None = None
    mbtiles: dict[str, Any] | None = None
    stac: dict[str, Any] | None = None
    custom: dict[str, Any] = field(default_factory=dict)


@dataclass
class Job:
    """Core job entity."""

    id: str
    task: TaskId
    params: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None

    status: JobStatus = JobStatus.queued
    progress: Progress = field(default_factory=Progress)
    error: ErrorInfo | None = None
    assets: Assets = field(default_factory=Assets)

    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    cancel_requested: bool = False
    retry_count: int = 0
    timeout_seconds: int | None = None

    worker_id: str | None = None  # Track which worker is processing

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict."""
        d = asdict(self)
        d["task"] = str(self.task)
        d["status"] = self.status.value
        d["progress"]["stage"] = self.progress.stage.value
        if self.error and self.error.stage:
            d["error"]["stage"] = self.error.stage.value
        return d

    def mark_updated(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = now_utc()
