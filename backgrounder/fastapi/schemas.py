from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, Field, StringConstraints

from ..models import TASK_ID_PATTERN, JobStatus, Stage

TaskIdStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=128,
        pattern=TASK_ID_PATTERN,
    ),
]


class SubmitJobRequest(BaseModel):
    """Request to submit a new job."""

    task: TaskIdStr
    params: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    priority: int = Field(100, ge=0, le=1000)
    timeout_seconds: int | None = Field(None, ge=1, le=86400)


class SubmitJobResponse(BaseModel):
    """Response after submitting a job."""

    job_id: str
    status: JobStatus
    links: dict[str, str]


class ProgressDetail(BaseModel):
    """Progress information."""

    percent: int = 0
    stage: Stage = Stage.idle
    processed_items: int = 0
    total_items: int = 0
    message: str | None = None


class ErrorInfo(BaseModel):
    """Error information."""

    code: str
    message: str
    stage: Stage | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class AssetRef(BaseModel):
    """Asset reference."""

    href: str
    extra: dict[str, Any] = Field(default_factory=dict)


class JobAssetsResponse(BaseModel):
    """Job output assets."""

    cog: dict[str, Any] | None = None
    geojson: dict[str, Any] | None = None
    mbtiles: dict[str, Any] | None = None
    stac: dict[str, Any] | None = None
    custom: dict[str, Any] = Field(default_factory=dict)


class JobStatusResponse(BaseModel):
    """Job status response."""

    job_id: str
    status: JobStatus
    progress: ProgressDetail
    task: TaskIdStr
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: ErrorInfo | None = None
    retry_count: int = 0


class ListJobsResponse(BaseModel):
    """List of jobs with pagination."""

    items: list[JobStatusResponse]
    total: int
    limit: int
    offset: int


class ResourceUsageResponse(BaseModel):
    """Resource usage information."""

    active_jobs: int
    max_concurrent: int
    memory_mb: int
    cpu_percent: int
    gpu_memory_mb: int
    disk_mb: int
