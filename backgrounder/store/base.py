from __future__ import annotations

import abc
import builtins
from datetime import datetime

from ..models import Assets, ErrorInfo, Job, JobStatus, Progress, Stage, TaskId


class JobStore(abc.ABC):
    """Abstract base for job persistence."""

    @abc.abstractmethod
    async def create(self, job: Job) -> Job:
        """Create a new job."""
        ...

    @abc.abstractmethod
    async def get(self, job_id: str) -> Job | None:
        """Get job by ID."""
        ...

    @abc.abstractmethod
    async def get_by_idempotency(self, key: str) -> Job | None:
        """Get job by idempotency key."""
        ...

    @abc.abstractmethod
    async def list(
        self,
        status: JobStatus | None,
        task: TaskId | None,
        limit: int,
        offset: int,
    ) -> tuple[builtins.list[Job], int]:
        """List jobs with filters."""
        ...

    @abc.abstractmethod
    async def update_status_progress(
        self,
        job_id: str,
        *,
        status: JobStatus | None = None,
        progress: Progress | None = None,
        stage: Stage | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        message: str | None = None,
        worker_id: str | None = None,
    ) -> Job | None:
        """Update job status and progress."""
        ...

    @abc.abstractmethod
    async def attach_assets(self, job_id: str, assets: Assets) -> Job | None:
        """Attach output assets to job."""
        ...

    @abc.abstractmethod
    async def mark_error(
        self,
        job_id: str,
        err: ErrorInfo,
        status: JobStatus = JobStatus.failed,
    ) -> Job | None:
        """Mark job as failed with error info."""
        ...

    @abc.abstractmethod
    async def request_cancel(self, job_id: str) -> Job | None:
        """Request job cancellation."""
        ...

    @abc.abstractmethod
    async def recover_stale_jobs(self, max_age_seconds: int = 3600) -> int:
        """Recover jobs stuck in running state."""
        ...

    @abc.abstractmethod
    async def cleanup_old_jobs(self, days: int = 7) -> int:
        """Delete old completed jobs."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close store connections."""
        ...
