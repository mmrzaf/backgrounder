from __future__ import annotations

import builtins
import logging
from typing import Any

from .models import Job, JobStatus, Progress, TaskId
from .queue.base import JobQueue, QueueItem
from .resources import ResourceManager
from .store.base import JobStore
from .task import TaskRegistry
from .util.ids import new_job_id

logger = logging.getLogger("backgrounder.manager")


class JobManager:
    """High-level job management API."""

    def __init__(
        self,
        store: JobStore,
        queue: JobQueue,
        registry: TaskRegistry | None = None,
        resource_manager: ResourceManager | None = None,
        **kwargs: Any,
    ):
        self._store = store
        self._queue = queue
        self.registry = registry or TaskRegistry()
        self.resource_manager = resource_manager

    async def submit(
        self,
        task: TaskId,
        params: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        priority: int = 100,
        timeout_seconds: int | None = None,
    ) -> Job:
        """Submit new job."""
        if idempotency_key:
            existing = await self._store.get_by_idempotency(idempotency_key)
            if existing:
                logger.info(f"Returning existing job for idempotency key: {idempotency_key}")
                return existing

        job = Job(
            id=new_job_id(),
            task=task,
            params=params,
            idempotency_key=idempotency_key,
            timeout_seconds=timeout_seconds,
        )
        await self._store.create(job)

        await self._queue.put(QueueItem(job_id=job.id, priority=priority))

        logger.info(f"Submitted job {job.id} for task {task}")
        return job

    async def get(self, job_id: str) -> Job | None:
        """Get job by ID."""
        return await self._store.get(job_id)

    async def list(
        self,
        status: JobStatus | None = None,
        task: TaskId | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[builtins.list[Job], int]:
        """List jobs with filters."""
        return await self._store.list(status, task, limit, offset)

    async def cancel(self, job_id: str) -> Job | None:
        """Cancel job."""
        job = await self._store.request_cancel(job_id)
        if job:
            logger.info(f"Cancelled job {job_id}")
        return job

    async def retry(self, job_id: str, priority: int = 100) -> Job | None:
        """Retry failed job."""
        job = await self._store.get(job_id)
        if not job:
            return None

        if job.status not in (JobStatus.failed, JobStatus.canceled, JobStatus.timeout):
            logger.warning(f"Cannot retry job {job_id} with status {job.status}")
            return None

        job.status = JobStatus.queued
        job.error = None
        job.progress = Progress()
        job.retry_count += 1
        job.started_at = None
        job.finished_at = None
        job.cancel_requested = False
        job.mark_updated()

        await self._store.update_status_progress(
            job_id,
            status=JobStatus.queued,
            progress=job.progress,
        )

        # Re-queue
        await self._queue.put(
            QueueItem(job_id=job.id, priority=priority, retry_count=job.retry_count)
        )

        logger.info(f"Retrying job {job_id} (attempt {job.retry_count})")
        return job

    def store(self) -> JobStore:
        """Get store instance."""
        return self._store

    def queue(self) -> JobQueue:
        """Get queue instance."""
        return self._queue
