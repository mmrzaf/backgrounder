from __future__ import annotations

import asyncio
import contextlib
import logging
import traceback
from datetime import UTC, datetime

from .events import ProgressEvent
from .exceptions import CancelledError
from .models import ErrorInfo, JobStatus, Progress, Stage
from .queue.base import JobQueue
from .resources import ResourceManager
from .store.base import JobStore
from .task import RunContext, TaskRegistry

logger = logging.getLogger("backgrounder.worker")


class Worker:
    """Job worker that processes queue items."""

    def __init__(
        self,
        queue: JobQueue,
        store: JobStore,
        registry: TaskRegistry,
        *,
        worker_id: str | None = None,
        on_event: callable | None = None,
        resource_manager: ResourceManager | None = None,
        heartbeat_interval: int = 30,
    ):
        self._queue = queue
        self._store = store
        self._registry = registry
        self._worker_id = worker_id or f"worker-{id(self)}"
        self._on_event = on_event
        self._resource_manager = resource_manager
        self._heartbeat_interval = heartbeat_interval

        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._current_job_id: str | None = None

    def start(self) -> asyncio.Task[None]:
        """Start worker."""
        if self._task and not self._task.done():
            return self._task

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name=f"worker-{self._worker_id}")
        logger.info(f"Worker {self._worker_id} started")
        return self._task
    async def stop(self, graceful: bool = True, grace_seconds: int = 30):
        self._stop_event.set()
        if graceful and self._current_job_id:
            await self._store.request_cancel(self._current_job_id)
            try:
                await asyncio.wait_for(self._task, timeout=grace_seconds)
            except asyncio.TimeoutError:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
        else:
            self._task.cancel()
            logger.info(f"Worker {self._worker_id} stopped")


    async def _run(self) -> None:
        """Main worker loop."""
        while not self._stop_event.is_set():
            try:
                # Get next job with timeout
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                await self._handle_job(item.job_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Worker loop error: {e}")
                await asyncio.sleep(0.1)

    async def _handle_job(self, job_id: str) -> None:
        """Handle single job execution."""
        self._current_job_id = job_id
        allocation = None

        try:
            job = await self._store.get(job_id)
            if not job:
                logger.warning(f"Job not found: {job_id}")
                return

            # Skip terminal jobs
            if job.status.is_terminal():
                logger.info(f"Skipping terminal job {job_id}: {job.status.value}")
                return

            # Get runner
            runner = self._registry.get(str(job.task))
            if not runner:
                await self._fail_job(
                    job_id,
                    ErrorInfo(
                        code="runner_not_found",
                        message=f"No runner registered for task: {job.task}",
                        stage=Stage.idle,
                    ),
                )
                return

            # Acquire resources
            if self._resource_manager:
                try:
                    # Default resource allocation
                    allocation = await self._resource_manager.acquire(
                        job_id,
                        memory_mb=job.params.get("memory_mb", 512),
                        cpu_percent=job.params.get("cpu_percent", 25),
                        gpu_memory_mb=job.params.get("gpu_memory_mb", 0),
                    )
                except Exception as e:
                    logger.error(f"Resource acquisition failed for {job_id}: {e}")
                    await self._fail_job(
                        job_id,
                        ErrorInfo(
                            code="resource_error",
                            message=str(e),
                            stage=Stage.idle,
                        ),
                    )
                    return

            # Mark as running
            await self._store.update_status_progress(
                job_id,
                status=JobStatus.running,
                progress=Progress(percent=0, stage=Stage.fetch),
                started_at=datetime.now(UTC),
                worker_id=self._worker_id,
            )

            # Create context
            ctx = self._create_context(job_id)

            # Execute with timeout
            timeout = job.timeout_seconds or 3600  # Default 1 hour
            try:
                await asyncio.wait_for(runner.execute(job, ctx), timeout=timeout)

                # Mark as done
                await self._store.update_status_progress(
                    job_id,
                    status=JobStatus.done,
                    progress=Progress(percent=100, stage=Stage.write),
                    finished_at=datetime.now(UTC),
                )

                if self._on_event:
                    self._on_event(
                        ProgressEvent(
                            etype="status",
                            job_id=job_id,
                            stage=Stage.write,
                            percent=100,
                            data={"status": "done"},
                            timestamp=datetime.now(UTC).isoformat(),
                        )
                    )

                logger.info(f"Job {job_id} completed successfully")

            except asyncio.TimeoutError:
                await self._fail_job(
                    job_id,
                    ErrorInfo(
                        code="timeout",
                        message=f"Job exceeded timeout of {timeout}s",
                        stage=Stage.infer,
                    ),
                    status=JobStatus.timeout,
                )

            except CancelledError:
                await self._store.update_status_progress(
                    job_id,
                    status=JobStatus.canceled,
                    progress=Progress(percent=0, stage=Stage.idle, message="Cancelled by user"),
                    finished_at=datetime.now(UTC),
                )

                if self._on_event:
                    self._on_event(
                        ProgressEvent(
                            etype="status",
                            job_id=job_id,
                            stage=Stage.idle,
                            percent=0,
                            data={"status": "canceled"},
                            timestamp=datetime.now(UTC).isoformat(),
                        )
                    )

                logger.info(f"Job {job_id} was cancelled")

            except Exception as e:
                tb = traceback.format_exc()
                await self._fail_job(
                    job_id,
                    ErrorInfo(
                        code="execution_error",
                        message=str(e),
                        stage=Stage.infer,
                        traceback=tb,
                    ),
                )

        finally:
            self._current_job_id = None

            # Release resources
            if allocation and self._resource_manager:
                await self._resource_manager.release(job_id)

    def _create_context(self, job_id: str) -> RunContext:
        """Create execution context for job."""

        async def save_assets(assets):
            await self._store.attach_assets(job_id, assets)

        async def set_progress(percent, stage, processed, total, message):
            await self._store.update_status_progress(
                job_id,
                progress=Progress(
                    percent=percent,
                    stage=stage,
                    processed_items=processed,
                    total_items=total,
                    message=message,
                ),
            )

        async def get_cancel_flag() -> bool:
            job = await self._store.get(job_id)
            return bool(job and job.cancel_requested)

        return RunContext(
            job_id=job_id,
            on_event=self._on_event,
            get_cancel_flag=get_cancel_flag,
            save_assets=save_assets,
            set_progress=set_progress,
        )

    async def _fail_job(
        self,
        job_id: str,
        error: ErrorInfo,
        status: JobStatus = JobStatus.failed,
    ) -> None:
        """Mark job as failed."""
        await self._store.mark_error(job_id, error, status)

        if self._on_event:
            self._on_event(
                ProgressEvent(
                    etype="error",
                    job_id=job_id,
                    stage=error.stage or Stage.idle,
                    percent=0,
                    data={"code": error.code, "message": error.message},
                    timestamp=datetime.now(UTC).isoformat(),
                )
            )

        logger.error(f"Job {job_id} failed: {error.code} - {error.message}")
