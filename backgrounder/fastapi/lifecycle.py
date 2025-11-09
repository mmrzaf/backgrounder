from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..manager import JobManager
from ..queue.base import JobQueue
from ..queue.memory import MemoryQueue
from ..resources import ResourceLimits, ResourceManager
from ..store.base import JobStore
from ..store.sqlite import SqliteJobStore
from ..task import TaskRegistry
from ..worker import Worker

logger = logging.getLogger("backgrounder.lifecycle")

MANAGER_STATE_KEY = "backgrounder_manager"
WORKER_STATE_KEY = "backgrounder_worker"
WORKER_TASK_STATE_KEY = "backgrounder_worker_task"


def setup_backgrounder(
    app: FastAPI,
    *,
    db_path: str | None = None,
    store: JobStore | None = None,
    queue: JobQueue | None = None,
    registry: TaskRegistry | None = None,
    resource_limits: ResourceLimits | None = None,
    include_router: bool = True,
    prefix: str = "/api/v1",
    worker_count: int = 1,
    **manager_kwargs,
) -> JobManager:
    """Setup backgrounder in FastAPI application."""
    # Initialize store
    if store is None:
        if not db_path:
            raise ValueError("Provide `store` or `db_path`")
        store = SqliteJobStore(db_path=db_path)

    # Initialize queue
    if queue is None:
        queue = MemoryQueue()

    # Initialize resource manager
    resource_manager = None
    if resource_limits:
        resource_manager = ResourceManager(resource_limits)

    # Create manager
    mgr = JobManager(
        store=store,
        queue=queue,
        registry=registry,
        resource_manager=resource_manager,
        **manager_kwargs,
    )
    setattr(app.state, MANAGER_STATE_KEY, mgr)

    # Setup lifecycle
    @asynccontextmanager
    async def _lifespan(app_: FastAPI):
        # Startup
        if hasattr(store, "_ensure"):
            await store._ensure()

        # Recover stale jobs
        if hasattr(store, "recover_stale_jobs"):
            try:
                recovered = await store.recover_stale_jobs(max_age_seconds=3600)
                if recovered > 0:
                    logger.warning(f"Recovered {recovered} stale jobs on startup")
            except Exception:
                logger.exception("Failed to recover stale jobs")

        # Start workers
        workers = []
        for i in range(worker_count):
            worker = Worker(
                queue=mgr.queue(),
                store=mgr.store(),
                registry=mgr.registry,
                worker_id=f"worker-{i}",
                resource_manager=resource_manager,
            )
            task = worker.start()
            workers.append((worker, task))

        setattr(app_.state, WORKER_STATE_KEY, workers)
        logger.info(f"Started {worker_count} workers")

        try:
            yield
        finally:
            # Shutdown
            logger.info("Shutting down backgrounder...")

            # Stop workers
            for worker, task in workers:
                try:
                    await worker.stop()
                except Exception:
                    logger.exception("Failed to stop worker")

            # Close queue
            try:
                await queue.close()
            except Exception:
                logger.exception("Failed to close queue")

            # Close store
            try:
                await store.close()
            except Exception:
                logger.exception("Failed to close store")

            logger.info("Backgrounder shutdown complete")

    # Compose with existing lifespan
    if app.router.lifespan_context is None:
        app.router.lifespan_context = _lifespan
    else:
        existing = app.router.lifespan_context

        @asynccontextmanager
        async def _composed(app_: FastAPI):
            async with existing(app_):
                async with _lifespan(app_):
                    yield

        app.router.lifespan_context = _composed

    # Include router
    if include_router:
        from .router import get_router

        app.include_router(get_router(), prefix=prefix)

    return mgr
