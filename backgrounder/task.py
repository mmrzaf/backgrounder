from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from .events import ProgressEvent
from .exceptions import CancelledError
from .models import Assets, Job, Stage

logger = logging.getLogger("backgrounder.task")


class TaskRunner(Protocol):
    """Protocol for task execution."""

    async def execute(self, job: Job, ctx: RunContext) -> Assets:
        """Execute task and return assets."""
        ...


@dataclass
class RunContext:
    """Context provided to task runners."""

    job_id: str
    on_event: Callable[[ProgressEvent], None] | None
    get_cancel_flag: Callable[[], Awaitable[bool]]
    save_assets: Callable[[Assets], Awaitable[None]]
    set_progress: Callable[[int, Stage, int, int, str | None], Awaitable[None]]

    async def progress(
        self,
        percent: int,
        stage: Stage,
        processed: int = 0,
        total: int = 0,
        message: str | None = None,
    ) -> None:
        """Update progress."""
        await self.set_progress(percent, stage, processed, total, message)
        if self.on_event:
            self.on_event(
                ProgressEvent(
                    etype="progress",
                    job_id=self.job_id,
                    stage=stage,
                    percent=percent,
                    data={"processed": processed, "total": total, "message": message},
                    timestamp=datetime.now(UTC).isoformat(),
                )
            )

    async def attach_assets(self, assets: Assets) -> None:
        """Attach output assets."""
        await self.save_assets(assets)
        if self.on_event:
            self.on_event(
                ProgressEvent(
                    etype="assets",
                    job_id=self.job_id,
                    stage=Stage.write,
                    percent=100,
                    data={"assets": True},
                    timestamp=datetime.now(UTC).isoformat(),
                )
            )

    async def check_canceled(self) -> None:
        """Check if job is cancelled."""
        if await self.get_cancel_flag():
            raise CancelledError("Job cancellation requested")

    def now(self) -> datetime:
        """Get current UTC time."""
        return datetime.now(UTC)


class TaskRegistry:
    """Registry of task runners."""

    def __init__(self) -> None:
        self._map: dict[str, TaskRunner] = {}

    def add(self, task_type: str, runner: TaskRunner) -> None:
        """Register a task runner."""
        self._map[task_type] = runner
        logger.info(f"Registered task runner: {task_type}")

    def get(self, task_type: str) -> TaskRunner | None:
        """Get task runner by type."""
        return self._map.get(task_type)

    def list_tasks(self) -> list[str]:
        """List all registered tasks."""
        return list(self._map.keys())


class SyncRunnerAdapter(TaskRunner):
    """Adapter to run synchronous functions as async tasks."""

    def __init__(
        self,
        fn: Callable[[Job, RunContext], Assets],
        *,
        name: str | None = None,
    ):
        self.fn = fn
        self.name = name or getattr(fn, "__name__", "sync_runner")

    async def execute(self, job: Job, ctx: RunContext) -> Assets:
        """Execute synchronous function in thread pool."""
        return await asyncio.to_thread(self.fn, job, ctx)


class NoopRunner(TaskRunner):
    """No-op runner for testing."""

    def __init__(self, sleep_ms: int = 25, stages: int = 5):
        self.sleep_ms = sleep_ms
        self.stages = stages

    async def execute(self, job: Job, ctx: RunContext) -> Assets:
        """Simulate work across stages."""
        all_stages = [Stage.fetch, Stage.preprocess, Stage.infer, Stage.postprocess, Stage.write]
        stages = all_stages[: self.stages]

        steps_per_stage = 100 // len(stages)
        step = 0

        for st in stages:
            for i in range(steps_per_stage):
                await ctx.check_canceled()
                await asyncio.sleep(self.sleep_ms / 1000.0)
                step += 1
                await ctx.progress(step, st, step, 100)

        assets = Assets(
            cog={"href": f"/assets/{job.id}/mask.tif", "size_mb": 42},
            geojson={"href": f"/assets/{job.id}/features.geojson", "features": 100},
            mbtiles={"href": f"/assets/{job.id}/result.mbtiles", "size_mb": 15},
            stac={"href": f"/assets/{job.id}/stac/item.json", "version": "1.0.0"},
        )
        await ctx.attach_assets(assets)
        return assets
