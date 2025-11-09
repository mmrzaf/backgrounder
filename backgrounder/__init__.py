"""
Backgrounder - Async job queue for FastAPI applications.

Usage:
    from backgrounder import setup_backgrounder, TaskRegistry, NoopRunner
    from backgrounder.resources import ResourceLimits

    registry = TaskRegistry()
    registry.add("test-task", NoopRunner())

    setup_backgrounder(
        app,
        db_path="./jobs.db",
        registry=registry,
        resource_limits=ResourceLimits(max_concurrent_jobs=5),
    )
"""

from .exceptions import (
    BackgrounderError,
    CancelledError,
    ResourceError,
    RunnerError,
    TimeoutError,
)
from .fastapi.lifecycle import setup_backgrounder
from .manager import JobManager
from .models import Assets, ErrorInfo, Job, JobStatus, Progress, Stage, TaskId
from .queue.base import JobQueue, QueueItem
from .queue.memory import MemoryQueue
from .resources import ResourceAllocation, ResourceLimits, ResourceManager
from .store.base import JobStore
from .store.sqlite import SqliteJobStore
from .task import NoopRunner, RunContext, SyncRunnerAdapter, TaskRegistry, TaskRunner
from .version import __version__
from .worker import Worker

# Conditional Redis import
try:
    from .queue.redis import RedisQueue

    __all_redis = ["RedisQueue"]
except ImportError:
    __all_redis = []

__all__ = [
    # Version
    "__version__",
    # Core
    "Job",
    "JobStatus",
    "Stage",
    "Progress",
    "ErrorInfo",
    "Assets",
    "TaskId",
    # Manager
    "JobManager",
    # Tasks
    "TaskRunner",
    "TaskRegistry",
    "RunContext",
    "NoopRunner",
    "SyncRunnerAdapter",
    # Store
    "JobStore",
    "SqliteJobStore",
    # Queue
    "JobQueue",
    "QueueItem",
    "MemoryQueue",
    # Resources
    "ResourceManager",
    "ResourceLimits",
    "ResourceAllocation",
    # Worker
    "Worker",
    # FastAPI
    "setup_backgrounder",
    # Exceptions
    "BackgrounderError",
    "CancelledError",
    "TimeoutError",
    "ResourceError",
    "RunnerError",
] + __all_redis
