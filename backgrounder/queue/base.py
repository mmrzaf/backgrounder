from __future__ import annotations

import abc
from dataclasses import dataclass


@dataclass
class QueueItem:
    """Item in job queue."""

    job_id: str
    priority: int = 100  # lower = higher priority
    retry_count: int = 0


class JobQueue(abc.ABC):
    """Abstract base for job queues."""

    @abc.abstractmethod
    async def put(self, item: QueueItem) -> None:
        """Add item to queue."""
        ...

    @abc.abstractmethod
    async def get(self) -> QueueItem:
        """Get next item from queue."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close queue."""
        ...

    @abc.abstractmethod
    def qsize(self) -> int:
        """Get queue size."""
        ...
