from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass, field

from .base import JobQueue, QueueItem


@dataclass(order=True)
class PriorityItem:
    """Priority queue item wrapper."""

    priority: int
    counter: int
    item: QueueItem = field(compare=False)


class MemoryQueue(JobQueue):
    """Priority queue with fast and normal lanes."""

    def __init__(self) -> None:
        self._heap: list[PriorityItem] = []
        self._counter = 0
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)
        self._closed = False

    async def put(self, item: QueueItem) -> None:
        """Add item to priority queue."""
        if self._closed:
            raise RuntimeError("Queue is closed")

        async with self._not_empty:
            # Use counter to maintain FIFO for same priority
            heapq.heappush(self._heap, PriorityItem(item.priority, self._counter, item))
            self._counter += 1
            self._not_empty.notify()

    async def get(self) -> QueueItem:
        """Get highest priority item."""
        async with self._not_empty:
            while not self._heap and not self._closed:
                await self._not_empty.wait()

            if self._closed and not self._heap:
                raise RuntimeError("Queue is closed")

            priority_item = heapq.heappop(self._heap)
            return priority_item.item

    async def close(self) -> None:
        """Close queue."""
        async with self._not_empty:
            self._closed = True
            self._not_empty.notify_all()

    def qsize(self) -> int:
        """Get queue size."""
        return len(self._heap)
