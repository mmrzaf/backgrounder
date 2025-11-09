from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from .base import JobQueue, QueueItem

logger = logging.getLogger("backgrounder.queue.redis")

try:
    import redis.asyncio as aioredis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("redis package not available")


class RedisQueue(JobQueue):
    """Redis-backed priority queue for distributed systems."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "backgrounder:queue",
        **redis_kwargs: Any,
    ):
        if not REDIS_AVAILABLE:
            raise ImportError("redis package required for RedisQueue")

        self._redis_url = redis_url
        self._queue_name = queue_name
        self._redis_kwargs = redis_kwargs
        self._redis: aioredis.Redis | None = None
        self._closed = False

    async def _ensure_redis(self) -> aioredis.Redis:
        """Ensure Redis connection."""
        if self._redis is None:
            self._redis = await aioredis.from_url(
                self._redis_url,
                decode_responses=True,
                **self._redis_kwargs,
            )
        return self._redis

    async def put(self, item: QueueItem) -> None:
        """Add item to Redis sorted set by priority."""
        if self._closed:
            raise RuntimeError("Queue is closed")

        redis = await self._ensure_redis()
        data = json.dumps(
            {
                "job_id": item.job_id,
                "priority": item.priority,
                "retry_count": item.retry_count,
            }
        )
        # Use priority as score (lower = higher priority)
        await redis.zadd(self._queue_name, {data: item.priority})

    async def get(self) -> QueueItem:
        """Get highest priority item (lowest score)."""
        redis = await self._ensure_redis()

        while not self._closed:
            # ZPOPMIN with blocking
            result = await redis.bzpopmin(self._queue_name, timeout=1)
            if result:
                _, data, _ = result
                item_dict = json.loads(data)
                return QueueItem(
                    job_id=item_dict["job_id"],
                    priority=item_dict["priority"],
                    retry_count=item_dict.get("retry_count", 0),
                )
            await asyncio.sleep(0.1)

        raise RuntimeError("Queue is closed")

    async def close(self) -> None:
        """Close Redis connection."""
        self._closed = True
        if self._redis:
            await self._redis.close()

    def qsize(self) -> int:
        """Get queue size (synchronous approximation)."""
        # Note: This is a limitation - Redis operations are async
        # Return 0 as placeholder
        return 0
