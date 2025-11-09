from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from .exceptions import ResourceError

logger = logging.getLogger("backgrounder.resources")


@dataclass
class ResourceLimits:
    """Resource limits for job execution."""

    max_memory_mb: int | None = None
    max_cpu_percent: int | None = None
    max_gpu_memory_mb: int | None = None
    max_concurrent_jobs: int = 10
    max_disk_mb: int | None = None


@dataclass
class ResourceAllocation:
    """Allocated resources for a job."""

    job_id: str
    memory_mb: int = 0
    cpu_percent: int = 0
    gpu_memory_mb: int = 0
    disk_mb: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class ResourceManager:
    """Manages resource allocation and limits for job execution."""

    def __init__(self, limits: ResourceLimits | None = None):
        self.limits = limits or ResourceLimits()
        self._lock = asyncio.Lock()
        self._allocations: dict[str, ResourceAllocation] = {}
        self._semaphore = asyncio.Semaphore(self.limits.max_concurrent_jobs)

    async def acquire(
        self,
        job_id: str,
        memory_mb: int = 0,
        cpu_percent: int = 0,
        gpu_memory_mb: int = 0,
        disk_mb: int = 0,
    ) -> ResourceAllocation:
        """Acquire resources for a job. Blocks if resources unavailable."""
        await self._semaphore.acquire()

        async with self._lock:
            # Check limits
            total_memory = sum(a.memory_mb for a in self._allocations.values()) + memory_mb
            total_cpu = sum(a.cpu_percent for a in self._allocations.values()) + cpu_percent
            total_gpu = sum(a.gpu_memory_mb for a in self._allocations.values()) + gpu_memory_mb
            total_disk = sum(a.disk_mb for a in self._allocations.values()) + disk_mb

            if self.limits.max_memory_mb and total_memory > self.limits.max_memory_mb:
                self._semaphore.release()
                raise ResourceError(
                    f"Memory limit exceeded: {total_memory}MB > {self.limits.max_memory_mb}MB"
                )

            if self.limits.max_cpu_percent and total_cpu > self.limits.max_cpu_percent:
                self._semaphore.release()
                raise ResourceError(
                    f"CPU limit exceeded: {total_cpu}% > {self.limits.max_cpu_percent}%"
                )

            if self.limits.max_gpu_memory_mb and total_gpu > self.limits.max_gpu_memory_mb:
                self._semaphore.release()
                raise ResourceError(
                    f"GPU memory limit exceeded: {total_gpu}MB > {self.limits.max_gpu_memory_mb}MB"
                )

            if self.limits.max_disk_mb and total_disk > self.limits.max_disk_mb:
                self._semaphore.release()
                raise ResourceError(
                    f"Disk limit exceeded: {total_disk}MB > {self.limits.max_disk_mb}MB"
                )

            allocation = ResourceAllocation(
                job_id=job_id,
                memory_mb=memory_mb,
                cpu_percent=cpu_percent,
                gpu_memory_mb=gpu_memory_mb,
                disk_mb=disk_mb,
            )
            self._allocations[job_id] = allocation
            logger.info(
                f"Resources acquired for {job_id}: mem={memory_mb}MB cpu={cpu_percent}% gpu={gpu_memory_mb}MB"
            )
            return allocation

    async def release(self, job_id: str) -> None:
        """Release resources for a job."""
        async with self._lock:
            if job_id in self._allocations:
                alloc = self._allocations.pop(job_id)
                logger.info(
                    f"Resources released for {job_id}: mem={alloc.memory_mb}MB cpu={alloc.cpu_percent}%"
                )
        self._semaphore.release()

    async def get_usage(self) -> dict[str, Any]:
        """Get current resource usage."""
        async with self._lock:
            return {
                "active_jobs": len(self._allocations),
                "max_concurrent": self.limits.max_concurrent_jobs,
                "memory_mb": sum(a.memory_mb for a in self._allocations.values()),
                "cpu_percent": sum(a.cpu_percent for a in self._allocations.values()),
                "gpu_memory_mb": sum(a.gpu_memory_mb for a in self._allocations.values()),
                "disk_mb": sum(a.disk_mb for a in self._allocations.values()),
            }

    async def list_allocations(self) -> list[ResourceAllocation]:
        """List all current allocations."""
        async with self._lock:
            return list(self._allocations.values())
