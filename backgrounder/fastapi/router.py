from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ..manager import JobManager
from ..models import JobStatus as MJobStatus
from ..models import TaskId as MTaskId
from .deps import get_job_manager
from .schemas import (
    JobAssetsResponse,
    JobStatusResponse,
    ListJobsResponse,
    ProgressDetail,
    ResourceUsageResponse,
    SubmitJobRequest,
    SubmitJobResponse,
)


def _map_status(job) -> JobStatusResponse:
    """Map Job to JobStatusResponse."""
    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        progress=ProgressDetail(
            percent=job.progress.percent,
            stage=job.progress.stage,
            processed_items=job.progress.processed_items,
            total_items=job.progress.total_items,
            message=job.progress.message,
        ),
        task=str(job.task),
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error.__dict__ if job.error else None,
        retry_count=job.retry_count,
    )


def get_router() -> APIRouter:
    """Get FastAPI router for job endpoints."""
    router = APIRouter(prefix="/jobs", tags=["Jobs"])

    @router.post("", response_model=SubmitJobResponse, status_code=status.HTTP_202_ACCEPTED)
    async def submit_job(
        body: SubmitJobRequest,
        manager: JobManager = Depends(get_job_manager),
    ) -> SubmitJobResponse:
        """Submit a new job for processing."""
        job = await manager.submit(
            MTaskId(body.task),
            params=body.params,
            idempotency_key=body.idempotency_key,
            priority=body.priority,
            timeout_seconds=body.timeout_seconds,
        )
        return SubmitJobResponse(
            job_id=job.id,
            status=job.status,
            links={
                "self": f"/api/v1/jobs/{job.id}",
                "assets": f"/api/v1/jobs/{job.id}/assets",
                "cancel": f"/api/v1/jobs/{job.id}/cancel",
            },
        )

    @router.get("/{job_id}", response_model=JobStatusResponse)
    async def get_job(
        job_id: str,
        manager: JobManager = Depends(get_job_manager),
    ) -> JobStatusResponse:
        """Get job status and progress."""
        job = await manager.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return _map_status(job)

    @router.get("", response_model=ListJobsResponse)
    async def list_jobs(
        status_filter: str | None = Query(default=None, alias="status"),
        task_filter: str | None = Query(default=None, alias="task"),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        manager: JobManager = Depends(get_job_manager),
    ) -> ListJobsResponse:
        """List jobs with optional filters."""
        s = MJobStatus(status_filter) if status_filter else None
        t = MTaskId(task_filter) if task_filter else None
        items, total = await manager.list(s, t, limit, offset)
        return ListJobsResponse(
            items=[_map_status(j) for j in items],
            total=total,
            limit=limit,
            offset=offset,
        )

    @router.post("/{job_id}/cancel")
    async def cancel_job(
        job_id: str,
        manager: JobManager = Depends(get_job_manager),
    ):
        """Request job cancellation."""
        job = await manager.cancel(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job_id": job.id, "status": job.status.value}

    @router.post("/{job_id}/retry")
    async def retry_job(
        job_id: str,
        priority: int = Query(100, ge=0, le=1000),
        manager: JobManager = Depends(get_job_manager),
    ):
        """Retry a failed job."""
        job = await manager.retry(job_id, priority=priority)
        if not job:
            raise HTTPException(
                status_code=404,
                detail="Job not found or cannot be retried",
            )
        return {"job_id": job.id, "status": job.status.value, "retry_count": job.retry_count}

    @router.get("/{job_id}/assets", response_model=JobAssetsResponse)
    async def get_job_assets(
        job_id: str,
        manager: JobManager = Depends(get_job_manager),
    ) -> JobAssetsResponse:
        """Get job output assets."""
        job = await manager.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobAssetsResponse(
            cog=job.assets.cog,
            geojson=job.assets.geojson,
            mbtiles=job.assets.mbtiles,
            stac=job.assets.stac,
            custom=job.assets.custom,
        )

    @router.get("/_health")
    async def health_check(manager: JobManager = Depends(get_job_manager)):
        """Health check endpoint."""
        return {"status": "healthy", "service": "backgrounder"}

    @router.get("/_resources", response_model=ResourceUsageResponse)
    async def get_resources(manager: JobManager = Depends(get_job_manager)):
        """Get current resource usage."""
        if not manager.resource_manager:
            raise HTTPException(
                status_code=404,
                detail="Resource manager not configured",
            )
        usage = await manager.resource_manager.get_usage()
        return ResourceUsageResponse(**usage)

    return router
