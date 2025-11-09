from fastapi import Request

from ..manager import JobManager
from .lifecycle import MANAGER_STATE_KEY


def get_job_manager(request: Request) -> JobManager:
    """Dependency to get JobManager from app state."""
    mgr = getattr(request.app.state, MANAGER_STATE_KEY, None)
    if mgr is None:
        raise RuntimeError("JobManager not initialized. Did you call setup_backgrounder()?")
    return mgr
