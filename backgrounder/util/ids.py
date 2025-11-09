from __future__ import annotations

import secrets
import uuid


def new_job_id() -> str:
    """Generate unique job ID with prefix."""
    return f"job_{uuid.uuid4().hex[:16]}"


def new_token(nbytes: int = 32) -> str:
    """Generate secure random token."""
    return secrets.token_urlsafe(nbytes)
