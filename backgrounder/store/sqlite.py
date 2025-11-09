from __future__ import annotations

import asyncio
import builtins
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiosqlite

from ..models import Assets, ErrorInfo, Job, JobStatus, Progress, Stage, TaskId
from ..util.time import now_utc, parse_iso
from .base import JobStore

logger = logging.getLogger("backgrounder.store.sqlite")


class SqliteJobStore(JobStore):
    """SQLite-based job store with WAL mode for better concurrency."""

    def __init__(self, db_path: str = "./jobs.db", timeout: float = 30.0):
        self._db_path = db_path
        self._timeout = timeout
        self._lock = asyncio.Lock()
        self._conn: aiosqlite.Connection | None = None

    async def _ensure(self) -> aiosqlite.Connection:
        """Ensure connection is established."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(
                self._db_path,
                isolation_level=None,
                timeout=self._timeout,
            )
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL;")
            await self._conn.execute("PRAGMA synchronous=NORMAL;")
            await self._conn.execute("PRAGMA cache_size=-64000;")  # 64MB cache
            await self._conn.execute("PRAGMA temp_store=MEMORY;")
            await self._init_schema()
        return self._conn

    async def _init_schema(self) -> None:
        """Initialize database schema."""
        cx = await self._ensure()
        await cx.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
              id TEXT PRIMARY KEY,
              idempotency_key TEXT,
              task TEXT NOT NULL,
              params TEXT NOT NULL,

              status TEXT NOT NULL,
              progress TEXT NOT NULL,
              error TEXT,
              assets TEXT NOT NULL,

              cancel_requested INTEGER NOT NULL DEFAULT 0,
              retry_count INTEGER NOT NULL DEFAULT 0,
              timeout_seconds INTEGER,
              worker_id TEXT,

              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              started_at TEXT,
              finished_at TEXT
            );
            """
        )
        await cx.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_idem ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;"
        )
        await cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")
        await cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_task ON jobs(task);")
        await cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);")
        await cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);")
        await cx.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs(worker_id) WHERE worker_id IS NOT NULL;"
        )

    @asynccontextmanager
    async def _tx(self) -> AsyncIterator[aiosqlite.Connection]:
        """Transaction context with lock."""
        async with self._lock:
            conn = await self._ensure()
            try:
                yield conn
            except Exception:
                logger.exception("Transaction error")
                raise

    async def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            try:
                await self._conn.close()
            finally:
                self._conn = None

    async def create(self, job: Job) -> Job:
        """Create new job."""
        async with self._tx() as cx:
            now = now_utc().isoformat()
            await cx.execute(
                """
                INSERT INTO jobs
                (id, idempotency_key, task, params, status, progress, error, assets,
                 cancel_requested, retry_count, timeout_seconds, worker_id,
                 created_at, updated_at, started_at, finished_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.idempotency_key,
                    str(job.task),
                    json.dumps(job.params),
                    job.status.value,
                    self._serialize_progress(job.progress),
                    self._serialize_error(job.error),
                    json.dumps(asdict(job.assets)),
                    1 if job.cancel_requested else 0,
                    job.retry_count,
                    job.timeout_seconds,
                    job.worker_id,
                    job.created_at.isoformat(),
                    now,
                    job.started_at.isoformat() if job.started_at else None,
                    job.finished_at.isoformat() if job.finished_at else None,
                ),
            )
        return job

    async def get(self, job_id: str) -> Job | None:
        """Get job by ID."""
        async with self._tx() as cx:
            row = await (await cx.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))).fetchone()
        return self._row_to_job(row) if row else None

    async def get_by_idempotency(self, key: str) -> Job | None:
        """Get job by idempotency key."""
        async with self._tx() as cx:
            row = await (
                await cx.execute(
                    "SELECT * FROM jobs WHERE idempotency_key = ? ORDER BY datetime(created_at) DESC LIMIT 1",
                    (key,),
                )
            ).fetchone()
        return self._row_to_job(row) if row else None

    async def list(
        self,
        status: JobStatus | None,
        task: TaskId | None,
        limit: int,
        offset: int,
    ) -> tuple[builtins.list[Job], int]:
        """List jobs with filters."""
        where, args = [], []
        if status:
            where.append("status = ?")
            args.append(status.value)
        if task:
            where.append("task = ?")
            args.append(str(task))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        count_sql = f"SELECT COUNT(*) AS n FROM jobs {where_sql}"
        list_sql = (
            f"SELECT * FROM jobs {where_sql} ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?"
        )

        async with self._tx() as cx:
            total = (await (await cx.execute(count_sql, args)).fetchone())["n"]
            rows = await (await cx.execute(list_sql, (*args, limit, offset))).fetchall()
        return [self._row_to_job(r) for r in rows], int(total)

    async def update_status_progress(
        self,
        job_id: str,
        *,
        status: JobStatus | None = None,
        progress: Progress | None = None,
        stage: Stage | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        message: str | None = None,
        worker_id: str | None = None,
    ) -> Job | None:
        """Update job status and progress."""
        job = await self.get(job_id)
        if not job:
            return None

        if status:
            job.status = status
        if progress:
            job.progress = progress
        elif stage or message is not None:
            if stage:
                job.progress.stage = stage
            if message is not None:
                job.progress.message = message
        if started_at:
            job.started_at = started_at
        if finished_at:
            job.finished_at = finished_at
        if worker_id is not None:
            job.worker_id = worker_id

        job.mark_updated()
        await self._persist(job)
        return job

    async def attach_assets(self, job_id: str, assets: Assets) -> Job | None:
        """Attach assets to job."""
        job = await self.get(job_id)
        if not job:
            return None
        job.assets = assets
        job.mark_updated()
        await self._persist(job)
        return job

    async def mark_error(
        self,
        job_id: str,
        err: ErrorInfo,
        status: JobStatus = JobStatus.failed,
    ) -> Job | None:
        """Mark job as failed."""
        job = await self.get(job_id)
        if not job:
            return None
        job.status = status
        job.error = err
        job.finished_at = now_utc()
        job.mark_updated()
        await self._persist(job)
        return job

    async def request_cancel(self, job_id: str) -> Job | None:
        """Request cancellation."""
        job = await self.get(job_id)
        if not job:
            return None

        job.cancel_requested = True
        if job.status == JobStatus.queued:
            job.status = JobStatus.canceled
            job.finished_at = now_utc()
        elif job.status == JobStatus.running:
            job.status = JobStatus.cancel_requested

        job.mark_updated()
        await self._persist(job)
        return job

    async def recover_stale_jobs(self, max_age_seconds: int = 3600) -> int:
        """Recover jobs stuck in running state."""
        cutoff = (now_utc() - timedelta(seconds=max_age_seconds)).isoformat()
        async with self._tx() as cx:
            cur = await cx.execute(
                """
                UPDATE jobs
                SET status = 'queued', worker_id = NULL, updated_at = ?
                WHERE status = 'running' AND datetime(updated_at) < datetime(?)
                """,
                (now_utc().isoformat(), cutoff),
            )
            count = cur.rowcount or 0
            if count > 0:
                logger.warning(f"Recovered {count} stale jobs")
            return count

    async def cleanup_old_jobs(self, days: int = 7) -> int:
        """Delete old completed jobs."""
        cutoff = (now_utc() - timedelta(days=days)).isoformat()
        async with self._tx() as cx:
            cur = await cx.execute(
                """
                DELETE FROM jobs
                WHERE status IN ('done', 'failed', 'canceled', 'timeout')
                AND datetime(finished_at) < datetime(?)
                """,
                (cutoff,),
            )
            count = cur.rowcount or 0
            if count > 0:
                logger.info(f"Cleaned up {count} old jobs")
            return count

    async def _persist(self, job: Job) -> None:
        """Persist job changes."""
        async with self._tx() as cx:
            await cx.execute(
                """
                UPDATE jobs SET
                  idempotency_key=?, task=?, params=?,
                  status=?, progress=?, error=?, assets=?,
                  cancel_requested=?, retry_count=?, timeout_seconds=?, worker_id=?,
                  updated_at=?, started_at=?, finished_at=?
                WHERE id=?
                """,
                (
                    job.idempotency_key,
                    str(job.task),
                    json.dumps(job.params),
                    job.status.value,
                    self._serialize_progress(job.progress),
                    self._serialize_error(job.error),
                    json.dumps(job.assets.__dict__),
                    1 if job.cancel_requested else 0,
                    job.retry_count,
                    job.timeout_seconds,
                    job.worker_id,
                    job.updated_at.isoformat(),
                    job.started_at.isoformat() if job.started_at else None,
                    job.finished_at.isoformat() if job.finished_at else None,
                    job.id,
                ),
            )

    def _serialize_progress(self, progress: Progress) -> str:
        """Serialize progress to JSON."""
        return json.dumps(
            {
                "percent": progress.percent,
                "stage": progress.stage.value,
                "processed_items": progress.processed_items,
                "total_items": progress.total_items,
                "message": progress.message,
            }
        )

    def _serialize_error(self, error: ErrorInfo | None) -> str | None:
        """Serialize error to JSON."""
        if not error:
            return None
        return json.dumps(
            {
                "code": error.code,
                "message": error.message,
                "stage": error.stage.value if error.stage else None,
                "details": error.details,
                "traceback": error.traceback,
            }
        )

    def _row_to_job(self, row: aiosqlite.Row) -> Job:
        """Convert database row to Job."""
        progress_dict = json.loads(row["progress"]) if row["progress"] else {}
        progress = Progress(
            percent=progress_dict.get("percent", 0),
            stage=Stage(progress_dict.get("stage", "idle")),
            processed_items=progress_dict.get("processed_items", 0),
            total_items=progress_dict.get("total_items", 0),
            message=progress_dict.get("message"),
        )

        error_dict = json.loads(row["error"]) if row["error"] else None
        error = None
        if error_dict:
            error = ErrorInfo(
                code=error_dict["code"],
                message=error_dict["message"],
                stage=Stage(error_dict["stage"]) if error_dict.get("stage") else None,
                details=error_dict.get("details", {}),
                traceback=error_dict.get("traceback"),
            )

        assets_dict = json.loads(row["assets"]) if row["assets"] else {}

        job = Job(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task=TaskId(row["task"]),
            params=json.loads(row["params"]) if row["params"] else {},
        )
        job.status = JobStatus(row["status"])
        job.progress = progress
        job.error = error
        job.assets = Assets(**assets_dict)
        job.cancel_requested = bool(row["cancel_requested"])
        job.retry_count = row["retry_count"] or 0
        job.timeout_seconds = row["timeout_seconds"]
        job.worker_id = row["worker_id"]

        job.created_at = parse_iso(row["created_at"])  # type: ignore
        job.updated_at = parse_iso(row["updated_at"])  # type: ignore
        job.started_at = parse_iso(row["started_at"])  # type: ignore
        job.finished_at = parse_iso(row["finished_at"])  # type: ignore
        return job
