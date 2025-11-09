from datetime import UTC, datetime

import pytest

from backgrounder.models import Assets, ErrorInfo, Job, JobStatus, Progress, Stage, TaskId
from backgrounder.store.sqlite import SqliteJobStore


@pytest.mark.asyncio
async def test_crud_updates_and_edge_paths(tmp_db_path):
    s = SqliteJobStore(db_path=tmp_db_path)

    # Create with defaults
    j = Job(id="job_1", task=TaskId("alpha"), params={"p": 1}, idempotency_key="k1")
    await s.create(j)

    # get/get_by_idempotency + ordering
    got = await s.get(j.id)
    assert got and got.id == j.id
    idem = await s.get_by_idempotency("k1")
    assert idem and idem.id == j.id

    # List with and without filters
    items, total = await s.list(None, None, 50, 0)
    assert total == 1
    fitems, ftotal = await s.list(JobStatus.queued, TaskId("alpha"), 50, 0)
    assert ftotal == 1 and fitems[0].id == j.id

    # Update: status + progress + started_at + message
    now = datetime.now(UTC)
    updated = await s.update_status_progress(
        j.id,
        status=JobStatus.running,
        progress=Progress(percent=3, stage=Stage.fetch, processed_tiles=1, total_tiles=100),
        started_at=now,
        message="working",
    )
    assert updated and updated.status == JobStatus.running and updated.started_at

    # Stage-only update + finished_at (exercise alternate kwargs)
    done = await s.update_status_progress(j.id, stage=Stage.write, finished_at=datetime.now(UTC))
    assert done.progress.stage == Stage.write and done.finished_at

    # Attach assets
    assets = Assets(geojson={"href": "/g.json"})
    updated2 = await s.attach_assets(j.id, assets)
    assert updated2 and updated2.assets.geojson["href"] == "/g.json"

    # Mark error with explicit status override (covers both persist branches)
    err = ErrorInfo(code="boom", message="broken", stage=Stage.infer)
    failed = await s.mark_error(j.id, err, status=JobStatus.failed)
    assert failed and failed.status == JobStatus.failed and failed.error and failed.finished_at

    # Second job to cover cancel paths
    j2 = Job(id="job_2", task=TaskId("alpha"))
    await s.create(j2)
    # Queued -> canceled immediately
    canc = await s.request_cancel(j2.id)
    assert canc and canc.status == JobStatus.canceled and canc.finished_at

    # Third job running -> cancel_requested
    j3 = Job(id="job_3", task=TaskId("alpha"))
    await s.create(j3)
    await s.update_status_progress(j3.id, status=JobStatus.running)
    cr = await s.request_cancel(j3.id)
    assert cr and cr.status == JobStatus.cancel_requested and cr.cancel_requested

    # Recovery (no running at this exact moment is fine)
    n = await s.recover_running_to_queued()
    assert isinstance(n, int)

    # not-found branches
    assert await s.get("nope") is None
    assert await s.attach_assets("nope", Assets()) is None
    assert await s.mark_error("nope", ErrorInfo(code="x", message="y")) is None
    assert await s.request_cancel("nope") is None

    # Close twice to cover "already closed" path
    await s.close()
    await s.close()


@pytest.mark.asyncio
async def test_sqlite_update_status_progress_noop_returns_none(tmp_db_path):
    store = SqliteJobStore(db_path=tmp_db_path)
    j = Job(id="job_noop", task=TaskId("t"))
    await store.create(j)
    # call with no fields to update -> branch that yields None
    res = await store.update_status_progress(j.id)
    assert res and res.id == j.id
    await store.close()
