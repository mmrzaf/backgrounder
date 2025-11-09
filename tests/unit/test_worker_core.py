import asyncio

import pytest

from backgrounder.models import Assets, Job, JobStatus, Stage, TaskId
from backgrounder.queue.base import JobQueue, QueueItem
from backgrounder.queue.memory import MemoryQueue
from backgrounder.store.sqlite import SqliteJobStore
from backgrounder.task import TaskRegistry
from backgrounder.util.ids import new_job_id
from backgrounder.worker import Worker


class ExplodingQueue(JobQueue):
    async def put(self, item: QueueItem) -> None: ...
    async def get(self) -> QueueItem:
        raise RuntimeError("boom_in_loop")

    async def close(self) -> None: ...
    def qsize(self) -> int:
        return 0


class SleepyQueue(JobQueue):
    async def put(self, item: QueueItem) -> None: ...
    async def get(self) -> QueueItem:
        while True:
            await asyncio.sleep(10)

    async def close(self) -> None: ...
    def qsize(self) -> int:
        return 0


class OneShotQueue(JobQueue):
    def __init__(self, item: QueueItem | None):
        self._item = item
        self._closed = False

    async def put(self, item: QueueItem) -> None:
        self._item = item

    async def get(self) -> QueueItem:
        if self._item is None:
            # force a quick sleep to hit idle/timeout handling between 50->59/79->85 windows
            await asyncio.sleep(0.01)
            raise asyncio.CancelledError()
        it = self._item
        self._item = None
        return it

    async def close(self) -> None:
        self._closed = True

    def qsize(self) -> int:
        return int(self._item is not None)


@pytest.mark.asyncio
async def test_worker_end_to_end_variants(tmp_db_path):
    store = SqliteJobStore(db_path=tmp_db_path)
    queue = MemoryQueue()
    reg = TaskRegistry()
    events = []

    class OkRunner:
        async def execute(self, job, ctx):
            await ctx.progress(5, Stage.fetch, 5, 100)
            return Assets(geojson={"href": "/ok.geojson"})

    class BoomRunner:
        async def execute(self, job, ctx):
            raise ValueError("oh no")

    class CancelRunner:
        async def execute(self, job, ctx):
            await ctx.progress(1, Stage.fetch, 1, 100)
            await asyncio.sleep(0.02)
            await ctx.check_canceled()
            return Assets()

    reg.add("ok", OkRunner())
    reg.add("boom", BoomRunner())
    reg.add("cancelme", CancelRunner())

    wk = Worker(queue, store, reg, on_event=events.append)
    # start idempotence
    t1 = wk.start()
    t2 = wk.start()
    assert t1 is t2

    # success path + status event
    j_ok = Job(id=new_job_id(), task=TaskId("ok"))
    await store.create(j_ok)
    await queue.put(QueueItem(job_id=j_ok.id))

    async def ok_done():
        j = await store.get(j_ok.id)
        return bool(j and j.status == JobStatus.done)

    for _ in range(200):
        if await ok_done():
            break
        await asyncio.sleep(0.01)
    assert any(e.etype == "status" and e.data.get("status") == "done" for e in events)
    events.clear()
    await wk.stop()

    # missing runner -> error event
    wk2 = Worker(queue, store, reg, on_event=events.append)
    j_missing = Job(id=new_job_id(), task=TaskId("missing"))
    await store.create(j_missing)
    await queue.put(QueueItem(job_id=j_missing.id))
    wk2.start()
    for _ in range(200):
        j = await store.get(j_missing.id)
        if j and j.status == JobStatus.failed and j.error and j.error.code == "runner_not_found":
            break
        await asyncio.sleep(0.01)
    assert any(e.etype == "error" and e.data.get("code") == "runner_not_found" for e in events)
    events.clear()
    await wk2.stop()

    # runner error -> error event
    wk3 = Worker(queue, store, reg, on_event=events.append)
    j_boom = Job(id=new_job_id(), task=TaskId("boom"))
    await store.create(j_boom)
    await queue.put(QueueItem(job_id=j_boom.id))
    wk3.start()
    for _ in range(200):
        j = await store.get(j_boom.id)
        if j and j.status == JobStatus.failed and j.error and j.error.code == "runner_error":
            break
        await asyncio.sleep(0.01)
    assert any(e.etype == "error" and e.data.get("code") == "runner_error" for e in events)
    events.clear()
    await wk3.stop()

    # cancel flow -> canceled status event
    wk4 = Worker(queue, store, reg, on_event=events.append)
    j_cancel = Job(id=new_job_id(), task=TaskId("cancelme"))
    await store.create(j_cancel)
    await queue.put(QueueItem(job_id=j_cancel.id))
    wk4.start()
    for _ in range(200):
        j = await store.get(j_cancel.id)
        if j and j.status == JobStatus.running:
            break
        await asyncio.sleep(0.01)
    await store.request_cancel(j_cancel.id)
    for _ in range(200):
        j = await store.get(j_cancel.id)
        if j and j.status == JobStatus.canceled:
            break
        await asyncio.sleep(0.01)
    assert any(e.etype == "status" and e.data.get("status") == "canceled" for e in events)
    events.clear()
    await wk4.stop()

    # terminal skip branch
    wk5 = Worker(queue, store, reg)
    j_term = Job(id=new_job_id(), task=TaskId("ok"))
    await store.create(j_term)
    await store.update_status_progress(j_term.id, status=JobStatus.done)
    await queue.put(QueueItem(job_id=j_term.id))
    wk5.start()
    await asyncio.sleep(0.02)
    await wk5.stop()

    # loop error branch
    wk6 = Worker(ExplodingQueue(), store, reg)
    wk6.start()
    await asyncio.sleep(0.05)
    await wk6.stop()

    # job_not_found branch
    wk7 = Worker(queue, store, reg)
    await queue.put(QueueItem(job_id="does_not_exist"))
    wk7.start()
    await asyncio.sleep(0.05)
    await wk7.stop()

    # stop() when never started (covers branch with no task)
    wk8 = Worker(queue, store, reg)
    await wk8.stop()

    # CancelledError branch in _run (get blocks, we cancel)
    wk9 = Worker(SleepyQueue(), store, reg)
    wk9.start()
    await asyncio.sleep(0.02)
    await wk9.stop()

    await store.close()


@pytest.mark.asyncio
async def test_worker_extra_exit_branches(tmp_db_path):
    store = SqliteJobStore(db_path=tmp_db_path)
    reg = TaskRegistry()
    queue = OneShotQueue(None)

    # a runner that returns quickly; we'll not enqueue anything to trigger get()-CancelledError
    class Quick:
        async def execute(self, job, ctx):
            return Assets(geojson={"href": "/x"})

    reg.add("quick", Quick())

    wk = Worker(queue, store, reg)
    # stop before start: 133->exit branch
    await wk.stop()
    # start/stop twice: 144->exit, 151->exit
    wk.start()
    await wk.stop()
    await wk.stop()

    await store.close()
