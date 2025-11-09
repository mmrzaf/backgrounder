import pytest

from backgrounder.models import Assets, Job, Stage, TaskId
from backgrounder.task import (
    CancelledError,
    NoopRunner,
    RunContext,
    SyncRunnerAdapter,
    TaskRegistry,
)
from backgrounder.util.ids import new_job_id


@pytest.mark.asyncio
async def test_runcontext_events_and_cancel():
    events = []
    saved = {}

    async def save_assets(a):
        saved["a"] = a

    async def set_progress(*args, **kw):
        pass

    async def cancel_true():
        return True

    ctx = RunContext("jid", events.append, cancel_true, save_assets, set_progress)
    await ctx.progress(10, Stage.fetch, 1, 10, "tick")
    assert events[-1].etype == "progress" and events[-1].percent == 10

    await ctx.attach_assets(Assets(geojson={"href": "/x"}))
    assert isinstance(saved["a"], Assets) and events[-1].etype == "assets"

    with pytest.raises(CancelledError):
        await ctx.check_canceled()

    assert ctx.now().tzinfo is not None  # UTC clock


@pytest.mark.asyncio
async def test_sync_runner_adapter_and_noop_runner_fast():
    reg = TaskRegistry()

    def sync_fn(job, ctx):
        return Assets(cog={"href": "/sync"})

    adapter = SyncRunnerAdapter(sync_fn, name="my_sync")
    reg.add("sync", adapter)
    assert reg.get("sync") is adapter

    events = []

    async def save_assets(a):
        pass

    async def set_progress(*args, **kw):
        pass

    async def never_cancel():
        return False

    ctx = RunContext("jid2", events.append, never_cancel, save_assets, set_progress)

    runner = NoopRunner(sleep_ms=0)
    a = await runner.execute(Job(id=new_job_id(), task=TaskId("noop")), ctx)
    assert a.cog and a.geojson and a.mbtiles and a.stac
    assert any(e.etype == "assets" for e in events)


async def test_sync_runner_adapter_raises_and_ctx_attach_none():
    # adapter error path (e.g., sync fn raises) covers 40->exit / 53->exit
    def bad_sync(job, ctx):
        raise RuntimeError("sync fail")

    adapter = SyncRunnerAdapter(bad_sync, name="bad")
    events = []

    async def never_cancel():
        return False

    async def save_assets(a):
        pass

    async def set_progress(*args, **kw):
        pass

    ctx = RunContext("jid", events.append, never_cancel, save_assets, set_progress)

    with pytest.raises(RuntimeError):
        await adapter.execute(Job(id="jid", task=TaskId("bad")), ctx)
    before = len(events)
    await ctx.attach_assets(None)  # still emits an "assets" event per implementation
    after = len(events)
    assert after == before + 1
    assert events[-1].etype == "assets"
