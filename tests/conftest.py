# tests/conftest.py
import asyncio
import os
import typing as t
from pathlib import Path

import pytest

from backgrounder.models import Job, TaskId
from backgrounder.queue.memory import MemoryQueue
from backgrounder.store.sqlite import SqliteJobStore


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def tmp_db_path(tmp_path: Path) -> str:
    return str(tmp_path / "jobs.db")


@pytest.fixture()
async def store(tmp_db_path: str):
    s = SqliteJobStore(db_path=tmp_db_path)
    await s._ensure()  # warm schema
    try:
        yield s
    finally:
        await s.close()


@pytest.fixture()
def queue():
    return MemoryQueue()


@pytest.fixture()
def make_job():
    def _mk(task: str = "test", **kwargs) -> Job:
        return Job(id=f"job_{os.urandom(6).hex()}", task=TaskId(task), **kwargs)

    return _mk


async def wait_for(
    predicate: t.Callable[[], t.Awaitable[bool]] | t.Callable[[], bool], timeout=2.0, interval=0.01
):
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        res = await predicate() if asyncio.iscoroutinefunction(predicate) else predicate()
        if res:
            return True
        await asyncio.sleep(interval)
    return False


class NoFeatureStore:
    """A minimal, fake store to cover hasattr(store, '_ensure')/recover branches in lifecycle."""

    async def close(self):
        pass
