import asyncio
import sys
from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backgrounder.fastapi import lifecycle as lf
from backgrounder.fastapi.lifecycle import setup_backgrounder
from backgrounder.queue.base import JobQueue, QueueItem


def test_setup_backgrounder_requires_store_or_db_path():
    from backgrounder.fastapi.lifecycle import setup_backgrounder

    with pytest.raises(ValueError):
        setup_backgrounder(FastAPI())  # no store and no db_path


def test_taskregistry_import_fallback(monkeypatch, tmp_path):
    """
    Force the 'except' branch:
      try:
          from ..task import TaskRegistry
      except Exception:
          TaskRegistry = object
    by removing the module and reloading lifecycle.
    """
    # Remove backgrounder.fastapi.lifecycle from sys.modules to reload a fresh copy
    sys.modules.pop("backgrounder.fastapi.lifecycle", None)
    # Temporarily make 'backgrounder.task' import fail
    monkeypatch.setitem(sys.modules, "backgrounder.task", None)

    import backgrounder.fastapi.lifecycle as lf  # reloads and hits fallback

    assert lf.TaskRegistry is object


def test_setup_backgrounder_twice_and_no_store_ensure(tmp_db_path, monkeypatch):
    """
    - first call: normal path
    - second call: hits guard/early-return paths
    - replace store._ensure with a no-op-less store to drive hasattr(store, '_ensure') False branch
    """
    app = FastAPI()

    # fake store without _ensure to hit hasattr(store,'_ensure') False
    class NoEnsureStore:
        async def close(self):
            pass

        async def recover_running_to_queued(self):
            return 0

    # first, normal setup to attach state fields
    setup_backgrounder(app, db_path=tmp_db_path, include_router=False)

    # monkeypatch to use NoEnsureStore on second setup
    monkeypatch.setattr(lf, "SqliteJobStore", lambda db_path: NoEnsureStore())
    setup_backgrounder(app, db_path=tmp_db_path, include_router=False)  # second setup

    # manually emulate shutdown with missing worker_task to hit 83-84/89 guards
    if hasattr(app.state, "backgrounder_worker_task"):
        delattr(app.state, "backgrounder_worker_task")
    with TestClient(app):
        pass


def test_setup_backgrounder_composed_lifespan_and_start_stop_errors(tmp_db_path, monkeypatch):
    """
    - compose lifespan to traverse 57->65
    - make Worker.start/stop raise to hit logged-except branches not covered before
    """
    app = FastAPI()

    @asynccontextmanager
    async def exists(_app):
        yield

    app.router.lifespan_context = exists

    # worker with failing start/stop
    class BoomWorker:
        def __init__(self, *a, **k):
            pass

        def start(self):
            raise RuntimeError("start_boom")

        async def stop(self):
            raise RuntimeError("stop_boom")

    monkeypatch.setattr(lf, "Worker", BoomWorker)
    setup_backgrounder(app, db_path=tmp_db_path, include_router=False)

    # run lifespan to exercise both exceptions
    with TestClient(app):
        pass


def test_shutdown_queue_close_failure(tmp_db_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backgrounder.fastapi.lifecycle import setup_backgrounder

    class BoomQueue(JobQueue):
        async def put(self, item: QueueItem) -> None: ...
        async def get(self) -> QueueItem:
            # block a bit so the worker loop starts, then get canceled on shutdown
            while True:
                await asyncio.sleep(10)

        async def close(self) -> None:
            # trigger the except: pass branch in shutdown
            raise RuntimeError("close_boom")

        def qsize(self) -> int:
            return 0

    app = FastAPI()
    setup_backgrounder(app, db_path=tmp_db_path, queue=BoomQueue(), include_router=False)
    # entering/exiting triggers startup + shutdown; 'close' raises, which is swallowed
    with TestClient(app):
        pass
