import asyncio
from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backgrounder.fastapi.deps import get_job_manager
from backgrounder.fastapi.lifecycle import setup_backgrounder
from backgrounder.fastapi.router import get_router
from backgrounder.queue.base import JobQueue, QueueItem


class IdleQueue(JobQueue):
    def __init__(self):
        self._items = []
        self._closed = False

    async def put(self, item: QueueItem) -> None:
        self._items.append(item)

    async def get(self) -> QueueItem:
        while True:
            await asyncio.sleep(10)

    async def close(self) -> None:
        self._closed = True

    def qsize(self) -> int:
        return len(self._items)


def _make_app(db_path: str, queue: JobQueue | None = None) -> FastAPI:
    app = FastAPI()
    setup_backgrounder(app, db_path=db_path, queue=queue or IdleQueue())
    app.include_router(get_router(), prefix="/api/v1")
    return app


def test_deps_guard_raises_without_setup():
    class Dummy:
        pass

    d = Dummy()
    d.app = Dummy()
    d.app.state = Dummy()
    with pytest.raises(RuntimeError):
        get_job_manager(d)  # no manager set


def test_router_endpoints_and_schema_validation(tmp_db_path):
    app = _make_app(tmp_db_path)
    with TestClient(app) as client:
        body = {"task": "noop", "params": {"note": "hi"}, "priority": 100, "idempotency_key": "abc"}
        r1 = client.post("/api/v1/jobs", json=body)
        assert r1.status_code == 202
        jid = r1.json()["job_id"]

        r2 = client.post("/api/v1/jobs", json=body)
        assert r2.status_code == 202 and r2.json()["job_id"] == jid

        g = client.get(f"/api/v1/jobs/{jid}")
        assert g.status_code == 200 and g.json()["job_id"] == jid
        lst = client.get(
            "/api/v1/jobs", params={"status": "queued", "task": "noop", "limit": 50, "offset": 0}
        )
        assert lst.status_code == 200 and lst.json()["total"] >= 1

        a = client.get(f"/api/v1/jobs/{jid}/assets")
        assert a.status_code == 200 and set(a.json().keys()) == {
            "cog",
            "geojson",
            "mbtiles",
            "stac",
        }

        c = client.post(f"/api/v1/jobs/{jid}/cancel")
        assert c.status_code == 200 and c.json()["status"] in ("canceled", "cancel_requested")

        assert client.get("/api/v1/jobs/nope").status_code == 404
        assert client.post("/api/v1/jobs/nope/cancel").status_code == 404
        assert (
            client.post("/api/v1/jobs", json={"task": "BAD TASK", "params": {}}).status_code == 422
        )


def test_lifecycle_recover_warn_and_worker_task(tmp_db_path, monkeypatch):
    from backgrounder.fastapi import lifecycle as lf

    calls = {"recov": 0}

    async def _spy(self):
        calls["recov"] += 1
        return 1  # trigger warning branch

    monkeypatch.setattr(lf.SqliteJobStore, "recover_running_to_queued", _spy, raising=True)

    app = _make_app(tmp_db_path)
    with TestClient(app) as client:
        assert calls["recov"] == 1
        assert hasattr(client.app.state, "backgrounder_manager")
        assert hasattr(client.app.state, "backgrounder_worker_task")


def test_lifecycle_variants(tmp_db_path, monkeypatch):
    from backgrounder.fastapi import lifecycle as lf

    calls = {"ens": 0, "recov": 0, "boom": 0}

    async def fake_ensure(self):
        calls["ens"] += 1

    async def fake_recover(self):
        calls["recov"] += 1
        return 0

    async def boom_recover(self):
        calls["boom"] += 1
        raise RuntimeError("x")

    # No router + recovered=0
    monkeypatch.setattr(lf.SqliteJobStore, "_ensure", fake_ensure, raising=True)
    monkeypatch.setattr(lf.SqliteJobStore, "recover_running_to_queued", fake_recover, raising=True)
    app = FastAPI()
    lf.setup_backgrounder(app, db_path=tmp_db_path, include_router=False)
    with TestClient(app):
        pass
    assert calls["ens"] == 1 and calls["recov"] == 1

    # Composed lifespan + exception in recovery
    app2 = FastAPI()

    @asynccontextmanager
    async def existing(app_):
        yield

    app2.router.lifespan_context = existing
    monkeypatch.setattr(lf.SqliteJobStore, "recover_running_to_queued", boom_recover, raising=True)
    lf.setup_backgrounder(app2, db_path=tmp_db_path, include_router=False)
    with TestClient(app2):
        pass
    assert calls["boom"] == 1


def test_worker_start_and_stop_failure_logging(tmp_db_path, monkeypatch):
    from backgrounder.fastapi import lifecycle as lf

    app = FastAPI()
    # force start() to throw
    monkeypatch.setattr(
        lf,
        "Worker",
        type(
            "W",
            (),
            {
                "__init__": lambda self, *a, **k: None,
                "start": lambda self: (_ for _ in ()).throw(RuntimeError("boom")),
                "stop": lambda self: (_ for _ in ()).throw(RuntimeError("boom_stop")),
            },
        ),
    )
    lf.setup_backgrounder(app, db_path=tmp_db_path, include_router=False)


def test_assets_404_when_no_assets(tmp_db_path):
    from backgrounder.fastapi.router import get_router

    app = FastAPI()
    setup_backgrounder(app, db_path=tmp_db_path)
    app.include_router(get_router(), prefix="/api/v1")
    with TestClient(app) as client:
        r = client.get("/api/v1/jobs/does-not-exist/assets")
        assert r.status_code == 404
