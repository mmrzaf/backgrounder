import re
from datetime import UTC, datetime, timedelta, timezone

from backgrounder import __version__
from backgrounder.events import ProgressEvent
from backgrounder.models import Assets, ErrorInfo, Job, JobStatus, Progress, Stage, TaskId
from backgrounder.util.ids import new_job_id
from backgrounder.util.time import iso, now_utc, parse_iso, to_utc


def test_time_helpers_and_parse_iso_roundtrip():
    n = now_utc()
    assert n.tzinfo is UTC

    naive = datetime(2020, 1, 1)
    assert to_utc(naive).tzinfo is UTC

    aware = datetime(2020, 1, 1, tzinfo=timezone(timedelta(hours=3)))
    back = to_utc(aware)
    assert back.utcoffset() == timedelta(0)

    assert to_utc(None) is None
    s = iso(aware)
    assert s.endswith("+00:00")
    assert iso(None) is None
    parsed = parse_iso(s)
    assert parsed.tzinfo is UTC
    assert parse_iso(None) is None


def test_new_job_id_and_models():
    jid = new_job_id()
    assert re.fullmatch(r"job_[0-9a-f]{16}", jid)

    job = Job(id="job_x", task=TaskId("abc"), params={"a": 1})
    job.status = JobStatus.queued
    job.progress = Progress(
        percent=5, stage=Stage.fetch, processed_tiles=1, total_tiles=10, message="hi"
    )
    job.assets = Assets(cog={"href": "/x.tif"})
    d = job.to_dict()
    assert d["task"] == "abc" and d["status"] == "queued" and d["progress"]["stage"] == "fetch"

    err = ErrorInfo(code="x", message="y", stage=Stage.infer, details={"k": "v"})
    assert err.code == "x" and err.stage == Stage.infer and err.details["k"] == "v"

    pe = ProgressEvent("progress", "job_x", Stage.fetch, 10, {"x": 1})
    assert pe.etype == "progress" and pe.percent == 10

    assert isinstance(__version__, str) and __version__
