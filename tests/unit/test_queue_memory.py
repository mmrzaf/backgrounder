import pytest

from backgrounder.queue.base import QueueItem
from backgrounder.queue.memory import MemoryQueue


@pytest.mark.asyncio
async def test_memory_queue_priority_close_and_size():
    q = MemoryQueue()
    await q.put(QueueItem(job_id="B", priority=100))
    await q.put(QueueItem(job_id="A", priority=10))
    assert q.qsize() == 2
    first = await q.get()
    second = await q.get()
    assert first.job_id == "A" and second.job_id == "B"
    await q.close()
    with pytest.raises(RuntimeError):
        await q.put(QueueItem(job_id="C", priority=0))
