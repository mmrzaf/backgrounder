"""
# Backgrounder

Async job queue library for FastAPI applications, designed for ML and data processing workloads.

## Features

- **Async-first**: Built on asyncio for high concurrency
- **Resource Management**: Control memory, CPU, GPU allocation
- **Multiple Queue Backends**: In-memory (dev) and Redis (production)
- **Job Lifecycle**: Queue → Running → Done/Failed/Cancelled
- **Progress Tracking**: Real-time progress updates with stages
- **Idempotency**: Prevent duplicate job submissions
- **Retry Logic**: Automatic retry for failed jobs
- **Timeout Handling**: Configurable job timeouts
- **FastAPI Integration**: Drop-in router and lifecycle management

## Installation

```bash
pip install aiosqlite  # Required
pip install redis       # Optional, for Redis queue
```

## Quick Start

```python
from fastapi import FastAPI
from backgrounder import setup_backgrounder, TaskRegistry, NoopRunner
from backgrounder.resources import ResourceLimits

app = FastAPI()
registry = TaskRegistry()
registry.add("my-task", NoopRunner())

setup_backgrounder(
    app,
    db_path="./jobs.db",
    registry=registry,
    resource_limits=ResourceLimits(max_concurrent_jobs=10),
    worker_count=4,
)
```

## Custom Task Example

```python
from backgrounder import Job, RunContext, Assets, Stage

class MyTask:
    async def execute(self, job: Job, ctx: RunContext) -> Assets:
        # Fetch data
        await ctx.progress(10, Stage.fetch, 1, 10)
        data = await fetch_data(job.params["url"])
        
        # Process
        await ctx.progress(50, Stage.infer, 5, 10)
        await ctx.check_canceled()  # Check for cancellation
        result = await process(data)
        
        # Save results
        await ctx.progress(90, Stage.write, 9, 10)
        output_path = await save_result(result)
        
        return Assets(custom={"output": {"href": output_path}})

registry.add("my-task", MyTask())
```

## API Endpoints

- `POST /api/v1/jobs` - Submit job
- `GET /api/v1/jobs/{job_id}` - Get job status
- `GET /api/v1/jobs` - List jobs (with filters)
- `POST /api/v1/jobs/{job_id}/cancel` - Cancel job
- `POST /api/v1/jobs/{job_id}/retry` - Retry failed job
- `GET /api/v1/jobs/{job_id}/assets` - Get job outputs
- `GET /api/v1/jobs/_health` - Health check
- `GET /api/v1/jobs/_resources` - Resource usage

## Submit Job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "task": "my-task",
    "params": {"url": "https://example.com/data"},
    "priority": 50,
    "timeout_seconds": 3600
  }'
```

## Using Redis Queue

```python
from backgrounder.queue.redis import RedisQueue

queue = RedisQueue(redis_url="redis://localhost:6379")
setup_backgrounder(app, db_path="./jobs.db", queue=queue)
```

## Resource Management

```python
from backgrounder.resources import ResourceLimits

limits = ResourceLimits(
    max_concurrent_jobs=10,
    max_memory_mb=4096,
    max_cpu_percent=80,
    max_gpu_memory_mb=8192,
)

setup_backgrounder(app, db_path="./jobs.db", resource_limits=limits)
```

## Architecture

```
┌─────────────┐
│   FastAPI   │
│     App     │
└──────┬──────┘
       │
┌──────▼──────────────┐
│   JobManager        │
│ - submit()          │
│ - get()             │
│ - cancel()          │
└─────┬───────────────┘
      │
      ├─────────────────┐
      │                 │
┌─────▼──────┐   ┌──────▼────────┐
│  JobStore  │   │   JobQueue    │
│  (SQLite)  │   │ (Memory/Redis)│
└────────────┘   └───────┬───────┘
                         │
                  ┌──────▼────────┐
                  │    Worker     │
                  │ - execute()   │
                  │ - heartbeat   │
                  └───────┬───────┘
                          │
                  ┌───────▼────────┐
                  │ TaskRegistry   │
                  │ - runners      │
                  └────────────────┘
```

## Best Practices

1. **Use idempotency keys** for critical operations
2. **Set appropriate timeouts** based on task complexity
3. **Monitor resource usage** via `/_resources` endpoint
4. **Implement progress updates** for long-running tasks
5. **Handle cancellation** with `ctx.check_canceled()`
6. **Use Redis queue** for production/distributed systems
7. **Run multiple workers** for better throughput
8. **Set resource limits** to prevent overload

## License

MIT
"""
