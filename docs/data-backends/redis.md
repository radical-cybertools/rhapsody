# Redis Data Backend

`RedisDataBackend` launches and owns one or more independent `redis-server` processes and hands back a `RedisEndpoint` per node — a `host:port` connection string. It never constructs a Redis client itself.

### Overview

- **Local, out of the box**: `RedisDataBackend()` with no arguments spawns a single local `redis-server` on an auto-picked free port.
- **Multi-node (HPC)**: an `srun`-style `cmd=` launch-command template, one independent `redis-server` per host.
- **No SmartSim, no Redis Cluster**: launching is done via a plain `asyncio` subprocess; `hosts=[...]` gives N independent, unconnected keyspaces (not a sharded cluster).
- **File-based logging**: each node's `redis-server` stdout/stderr is redirected to its own log file — never left as an undrained pipe.

### Installation

`RedisDataBackend` needs no extra Python packages — it's part of the core `rhapsody-py` install:

```bash
pip install rhapsody-py
```

!!! warning "Prerequisites"
    A `redis-server` binary must be reachable — either on `PATH`, or pointed at explicitly via `redis_server_path=`. It is **not** a pip-installable Python package; install it through your OS package manager or build it from source.

### Basic Usage

```python
import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor

import rhapsody

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.backends.data import RedisDataBackend

rhapsody.enable_logging(level=logging.INFO)


# NOTE: task functions below are each fully self-contained. A task may be
# invoked in a completely separate process/node with no knowledge of this
# module or anything else defined here -- every import and every bit of
# setup a task needs must live inside that task's own function body, never
# factored into a shared helper or relying on driver-scope state. The only
# input a task gets is whatever is explicitly passed as an argument
# (`descriptor`, here) -- RedisDataBackend hands that back from `.start()`,
# it never constructs a client itself.


# func1 (producer) and func2 (consumer) are submitted together, with no
# ordering guarantee between them -- func2 uses wait_for_*, not get_*, so
# it correctly blocks until func1's data actually lands instead of racing
# it.


def func1(descriptor):
    import os

    import numpy as np

    from radex.clients.core import RedisClient
    from radex.handles.handles import OutgoingHandle

    os.environ["RADEX_STORE"] = descriptor
    os.environ["RADEX_STORE_OPTS"] = "Standalone"
    client = RedisClient()

    samples = np.arange(10, dtype=np.float64) ** 2  # [0, 1, 4, 9, ..., 81]
    client.put_tensor(OutgoingHandle("samples"), samples)
    client.put_scalar(OutgoingHandle("sample-count"), len(samples))
    return len(samples)


def func2(descriptor):
    import os

    from radex.clients.core import RedisClient
    from radex.handles.handles import IncomingHandle

    os.environ["RADEX_STORE"] = descriptor
    os.environ["RADEX_STORE_OPTS"] = "Standalone"
    client = RedisClient()

    samples = client.wait_for_tensor(IncomingHandle("samples"), 10)
    count = client.wait_for_scalar(IncomingHandle("sample-count"), 10)
    return {"count": int(count), "sum": float(samples.sum()), "mean": float(samples.mean())}


async def main():
    session = Session()
    exec_backend = await ConcurrentExecutionBackend(ProcessPoolExecutor())

    # RHAPSODY owns launching the Redis infrastructure; RADEX only ever
    # sees the resulting endpoint, never the launch mechanism. work_dir is
    # tied to the session's own directory so redis-server's log file lands
    # next to whatever else the session writes.
    data_backend = await RedisDataBackend(
        work_dir=os.path.join(session.work_dir, session.uid)
    )

    session.add_backend(exec_backend)
    session.add_backend(data_backend)

    descriptor = data_backend.endpoints[0].serialize()

    tasks = [
        ComputeTask(function=func1, args=(descriptor,)),
        ComputeTask(function=func2, args=(descriptor,)),
    ]

    futures = await session.submit_tasks(tasks)
    await asyncio.gather(*futures)

    for task in tasks:
        print(f"Task {task.uid} in {task.state} state.")
        print(f"Output: {task.return_value}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
```

!!! success "Output"
    ```
    Task task.000001 in DONE state.
    Output: 10
    Task task.000002 in DONE state.
    Output: {'count': 10, 'sum': 285.0, 'mean': 28.5}
    ```

The full runnable file lives at [`examples/data/00-producer-consumer-redis-radex.py`](https://github.com/radical-cybertools/rhapsody/blob/main/examples/data/00-producer-consumer-redis-radex.py).

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `"redis"` | Name this backend is registered under when attached to a `Session` |
| `hosts` | `Sequence[str] \| None` | `["localhost"]` | Hostnames to launch a `redis-server` on, one per entry -- independent, unconnected keyspaces, not a cluster |
| `port` | `int \| None` | auto-picked | Port to bind on every host. Required when `cmd` is given |
| `cmd` | `str \| None` | `None` | Launch-command template, formatted with `host`/`port`, e.g. `"srun --nodelist={host} redis-server --port {port}"` -- see [Multi-node / HPC](#multi-node-hpc) |
| `redis_server_path` | `str` | `"redis-server"` | Path to the `redis-server` executable, used when `cmd` is not given |
| `extra_args` | `Sequence[str]` | `()` | Extra CLI arguments appended when `cmd` is not given |
| `env` | `Mapping[str, str] \| None` | `None` | Environment variables to add/override for the launched process(es) -- merged with, not replacing, the parent's own environment |
| `work_dir` | `str \| None` | fresh `rhapsody.data.<hash>` dir | Directory for per-node `redis.node{index}.log` files |
| `connect_timeout` | `float` | `5.0` | Per-attempt timeout for the readiness PING |
| `startup_timeout` | `float` | `30.0` | Overall timeout to wait for each node to become ready |
| `poll_interval` | `float` | `0.2` | Delay between readiness poll attempts |
| `shutdown_grace_period` | `float` | `5.0` | Time to wait after SIGTERM before escalating to SIGKILL |

### Multi-node / HPC

For one `redis-server` per compute node on an HPC allocation, provide a `cmd=` template and an explicit `port` (a free port picked on the launching host says nothing about availability on a remote target host):

```python
data_backend = await RedisDataBackend(
    hosts=["node001", "node002", "node003"],
    port=6379,
    cmd="srun --nodelist={host} redis-server --port {port}",
)
```

Each `{host}`/`{port}` pair is formatted into its own command and executed directly (never through a shell). `backend.endpoints` then has one `RedisEndpoint` per host, each independently addressable.

### Using a native client

`RedisEndpoint.serialize()` returns a plain `"host:port"` string — any Redis client can use it, not just RADEX's:

```python
import redis

host, port = descriptor.split(":")
client = redis.Redis(host=host, port=int(port))
```

redis-py has no `wait_for_*` blocking primitive, so a native consumer has to poll for key existence itself — see [`examples/data/02-producer-consumer-redis-native.py`](https://github.com/radical-cybertools/rhapsody/blob/main/examples/data/02-producer-consumer-redis-native.py) for the full pattern (`pip install redis` is required for this one, and only this one).
