# Dragon Data Backend

`DragonDataBackend` constructs and owns a `dragon.data.ddict.DDict` and hands back a `DragonEndpoint` — an opaque, serialized descriptor. It never constructs a Dragon client itself.

### Overview

- Constructing a `DDict` is itself the blocking startup call — it spins up the orchestrator and manager processes, with no separate "start" step; `DragonDataBackend` runs that construction in a thread so it doesn't block the event loop.
- `wait_for_keys` is always forced to `True` — downstream clients (RADEX's compiled `DragonClient`, and native `DDict.__getitem__`) rely on a blocking get, not an immediate `KeyError`, when a key hasn't landed yet.
- All other `DDict` construction arguments are accepted as arbitrary keyword arguments and forwarded to `DDict(...)` unchanged — this class does not mirror or re-validate `DDict.__init__`'s own parameter list. If you pass something `DDict` doesn't like, `DDict` raises its own error.

### Installation

```bash
pip install "rhapsody-py[dragon]"
```

!!! warning "Prerequisites"
    Requires the Dragon runtime, and scripts must be launched through it:
    ```bash
    dragon -s -- python3 my_script.py
    ```

### Basic Usage

```python
import asyncio
import logging

import rhapsody

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonExecutionBackend
from rhapsody.backends.data import DragonDataBackend

rhapsody.enable_logging(level=logging.INFO)


# NOTE: task functions below are each fully self-contained. A task may be
# invoked in a completely separate process/node with no knowledge of this
# module or anything else defined here -- every import and every bit of
# setup a task needs must live inside that task's own function body, never
# factored into a shared helper or relying on driver-scope state. The only
# input a task gets is whatever is explicitly passed as an argument
# (`descriptor`, here) -- DragonDataBackend hands that back from
# `.start()`, it never constructs a client itself. Unlike Redis, the
# Dragon client takes the descriptor directly as a constructor argument --
# no environment variables involved.


# func1 (producer) and func2 (consumer) are submitted together, with no
# ordering guarantee between them -- func2 uses wait_for_*, not get_*, so
# it correctly blocks until func1's data actually lands instead of racing
# it.


def func1(descriptor):
    import numpy as np

    from radex.clients.core import DragonClient
    from radex.handles.handles import OutgoingHandle

    client = DragonClient(descriptor=descriptor, timeout=5)

    samples = np.arange(10, dtype=np.float64) ** 2  # [0, 1, 4, 9, ..., 81]
    client.put_tensor(OutgoingHandle("samples"), samples)
    client.put_scalar(OutgoingHandle("sample-count"), len(samples))
    return len(samples)


def func2(descriptor):
    from radex.clients.core import DragonClient
    from radex.handles.handles import IncomingHandle

    client = DragonClient(descriptor=descriptor, timeout=5)

    samples = client.wait_for_tensor(IncomingHandle("samples"), 10)
    count = client.wait_for_scalar(IncomingHandle("sample-count"), 10)
    return {"count": int(count), "sum": float(samples.sum()), "mean": float(samples.mean())}


async def main():
    session = Session()
    exec_backend = await DragonExecutionBackend()

    # RHAPSODY owns launching the Dragon DDict; RADEX only ever sees the
    # resulting endpoint, never the launch mechanism.
    data_backend = await DragonDataBackend(managers_per_node=1, n_nodes=1)

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

The full runnable file lives at [`examples/data/01-producer-consumer-dragon-radex.py`](https://github.com/radical-cybertools/rhapsody/blob/main/examples/data/01-producer-consumer-dragon-radex.py).

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `"dragon"` | Name this backend is registered under when attached to a `Session` |
| `**ddict_kwargs` | — | — | Forwarded directly to `dragon.data.ddict.DDict(...)` -- e.g. `managers_per_node`, `n_nodes`, `trace`. See the [Dragon `DDict` reference](https://dragonhpc.github.io/dragon/doc/_build/html/) for the full set |
| `working_set_size` | `int` (via `**ddict_kwargs`) | `2` | Not `DDict`'s own default of `1` -- that's incompatible with the `wait_for_keys=True` this class always forces. Override explicitly if you need something else |

`wait_for_keys` cannot be passed via `**ddict_kwargs` — the class hard-enforces `True` and raises `ValueError` if you try to override it (RADEX's compiled client refuses to attach to a `DDict` built with `wait_for_keys=False`, a constraint `DDict` itself has no way to know about).

### Using a native client

`DragonEndpoint.serialize()` returns the same serialized descriptor `DDict.attach()` expects — no RADEX involved:

```python
from dragon.data.ddict import DDict

client = DDict.attach(descriptor, timeout=10)
client["key"] = value          # numpy arrays and other picklables work directly
value = client["key"]          # blocks until the key exists, since wait_for_keys=True
client.detach()                # releases this client's resources; does NOT destroy the dict
```

See [`examples/data/03-producer-consumer-dragon-native.py`](https://github.com/radical-cybertools/rhapsody/blob/main/examples/data/03-producer-consumer-dragon-native.py) for the full pattern.
