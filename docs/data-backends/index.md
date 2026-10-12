# Data Backends

RHAPSODY's `rhapsody.backends.data` package launches and owns the lifecycle of data infrastructure — a `redis-server` process, a Dragon `DDict` — and hands back a connection **endpoint**, mirroring how execution backends launch and own compute infrastructure. A `DataBackend` never constructs a client itself: it only ever gets infrastructure up, ready, and torn down again. Any client that understands the endpoint can use it — RADEX's typed clients (see [Redis](redis.md)/[Dragon](dragon.md)) or a native client like plain `redis-py` or `dragon.data.ddict.DDict.attach()` (see each page's "Using a native client" section).

## Lifecycle

Every `DataBackend` (`RedisDataBackend`, `DragonDataBackend`) follows the same state machine, regardless of what it launches underneath:

```
CREATED ──start()──► STARTING ──┬──► READY ──shutdown()──► SHUTDOWN
                                 └──► FAILED ──shutdown()──► SHUTDOWN
```

`FAILED` and `SHUTDOWN` are terminal — a `DataBackend` cannot be restarted once it lands in either; construct a new instance instead. `start()`/`shutdown()` are both idempotent and safe to call concurrently.

## Using it directly

```python
data_backend = await RedisDataBackend()          # __await__ starts it, returns self
descriptor = data_backend.endpoints[0].serialize()  # opaque, client-agnostic string
...
await data_backend.shutdown()
```

`await RedisDataBackend(...)`/`await DragonDataBackend(...)` starts the backend and returns `self` in one step, matching how `ConcurrentExecutionBackend`/`DragonExecutionBackend` already work.

## Using it with `Session`

A `DataBackend` can be registered with a `Session` alongside execution backends, in the same list — `Session` dispatches internally based on whether a backend executes tasks (`hasattr(backend, "submit_tasks")`), so a `DataBackend` never receives a task but still gets its lifecycle managed uniformly:

```python
session = Session()
exec_backend = await ConcurrentExecutionBackend()
data_backend = await RedisDataBackend(work_dir=os.path.join(session.work_dir, session.uid))

session.add_backend(exec_backend)
session.add_backend(data_backend)
...
await session.close()   # shuts down every registered backend, data and execution alike
```

Tying `work_dir` to `session.work_dir`/`session.uid` (Redis only — Dragon writes no log files) puts `redis-server`'s log file inside the session's own directory rather than a separate one; see the [Redis Data Backend](redis.md) page.

<div class="grid cards" markdown>

-   :material-database: **[Redis Data Backend](redis.md)**

    Launches one or more independent `redis-server` processes. Works out of the box locally; multi-node via an `srun`-style `cmd=` template.

-   :material-graph-outline: **[Dragon Data Backend](dragon.md)**

    Constructs and owns a `dragon.data.ddict.DDict`. Requires the Dragon runtime (`dragon -s -- ...`).

</div>
