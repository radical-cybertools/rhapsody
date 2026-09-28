# RHAPSODY Data Backend Examples

Scripts demonstrating `rhapsody.backends.data` — `RedisDataBackend`/`DragonDataBackend`, which launch and own the lifecycle of data infrastructure (a `redis-server` process, a Dragon `DDict`) and hand back a connection endpoint. Each pair below runs the same producer/consumer exchange (a numpy tensor + a scalar count, read back and summarized), once through **RADEX's** typed client and once through a **native** client — same infrastructure, different client, proving the data backend has no RADEX coupling of its own.

See also: [Data Backends docs](../../docs/data-backends/index.md) for the full API reference, configuration options, and multi-node/HPC notes.

---

## Setting up RADEX

`02`/`03` use RADEX's typed clients and need `radex` installed first. See RADEX's own installation guide:

**[radical-cybertools.github.io/radex/getting-started/installation](https://radical-cybertools.github.io/radex/getting-started/installation/)**

`00`/`01` need **no RADEX install at all** — just `pip install redis` for `00`, and the Dragon runtime (already required by `rhapsody[dragon]`) for `01`.

---

## How to Run

| Example | Backend | Launcher | Command |
|---|---|---|---|
| `00-producer-consumer-redis.py` | Redis (native `redis-py`) | Standard Python | `python 00-producer-consumer-redis.py` |
| `01-producer-consumer-dragon.py` | Dragon (native `DDict`) | Dragon runtime | `dragon -s -- python3 01-producer-consumer-dragon.py` |
| `02-producer-consumer-redis-radex.py` | Redis (RADEX `RedisClient`) | Standard Python | `python 02-producer-consumer-redis-radex.py` |
| `03-producer-consumer-dragon-radex.py` | Dragon (RADEX `DragonClient`) | Dragon runtime | `dragon -s -- python3 03-producer-consumer-dragon-radex.py` |

`redis-server` must be reachable on `PATH` (or pass `redis_server_path=`/`RedisDataBackend`'s `cmd=` for a custom build) for the two Redis examples.

---

## Files

### `00-producer-consumer-redis.py`
`RedisDataBackend` launches `redis-server`; the producer/consumer tasks talk to it with plain `redis-py` (`redis.Redis(host, port)`), pickling the tensor themselves since redis-py has no native tensor type. Also has to **poll** for key existence in the consumer — redis-py has no blocking-get primitive.
**Use it to:** confirm `RedisDataBackend`'s endpoint works with any Redis client, not just RADEX's.

### `01-producer-consumer-dragon.py`
`DragonDataBackend` constructs the `DDict`; the tasks attach natively via `dragon.data.ddict.DDict.attach(descriptor)` and use it as a plain dict (`client["key"] = value`). No polling needed — `DragonDataBackend` always forces `wait_for_keys=True`, so `__getitem__` blocks until the key lands.
**Use it to:** confirm `DragonDataBackend`'s endpoint works with any Dragon `DDict` client, not just RADEX's.

### `02-producer-consumer-redis-radex.py`
Same exchange as `00`, but through RADEX's typed `RedisClient` (`radex.clients.core.RedisClient`, connected via `RADEX_STORE`/`RADEX_STORE_OPTS` env vars) and `wait_for_tensor`/`wait_for_scalar` instead of a manual poll loop.
**Use it to:** see the ergonomics RADEX adds on top of the raw endpoint (typed put/get, blocking waits, no manual (de)serialization).

### `03-producer-consumer-dragon-radex.py`
Same exchange as `01`, but through RADEX's typed `DragonClient` (constructed directly from the descriptor: `DragonClient(descriptor=descriptor, timeout=5)`).
**Use it to:** see the Dragon-side equivalent of `02` — same RADEX ergonomics, no environment variables needed this time since `DragonClient` takes the descriptor as a constructor argument.

---

## Choosing between them

| | Native (`00`/`01`) | RADEX (`02`/`03`) |
|---|---|---|
| **Extra install** | `pip install redis` (Redis only) | RADEX built from source |
| **Tensor handling** | Manual (pickle, or dtype/shape bookkeeping) | Automatic (`put_tensor`/`wait_for_tensor`) |
| **Blocking wait for a key** | Redis: manual poll loop · Dragon: built into `DDict` | Built in (`wait_for_scalar`/`wait_for_tensor`) |
| **When to reach for it** | You don't want a RADEX dependency, or need a client RADEX doesn't wrap | You want typed put/get and blocking waits without writing them yourself |

All four scripts follow the same driver shape: construct `Session()`, start the execution + data backends, `session.add_backend(...)` both, submit `produce`/`consume` as `ComputeTask`s, print results, `session.close()`.
