"""Mpix execution backend for Rhapsody.

Single-file backend that manages both regular (multiprocess) and MPI subprocess
workers via ZMQ PUSH/PULL.  MPI workers are launched via mpirun/mpiexec.

All channel logic, resource allocation, worker entry points, and execution
helpers are self-contained.

Regular workers:
  mp.Process children + mp.Queue, result-collector thread, _Resources alloc.

MPI workers:
  Rank 0 as manager, internal PUB/SUB, slow-joiner handshake,
  subcommunicator creation, result aggregation.
"""

from __future__ import annotations

import asyncio
import io
import logging
import multiprocessing as mp
import os
import pickle
import shutil
import socket
import subprocess
import sys
import threading
import time
from queue import Empty
from typing import Any
from typing import Callable

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper

# ---------------------------------------------------------------------------
# Gate imports
# ---------------------------------------------------------------------------
try:
    import zmq
except ImportError:
    zmq = None  # type: ignore[assignment]

try:
    import cloudpickle
except ImportError:
    cloudpickle = None  # type: ignore[assignment]


# ==============================================================================
# Helpers
# ==============================================================================


def _get_logger() -> logging.Logger:
    """Lazy logger — created after user configures logging."""
    return logging.getLogger(__name__)


def _serialize(obj: Any) -> bytes:
    """Serialize using cloudpickle when available, else stdlib pickle."""
    if cloudpickle is not None:
        return cloudpickle.dumps(obj)
    return pickle.dumps(obj)


def _deserialize(data: bytes) -> Any:
    """Deserialize with stdlib pickle (compatible with both serializers)."""
    return pickle.loads(data)  # noqa: S301


def _get_bind_address() -> str:
    """Hostname for ZMQ bind addresses.

    ``MPIX_HOSTNAME`` env var overrides (useful for HPC multi-node).
    """
    return os.environ.get("MPIX_HOSTNAME", socket.gethostname())


# ==============================================================================
# Task execution helpers  (module-level — used by both worker types)
# ==============================================================================


def _execute_task(task: dict) -> dict:
    """Dispatch to function or executable runner."""
    mode = task.get("mode", "function")
    if mode == "function":
        return _exec_function(task)
    if mode == "executable":
        return _exec_executable(task)
    raise ValueError(f"Unknown task mode: {mode}")


def _exec_function(task: dict) -> dict:
    """Run a callable, capture stdout/stderr.
    """
    func = task.get("function")
    args = list(task.get("args", []))
    kwargs = task.get("kwargs", {})

    # MPI communicator injection (convention: args[0] == None → inject comm)
    mpi_comm = task.get("mpi_comm")
    if mpi_comm and args and args[0] is None:
        args[0] = mpi_comm

    old_stdout, old_stderr = sys.stdout, sys.stderr
    stdout_cap, stderr_cap = io.StringIO(), io.StringIO()
    try:
        sys.stdout, sys.stderr = stdout_cap, stderr_cap

        if not callable(func):
            raise ValueError(f"Task 'function' must be callable, got {type(func)}")

        if asyncio.iscoroutinefunction(func):
            ret = asyncio.run(func(*args, **kwargs))
        else:
            ret = func(*args, **kwargs)

        return {
            "task_id": task.get("task_id"),
            "exit_code": 0,
            "stdout": stdout_cap.getvalue(),
            "stderr": stderr_cap.getvalue(),
            "return_value": ret,
            "allocated_cores": task.get("allocated_cores", []),
            "allocated_ranks": task.get("allocated_ranks", []),
            "n_ranks": task.get("n_ranks", 1),
            "rank": task.get("rank"),
        }
    except Exception as e:
        return {
            "task_id": task.get("task_id"),
            "exit_code": 1,
            "stdout": stdout_cap.getvalue(),
            "stderr": stderr_cap.getvalue() + f"\nError: {e}",
            "allocated_cores": task.get("allocated_cores", []),
            "allocated_ranks": task.get("allocated_ranks", []),
            "n_ranks": task.get("n_ranks", 1),
            "rank": task.get("rank"),
        }
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr


def _exec_executable(task: dict) -> dict:
    """Run a subprocess (shell or exec mode)."""
    executable = task.get("executable")
    arguments = task.get("arguments", [])
    shell = task.get("shell", False)
    timeout = task.get("timeout", 3600)

    try:
        cmd: str | list[str] = (
            " ".join([executable] + arguments) if shell else [executable] + arguments
        )
        proc = subprocess.run(
            cmd,
            shell=shell,  # noqa: S602
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "task_id": task.get("task_id"),
            "exit_code": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "allocated_cores": task.get("allocated_cores", []),
            "allocated_ranks": task.get("allocated_ranks", []),
            "n_ranks": task.get("n_ranks", 1),
            "rank": task.get("rank"),
        }
    except subprocess.TimeoutExpired:
        return {
            "task_id": task.get("task_id"),
            "exit_code": -1,
            "stdout": "",
            "stderr": "Command timed out",
            "allocated_cores": task.get("allocated_cores", []),
            "allocated_ranks": task.get("allocated_ranks", []),
            "n_ranks": task.get("n_ranks", 1),
            "rank": task.get("rank"),
        }
    except Exception as e:
        return {
            "task_id": task.get("task_id"),
            "exit_code": -1,
            "stdout": "",
            "stderr": str(e),
            "allocated_cores": task.get("allocated_cores", []),
            "allocated_ranks": task.get("allocated_ranks", []),
            "n_ranks": task.get("n_ranks", 1),
            "rank": task.get("rank"),
        }


# ==============================================================================
# _Resources — slot allocator  (shared by regular + MPI workers)
# ==============================================================================
_FREE, _BUSY = 0, 1


class _Resources:
    """Blocking slot allocator."""

    def __init__(self, n_slots: int):
        self._n_slots = n_slots
        self._slots = [_FREE] * n_slots
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._event.set()  # Initially all free

    def alloc(self, n_needed: int) -> list[int]:
        """Block until *n_needed* slots are free, then mark them BUSY."""
        if n_needed > self._n_slots:
            raise ValueError(f"Need {n_needed} slots but only have {self._n_slots}")

        while True:
            if self._event.is_set():
                with self._lock:
                    if self._slots.count(_FREE) >= n_needed:
                        allocated: list[int] = []
                        for i in range(self._n_slots):
                            if self._slots[i] == _FREE:
                                self._slots[i] = _BUSY
                                allocated.append(i)
                                if len(allocated) == n_needed:
                                    return allocated
                    else:
                        self._event.clear()
            else:
                self._event.wait(timeout=0.1)

    def dealloc(self, slots: list[int]) -> None:
        """Release previously allocated slots."""
        with self._lock:
            for i in slots:
                self._slots[i] = _FREE
            self._event.set()


# ==============================================================================
# _worker_process  — mp.Process child target
# ==============================================================================


def _worker_process(
    process_id: int,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    term_event: mp.Event,
) -> None:
    """Pull → execute → push loop running inside each mp.Process child.

    Tasks arrive as cloudpickle-serialized *bytes* (not raw dicts) because
    mp.Queue's ForkingPickler cannot re-pickle cloudpickle-deserialized
    function objects.  Results are plain dicts — they contain only
    JSON-safe types (no callables) and pickle fine via ForkingPickler.
    """
    while not term_event.is_set():
        try:
            task_bytes = task_queue.get(timeout=1)
        except Empty:
            continue
        task = None
        try:
            task = _deserialize(task_bytes)
            result = _execute_task(task)
            result["allocated_cores"] = task.get("allocated_cores", [])
            result_queue.put(result)
        except Exception as e:
            result_queue.put(
                {
                    "task_id": task.get("task_id") if task else None,
                    "exit_code": 1,
                    "stdout": "",
                    "stderr": str(e),
                    "allocated_cores": task.get("allocated_cores", []) if task else [],
                }
            )


# ==============================================================================
# _regular_worker_main  — regular worker entry point
# ==============================================================================


def _regular_worker_main(
    task_addr: str, result_addr: str, n_cores: int, worker_id: str
) -> None:
    """Subprocess entry point for a regular (non-MPI) worker.

    Architecture::
      1. Spawn *n_cores* ``mp.Process`` children
      2. Single result-collector thread reads ``mp.Queue`` → sends on ZMQ PUSH
      3. Main loop polls ZMQ PULL, allocates resources, enqueues tasks
      4. Registration: ``time.sleep(0.2)`` then send registration msg
    """
    import zmq as _zmq

    ctx = _zmq.Context()
    pull = ctx.socket(_zmq.PULL)
    pull.connect(task_addr)
    push = ctx.socket(_zmq.PUSH)
    push.connect(result_addr)

    resources = _Resources(n_cores)

    tq: mp.Queue = mp.Queue()
    rq: mp.Queue = mp.Queue()
    term = mp.Event()
    stop_collector = threading.Event()

    # --- Spawn child processes ---
    procs: list[mp.Process] = []
    for i in range(n_cores):
        p = mp.Process(target=_worker_process, args=(i, tq, rq, term), daemon=True)
        p.start()
        procs.append(p)

    # --- Result-collector daemon (only writer on push — no lock needed) ---
    log = logging.getLogger(worker_id)

    def _collector() -> None:
        while not stop_collector.is_set():
            try:
                res = rq.get(timeout=1)
                push.send(_serialize(res))
                resources.dealloc(res.get("allocated_cores", []))
            except Empty:
                continue
            except Exception as e:
                log.error(f"Result collector error: {e}")

    ct = threading.Thread(target=_collector, daemon=True)
    ct.start()

    # --- Registration ---
    time.sleep(0.2)
    push.send(
        _serialize(
            {
                "msg_type": "registration",
                "worker_id": worker_id,
                "worker_data": {"n_cores": n_cores, "type": "regular"},
            }
        )
    )

    # --- Main poller loop ---
    poller = _zmq.Poller()
    poller.register(pull, _zmq.POLLIN)
    try:
        while True:
            if pull in dict(poller.poll(timeout=1000)):
                task = _deserialize(pull.recv())
                if task.get("task_id") == "__SHUTDOWN__":
                    break
                allocated = resources.alloc(task.get("n_ranks", 1))
                task["allocated_cores"] = allocated
                tq.put(_serialize(task))  # serialize: mp.Queue can't re-pickle cloudpickled fns
    finally:
        term.set()
        for p in procs:
            p.join(timeout=2)
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
        stop_collector.set()
        ct.join(timeout=2)
        pull.close()
        push.close()
        ctx.term()


# ==============================================================================
# _mpi_worker_main  — MPI worker entry point
# ==============================================================================


def _mpi_worker_main(task_addr: str, result_addr: str, worker_id: str) -> None:
    """Subprocess entry point for an MPI worker (all ranks run this).

    Architecture:
      - Rank 0 = manager: connects to backend, runs distributor + aggregator
      - Internal PUB/SUB for task broadcast to ranks
      - Slow-joiner handshake
      - Registration: ``time.sleep(0.5)`` then send (verbatim, rank 0 only)
    """
    import zmq as _zmq
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.rank
    size = comm.size

    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stdout,  # stderr is PIPEd by the parent for crash diagnostics
        format="%(asctime)s | %(levelname)-8s | [%(name)s] | %(message)s",
    )
    group = comm.Get_group()
    is_mgr = rank == 0

    ctx = _zmq.Context()
    shutdown = threading.Event()

    # ----- rank 0: bind internal sockets + connect to master -----
    pub = int_pull = m_pull = m_push = None  # type: ignore[assignment]
    if is_mgr:
        hostname = _get_bind_address()

        pub = ctx.socket(_zmq.PUB)
        pub.setsockopt(_zmq.SNDHWM, 1000)
        pub.setsockopt(_zmq.LINGER, 0)
        pub.setsockopt(_zmq.TCP_KEEPALIVE, 1)
        pub.setsockopt(_zmq.TCP_KEEPALIVE_IDLE, 300)
        pub_port = pub.bind_to_random_port("tcp://*")
        pub_addr = f"tcp://{hostname}:{pub_port}"

        int_pull = ctx.socket(_zmq.PULL)
        int_pull.setsockopt(_zmq.RCVHWM, 1000)
        int_pull.setsockopt(_zmq.LINGER, 0)
        ip = int_pull.bind_to_random_port("tcp://*")
        int_pull_addr = f"tcp://{hostname}:{ip}"

        m_pull = ctx.socket(_zmq.PULL)
        m_pull.connect(task_addr)

        m_push = ctx.socket(_zmq.PUSH)
        m_push.connect(result_addr)

        info: dict | None = {"pub_addr": pub_addr, "int_pull_addr": int_pull_addr}
    else:
        info = None

    # Broadcast internal addresses
    info = comm.bcast(info, root=0)

    # All ranks: connect SUB + PUSH
    sub = ctx.socket(_zmq.SUB)
    sub.setsockopt(_zmq.RCVHWM, 1000)
    sub.setsockopt(_zmq.LINGER, 0)
    sub.setsockopt(_zmq.TCP_KEEPALIVE, 1)
    sub.setsockopt(_zmq.TCP_KEEPALIVE_IDLE, 300)
    sub.setsockopt_string(_zmq.SUBSCRIBE, "")
    sub.connect(info["pub_addr"])

    rpush = ctx.socket(_zmq.PUSH)
    rpush.setsockopt(_zmq.SNDHWM, 1000)
    rpush.setsockopt(_zmq.LINGER, 0)
    rpush.connect(info["int_pull_addr"])

    # ----- Handshake -----
    log = logging.getLogger(f"mpi_worker_{size}ranks.{rank}")
    if is_mgr:
        ready_count = 0
        expected = size - 1
        while ready_count < expected:
            msg = _deserialize(int_pull.recv())
            if isinstance(msg, dict) and msg.get("type") == "READY":
                ready_count += 1
                log.info("Rank %d ready (%d/%d)", msg.get("rank"), ready_count, expected)
    else:
        rpush.send(_serialize({"type": "READY", "rank": rank}))

    comm.barrier()

    # ----- All ranks: start per-rank worker thread -----
    threading.Thread(
        target=_mpi_rank_worker,
        args=(rank, comm, group, sub, rpush, shutdown),
        daemon=True,
    ).start()

    # ----- Rank 0: start distributor + aggregator, then register -----
    if is_mgr:
        resources = _Resources(size)

        threading.Thread(
            target=_mpi_task_distributor,
            args=(m_pull, pub, resources, shutdown),
            daemon=True,
        ).start()

        threading.Thread(
            target=_mpi_result_aggregator,
            args=(m_push, int_pull, resources, size, shutdown),
            daemon=True,
        ).start()

        # Registration
        time.sleep(0.5)
        m_push.send(
            _serialize(
                {
                    "msg_type": "registration",
                    "worker_id": worker_id,
                    "worker_data": {"n_ranks": size, "type": "mpi"},
                }
            )
        )

    # Block until shutdown signal
    while not shutdown.is_set():
        time.sleep(1)

    # Cleanup
    comm.barrier()
    sub.close()
    rpush.close()
    if is_mgr:
        pub.close()
        int_pull.close()
        m_pull.close()
        m_push.close()
    ctx.term()


# ==============================================================================
# _mpi_rank_worker  — per-rank worker thread
# ==============================================================================


def _mpi_rank_worker(
    rank: int,
    comm: Any,
    group: Any,
    sub_socket: Any,
    push_socket: Any,
    shutdown: threading.Event,
) -> None:
    """Poll internal SUB, filter by allocated_ranks, create subcomm, execute."""
    import zmq as _zmq

    poller = _zmq.Poller()
    poller.register(sub_socket, _zmq.POLLIN)

    while not shutdown.is_set():
        if sub_socket not in dict(poller.poll(timeout=1000)):
            continue

        task = _deserialize(sub_socket.recv())

        if task.get("task_id") == "__SHUTDOWN__":
            return

        if rank not in task.get("allocated_ranks", []):
            continue

        allocated = task.get("allocated_ranks", [rank])
        n = len(allocated)

        sub_comm = sub_grp = None
        try:
            if n > 1:
                sub_grp = group.Incl(allocated)
                sub_comm = comm.Create_group(sub_grp)

            # Inject comm for _exec_function
            task["mpi_comm"] = sub_comm if sub_comm else comm
            task["rank"] = rank
            task["n_ranks"] = n

            result = _execute_task(task)
            result["rank"] = rank
            result["n_ranks"] = n
            result["allocated_ranks"] = allocated
            push_socket.send(_serialize(result))
        except Exception as e:
            push_socket.send(
                _serialize(
                    {
                        "task_id": task.get("task_id"),
                        "exit_code": 1,
                        "stdout": "",
                        "stderr": str(e),
                        "rank": rank,
                        "n_ranks": n,
                        "allocated_ranks": allocated,
                    }
                )
            )
        finally:
            if sub_grp:
                sub_grp.Free()
            if sub_comm:
                sub_comm.Free()


# ==============================================================================
# _mpi_task_distributor  — rank 0 thread
# ==============================================================================


def _mpi_task_distributor(
    master_pull: Any,
    pub_socket: Any,
    resources: _Resources,
    shutdown: threading.Event,
) -> None:
    """Pull from master → alloc ranks → broadcast via PUB."""
    import zmq as _zmq

    poller = _zmq.Poller()
    poller.register(master_pull, _zmq.POLLIN)

    while not shutdown.is_set():
        if master_pull not in dict(poller.poll(timeout=1000)):
            continue

        task = _deserialize(master_pull.recv())

        # Sentinel → forward to ranks and exit
        if task.get("task_id") == "__SHUTDOWN__":
            pub_socket.send(_serialize(task))
            shutdown.set()
            return

        # Allocate ranks (blocking until available)
        allocated = resources.alloc(task.get("n_ranks", 1))
        task["allocated_ranks"] = allocated
        task["n_ranks"] = len(allocated)

        pub_socket.send(_serialize(task))


# ==============================================================================
# _mpi_result_aggregator  — rank 0 thread
# ==============================================================================


def _mpi_result_aggregator(
    master_push: Any,
    internal_pull: Any,
    resources: _Resources,
    total_ranks: int,
    shutdown: threading.Event,
) -> None:
    """Collect per-rank results → aggregate → send to master.

    Aggregation logic:
      stdout / stderr / return_value → lists, exit_code → max.
    """
    import zmq as _zmq

    poller = _zmq.Poller()
    poller.register(internal_pull, _zmq.POLLIN)

    cache: dict[str, list[dict]] = {}

    while not shutdown.is_set():
        if internal_pull not in dict(poller.poll(timeout=1000)):
            continue

        result = _deserialize(internal_pull.recv())

        # Stale READY messages from handshake phase — discard
        if isinstance(result, dict) and result.get("type") == "READY":
            continue

        task_id = result.get("task_id")
        if not task_id:
            continue

        n_ranks = result.get("n_ranks", 1)
        cache.setdefault(task_id, []).append(result)

        # Once all allocated ranks have reported, aggregate and forward
        if len(cache[task_id]) >= n_ranks:
            results = cache.pop(task_id)

            # Aggregate
            agg = results[0].copy()
            agg["stdout"] = [r.get("stdout", "") for r in results]
            agg["stderr"] = [r.get("stderr", "") for r in results]
            agg["return_value"] = [r.get("return_value") for r in results]
            agg["exit_code"] = max(r.get("exit_code", 0) for r in results)

            master_push.send(_serialize(agg))
            resources.dealloc(agg.get("allocated_ranks", []))


# ==============================================================================
# MpixExecutionBackend  — the public backend class
# ==============================================================================


class MpixExecutionBackend(BaseBackend):
    """ZMQ-based execution backend with regular and MPI worker support.

    Regular workers use ``mp.Process`` children + ``mp.Queue``.

    MPI workers are launched via ``mpirun``/``mpiexec`` with rank 0 as manager.

    Args:
        n_workers: Number of regular (non-MPI) worker subprocesses.
        n_cores: Number of ``mp.Process`` children per regular worker.
        n_mpi_ranks: Total MPI ranks to launch (0 = no MPI worker).
        mpi_launcher: Path to mpirun/mpiexec.  Auto-detected if ``None``.
        mpi_launcher_args: Extra arguments inserted after ``-n N`` in the
            MPI launch command (e.g. ``["--oversubscribe"]``).
        name: Backend name for discovery/logging.
    """

    def __init__(
        self,
        n_workers: int = 1,
        n_cores: int = 4,
        n_mpi_ranks: int = 0,
        mpi_launcher: str | None = None,
        mpi_launcher_args: list[str] | None = None,
        name: str = "mpix",
    ):
        if zmq is None:
            raise ImportError(
                "pyzmq is required for MpixExecutionBackend. "
                "Install: pip install pyzmq"
            )

        super().__init__(name=name)
        self.logger = _get_logger()

        # --- config ---
        self._n_workers = n_workers
        self._n_cores = n_cores
        self._n_mpi_ranks = n_mpi_ranks
        self._mpi_launcher = mpi_launcher
        self._mpi_launcher_args = mpi_launcher_args

        # --- state ---
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._shutdown_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

        # --- zmq (created in _setup_zmq) ---
        self._zmq_ctx: Any = None
        self._regular_push: Any = None
        self._mpi_push: Any = None
        self._result_pull: Any = None
        self._regular_task_addr = ""
        self._mpi_task_addr = ""
        self._result_addr = ""

        # --- tracking ---
        self._tasks: dict[str, dict] = {}
        self._workers: dict[str, dict] = {}
        self._regular_procs: list[subprocess.Popen] = []
        self._mpi_proc: subprocess.Popen | None = None
        self._result_thread: threading.Thread | None = None

        # --- callback (set via register_callback, guaranteed to be set) ---
        self._callback_func: Callable

    # -----------------------------------------------------------
    # Awaitable init  (same pattern as ConcurrentExecutionBackend)
    # -----------------------------------------------------------

    def __await__(self):
        """Make backend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization."""
        if not self._initialized:
            try:
                self._loop = asyncio.get_running_loop()

                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                self._setup_zmq()
                self._launch_workers()
                self._initialized = True
                self.logger.info("Mpix execution backend started successfully")
            except Exception as e:
                self.logger.exception(f"Mpix backend initialization failed: {e}")
                self._initialized = False
                raise
        return self

    # -----------------------------------------------------------
    # ZMQ setup
    # -----------------------------------------------------------

    def _setup_zmq(self) -> None:
        """Bind PUSH (regular), PUSH (mpi), PULL (results)."""
        hostname = _get_bind_address()
        self._zmq_ctx = zmq.Context()

        self._regular_push = self._zmq_ctx.socket(zmq.PUSH)
        p = self._regular_push.bind_to_random_port("tcp://*")
        self._regular_task_addr = f"tcp://{hostname}:{p}"

        self._mpi_push = self._zmq_ctx.socket(zmq.PUSH)
        p = self._mpi_push.bind_to_random_port("tcp://*")
        self._mpi_task_addr = f"tcp://{hostname}:{p}"

        self._result_pull = self._zmq_ctx.socket(zmq.PULL)
        p = self._result_pull.bind_to_random_port("tcp://*")
        self._result_addr = f"tcp://{hostname}:{p}"

        self._result_thread = threading.Thread(target=self._result_loop, daemon=True)
        self._result_thread.start()

    # -----------------------------------------------------------
    # Worker launch
    # -----------------------------------------------------------

    def _launch_workers(self) -> None:
        """Spawn regular and MPI worker subprocesses."""
        # --- regular workers ---
        for i in range(self._n_workers):
            wid = f"regular_{i}"
            bootstrap = (
                f"from rhapsody.backends.execution.mpix import _regular_worker_main; "
                f"_regular_worker_main({self._regular_task_addr!r}, "
                f"{self._result_addr!r}, {self._n_cores!r}, {wid!r})"
            )
            proc = subprocess.Popen(
                [sys.executable, "-c", bootstrap],
                stderr=subprocess.PIPE,
            )
            self._regular_procs.append(proc)
            self.logger.info(f"Launched regular worker {wid} (pid {proc.pid})")

        # Verify regular workers started
        if self._regular_procs:
            time.sleep(0.5)
            for i, proc in enumerate(self._regular_procs):
                if proc.poll() is not None:
                    err = proc.stderr.read().decode() if proc.stderr else ""
                    raise RuntimeError(
                        f"Regular worker {i} exited immediately:\n{err}"
                    )

        # --- MPI worker ---
        if self._n_mpi_ranks > 0:
            launcher = self._mpi_launcher
            if launcher is None:
                launcher = shutil.which("mpirun") or shutil.which("mpiexec")
                if launcher is None:
                    raise RuntimeError(
                        "No MPI launcher found. Set mpi_launcher explicitly or "
                        "ensure mpirun/mpiexec is on PATH."
                    )

            wid = "mpi_0"
            bootstrap = (
                f"from rhapsody.backends.execution.mpix import _mpi_worker_main; "
                f"_mpi_worker_main({self._mpi_task_addr!r}, "
                f"{self._result_addr!r}, {wid!r})"
            )
            cmd = (
                [launcher, "-n", str(self._n_mpi_ranks)]
                + (self._mpi_launcher_args or [])
                + [sys.executable, "-c", bootstrap]
            )
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)
            self._mpi_proc = proc
            self.logger.info(
                f"Launched MPI worker {wid} with {self._n_mpi_ranks} ranks "
                f"(pid {proc.pid})"
            )

            # MPI init is slower — longer verification window
            time.sleep(1.0)
            if proc.poll() is not None:
                err = proc.stderr.read().decode() if proc.stderr else ""
                raise RuntimeError(f"MPI worker exited immediately:\n{err}")

    # -----------------------------------------------------------
    # Result loop  (daemon thread)
    # -----------------------------------------------------------

    def _result_loop(self) -> None:
        """Receive and dispatch results/registrations from all workers."""
        poller = zmq.Poller()
        poller.register(self._result_pull, zmq.POLLIN)

        while not self._shutdown_event.is_set():
            if self._result_pull not in dict(poller.poll(timeout=1000)):
                continue

            try:
                msg = _deserialize(self._result_pull.recv())
            except Exception as e:
                self.logger.warning(f"Deserialize error: {e}")
                continue

            # --- registration ---
            if msg.get("msg_type") == "registration":
                wid = msg.get("worker_id", "?")
                self._workers[wid] = msg.get("worker_data", {})
                self.logger.info(
                    f"Worker registered: {wid} {self._workers[wid]}"
                )
                continue

            # --- task result ---
            task_id = msg.get("task_id")
            if not task_id:
                continue

            task = self._tasks.get(task_id)
            if task is None:
                # Already canceled or unknown
                self.logger.debug(
                    f"Discarding result for unknown task {task_id}"
                )
                continue

            # Update task dict in-place (ComputeTask is a dict subclass)
            task["stdout"] = msg.get("stdout")
            task["stderr"] = msg.get("stderr")
            task["exit_code"] = msg.get("exit_code", 1)
            task["return_value"] = msg.get("return_value")
            state = "DONE" if msg.get("exit_code", 1) == 0 else "FAILED"
            task["state"] = state

            self._tasks.pop(task_id, None)

            # Dispatch callback + future resolution on event loop thread
            if self._loop and not self._loop.is_closed():
                try:
                    self._loop.call_soon_threadsafe(
                        self._on_task_complete, task, state
                    )
                except RuntimeError:
                    self._on_task_complete(task, state)
            else:
                self._on_task_complete(task, state)

    def _on_task_complete(self, task: dict, state: str) -> None:
        """Fire callback — futures are handled by the session."""
        self._callback_func(task, state)

    # -----------------------------------------------------------
    # Task submission
    # -----------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit tasks for execution via ZMQ."""
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            uid = task["uid"]
            self._tasks[uid] = task

            # Build wire payload
            payload: dict[str, Any] = {
                "task_id": uid,
                "n_ranks": task.get("ranks", 1),
                "timeout": task.get("timeout", 3600),
            }
            if task.get("function"):
                payload["mode"] = "function"
                payload["function"] = task["function"]
                payload["args"] = task.get("args", ())
                payload["kwargs"] = task.get("kwargs", {})
            else:
                payload["mode"] = "executable"
                payload["executable"] = task.get("executable")
                payload["arguments"] = task.get("arguments", [])
                payload["shell"] = task.get("shell", False)

            # Route and send
            target = self._route_task(task)
            if target is None:
                task.update(
                    {
                        "stdout": None,
                        "stderr": "No live worker available for this task",
                        "exit_code": 1,
                        "return_value": None,
                        "state": "FAILED",
                    }
                )
                self._tasks.pop(uid, None)
                self._callback_func(task, "FAILED")
            elif target == "regular":
                self._regular_push.send(_serialize(payload))
            else:  # "mpi"
                self._mpi_push.send(_serialize(payload))

        return None

    # -----------------------------------------------------------
    # Routing
    # -----------------------------------------------------------

    def _route_task(self, task: dict) -> str | None:
        """Determine target worker type: ``'regular'``, ``'mpi'``, or None."""
        # Explicit override via task kwargs
        wt = task.get("worker_type") or task.get(
            "task_backend_specific_kwargs", {}
        ).get("worker_type")

        if wt is None:
            wt = "mpi" if task.get("ranks", 1) > 1 else "regular"

        # Validate at least one live process of that type
        if wt == "regular":
            return (
                "regular"
                if any(p.poll() is None for p in self._regular_procs)
                else None
            )
        if wt == "mpi":
            return (
                "mpi"
                if (self._mpi_proc and self._mpi_proc.poll() is None)
                else None
            )
        return None

    # -----------------------------------------------------------
    # Cancellation
    # -----------------------------------------------------------

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a pending task.

        Removes from tracking so any late result from the worker is discarded.
        In-flight tasks cannot be recalled from ZMQ.
        """
        task = self._tasks.pop(uid, None)
        if task is not None:
            task["state"] = "CANCELED"
            self._callback_func(task, "CANCELED")
            return True
        return False

    # -----------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------

    async def shutdown(self) -> None:
        """Gracefully shut down all workers and ZMQ resources."""
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        self._tasks.clear()

        # Sentinels → regular workers (round-robin delivers one per worker)
        sentinel = _serialize({"task_id": "__SHUTDOWN__"})
        for _ in range(self._n_workers):
            try:
                self._regular_push.send(sentinel)
            except Exception as e:
                self.logger.warning(f"Error sending shutdown sentinel to regular worker: {e}")

        # Sentinel → MPI worker
        if self._mpi_proc and self._mpi_proc.poll() is None:
            try:
                self._mpi_push.send(sentinel)
            except Exception as e:
                self.logger.warning(f"Error sending shutdown sentinel to MPI worker: {e}")

        # Wait for worker processes to exit
        for proc in self._regular_procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)

        if self._mpi_proc:
            try:
                self._mpi_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._mpi_proc.kill()
                self._mpi_proc.wait(timeout=1)

        # Stop result loop
        self._shutdown_event.set()
        if self._result_thread:
            self._result_thread.join(timeout=2)

        # Close ZMQ sockets and context
        for sock in (self._regular_push, self._mpi_push, self._result_pull):
            if sock:
                sock.close()
        if self._zmq_ctx:
            self._zmq_ctx.term()

        self.logger.info("Mpix execution backend shutdown complete")

    # -----------------------------------------------------------
    # State / stubs  (matching ConcurrentExecutionBackend)
    # -----------------------------------------------------------

    async def state(self) -> str:
        """Get backend state."""
        return self._backend_state.value

    def get_task_states_map(self):
        """Return state mapper for this backend."""
        return StateMapper(backend=self)

    def build_task(self, uid=None, task_desc=None, task_specific_kwargs=None):
        pass

    def task_state_cb(self, task=None, state=None):
        pass

    def link_implicit_data_deps(self, src_task=None, dst_task=None):
        pass

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        pass

    # -----------------------------------------------------------
    # Async context manager
    # -----------------------------------------------------------

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()
