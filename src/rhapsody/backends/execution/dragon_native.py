"""Dragon execution backend on Dragon's native API, without ``dragon.workflows.batch``.

``DragonNativeExecutionBackend`` takes the same task descriptions as ``DragonExecutionBackend``
and runs them on Dragon's ``Process``, ``ProcessGroup``, ``Policy`` and ``Queue``:

- **Function tasks** run in a persistent pool of worker processes, each pinned to its host and
  CPUs, each with its own inbox queue on its node. Results come back on one queue on the client's
  node. A task with a placement goes to a free worker on that host; a task that asks for GPUs goes
  to a worker started for that host and GPU set. A relay process on the client's node puts each
  task on its inbox and takes each result, and trades them with the client in batches, so a busy
  client process, whose GIL the backend's threads share, does not slow every task down.
- **Executable tasks**, and function tasks with a ``process_template``, run as one placed native
  ``Process`` each.
- **Tasks with ``process_templates``** run as one ``ProcessGroup`` each, with the PMI the task or
  the backend chooses (``pmi``), and without one when neither does.

The user guide is ``docs/execution/dragon-native.md``.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import logging
import os
import queue
import resource
import shutil
import signal
import threading
import time
import uuid
from collections import Counter
from collections import defaultdict
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

from rhapsody.api.errors import TaskExecutionError

from ..base import BaseBackend
from ..constants import BackendMainStates
from ..constants import StateMapper
from . import _dragon_native_worker as _worker
from . import _measure
from . import _peak
from .dragon import CRAY_MPICH_ENV
from .dragon import PEAK_SOURCES
from .dragon import _pmi_options

DRAGON_NATIVE_INIT_ERROR = None

try:
    import cloudpickle
except ImportError:  # pragma: no cover - Dragon depends on cloudpickle
    cloudpickle = None

try:
    from dragon.globalservices.process import kill as process_kill
    from dragon.globalservices.process import multi_join
    from dragon.infrastructure.policy import Policy
    from dragon.native.machine import Node
    from dragon.native.machine import System
    from dragon.native.machine import current as current_node
    from dragon.native.process import Process
    from dragon.native.process import ProcessTemplate
    from dragon.native.process_group import ProcessGroup
    from dragon.native.queue import Queue

except ImportError as e:  # pragma: no cover - environment without Dragon
    process_kill = None
    multi_join = None
    Policy = None
    Node = None
    System = None
    current_node = None
    Process = None
    ProcessTemplate = None
    ProcessGroup = None
    Queue = None
    DRAGON_NATIVE_INIT_ERROR = e

if TYPE_CHECKING:
    from dragon.infrastructure.facts import PMIBackend


def _get_logger() -> logging.Logger:
    """Get the module logger lazily, after the user has configured logging."""
    return logging.getLogger(__name__)


# Inbox and result queues carry small pickled messages; a larger one takes a side allocation.
_INBOX_SIZE = 8
_RESULT_SIZE = 4096
_BLOCK_SIZE = 4096
# Batches of operations the client may queue for the relay. The relay's queue to the client holds
# one batch: while the client handles it, the relay gathers the next.
_CONTROL_SIZE = 1024
_INBOUND_SIZE = 1
# Most seconds the relay waits, after the client sent tasks with its acknowledgement of a batch,
# before it forwards the next: short tasks then come back in that batch, not one batch each.
_RELAY_LINGER = 0.001
# Seconds between the worker watcher's non-blocking checks for dead workers, and the longest the
# result thread blocks on an empty queue.
_WATCH_INTERVAL = 1.0
# Seconds a task's put on a worker's inbox may take. A put holds the backend's lock, and Dragon's
# default is to wait for good: a longer one counts as a broken inbox.
_SEND_TIMEOUT = 10.0
# Seconds before a worker is sent a task again after Dragon's memory pool was full, doubled at
# each failure up to the maximum.
_RETRY_FIRST = 0.01
_RETRY_MAX = 1.0
# Seconds shutdown gives all kills, on top of kill_grace, and then all task threads.
_STOP_KILLS = 20.0
_STOP_THREADS = 15.0
# Template keys the backend sets itself.
_OWN_TEMPLATE_KEYS = ("target", "args", "kwargs")
# Where a result message holds the task's pickled value.
_VALUE_INDEX = {_worker.DONE: 4, _worker.RESULT: 3}
# Descriptors Local Services holds, per node and per Python process it starts; 1 per executable.
_FDS_PER_NODE = 40
_FDS_PER_PYTHON_PROCESS = 3
# The kernel's list of the hardware threads that share a CPU's core.
_SIBLINGS_PATH = "/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list"


class TaskStateMapper:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    terminal_states = {DONE, FAILED, CANCELED}


class _Worker:
    """One pool worker: a Dragon process with its own inbox, on one host and CPU/GPU set."""

    __slots__ = (
        "wid",
        "host",
        "cpus",
        "gpus",
        "inbox",
        "proc",
        "state",
        "task",
        "initial",
        "retry",
        "backoff",
        "pid",
        "node",
        "oom_score_adj",
        "sent",
    )

    def __init__(self, wid: int, host: str, cpus: tuple, gpus: tuple, initial: bool, retry: bool):
        self.wid = wid
        self.host = host
        self.cpus = cpus
        self.gpus = gpus
        self.inbox: Any = None
        self.proc: Any = None
        # starting, idle, busy, dead, or paused: waiting to be sent a task after a full memory pool
        self.state = "starting"
        self.task: _Task | None = None
        self.initial = initial
        # A replacement for a worker that died: if it dies before it is ready, it is not replaced.
        self.retry = retry
        self.backoff = _RETRY_FIRST
        # From its READY: its OS pid and host name, and the oom_score_adj it runs with.
        self.pid: int | None = None
        self.node: str | None = None
        self.oom_score_adj: int | None = None
        # Tasks put on its inbox: the worker numbers them the same way, from 1.
        self.sent = 0

    @property
    def key(self) -> tuple:
        return (self.host, self.gpus)


class _Task:
    """The backend's record of one submitted task."""

    __slots__ = (
        "uid",
        "desc",
        "kind",
        "key",
        "timeout",
        "token",
        "payload",
        "proc",
        "templates",
        "pmi",
        "group",
        "puids",
        "ranks",
        "state",
        "worker",
        "cancelled",
        "timed_out",
        "stdio",
        "inline_files",
        "is_function",
        "result",
        "result_event",
        "thread",
        "timer",
        "put_deadline",
        "put_failed_on",
        "repeats",
        "measured",
        "peak_dir",
        "seq",
        "sent_at",
        "started_at",
    )

    def __init__(self, uid: str, desc: dict, kind: str, timeout: float | None, token: str):
        self.uid = uid
        self.desc = desc
        # Descriptions submitted with this uid while the task ran: they get its states, in order.
        self.repeats: list = []
        self.kind = kind  # pool, process, group
        self.key: tuple = (None, ())
        self.timeout = timeout
        self.token = token
        self.payload: bytes | None = None
        self.proc: Any = None
        self.templates: list = []
        self.pmi: Any = None
        self.group: Any = None
        self.puids: list = []
        self.ranks = 1
        self.state = "queued"  # queued, running, done
        self.worker: Any = None
        self.cancelled: Any = False  # set from other threads: a type checker must not narrow it
        self.timed_out = False
        self.stdio: tuple = (None, None)
        self.inline_files = False
        self.is_function = False
        self.result: tuple | None = None
        self.result_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.timer: int | None = None
        # A pool task's send: when it stops waiting for a full memory pool, and the host where it
        # failed once with another error.
        self.put_deadline: float | None = None
        self.put_failed_on: str | None = None
        # observe_peaks: a pool task's value comes back with its measurement; a process or group
        # task's processes write their records to peak_dir, read when it ends.
        self.measured = False
        self.peak_dir: str | None = None
        # A pool task's number on its worker, and when it was sent there; a process task's start.
        # Wall clock: what observe_peaks reports for a task that died with its process.
        self.seq: int | None = None
        self.sent_at: float | None = None
        self.started_at: float | None = None


def _read_siblings(cpu: int) -> str:
    """The kernel's list of the hardware threads on ``cpu``'s core; empty when it has none."""
    try:
        with open(_SIBLINGS_PATH.format(cpu)) as f:
            return f.read().strip()
    except OSError:
        return ""


def _group_by_core(cpus: list) -> list:
    """Group ``cpus`` into one tuple per physical core, by their lowest CPU.

    A job's CPUs need not list a core's hardware threads side by side: 0-7 and 256-263 can be eight
    cores twice over. The topology read here is the client node's, so a node numbered differently
    gets the same CPUs, grouped less well: its workers may share cores, and the default pool on it
    may not have one worker per core. A CPU with no known siblings is its own core.
    """
    cores: dict = {}
    for cpu in sorted(cpus):  # Node.cpus comes from a set
        cores.setdefault(_read_siblings(cpu) or cpu, []).append(cpu)
    return [tuple(core) for core in cores.values()]


def _split_cpus(cores: list, n: int) -> list:
    """Split CPUs, given as one tuple per core, into ``n`` near-equal slices.

    Up to one worker per core, each slice takes whole cores, so no two workers share one. With more
    workers than cores, the slices take hardware threads; one CPU each if ``n`` is larger still.
    """
    if n <= 0:
        return []
    if not cores:
        return [()] * n
    units = cores
    if n > len(cores):
        units = [(cpu,) for core in cores for cpu in core]
    if n > len(units):
        return [units[i % len(units)] for i in range(n)]
    base, extra = divmod(len(units), n)
    slices, start = [], 0
    for i in range(n):
        end = start + base + (1 if i < extra else 0)
        slices.append(tuple(cpu for unit in units[start:end] for cpu in unit))
        start = end
    return slices


def _group_pmi(pmi) -> PMIBackend | None:
    """Return the ``ProcessGroup`` PMI for ``pmi``.

    ``pmi`` is ``"pmix"``, ``"cray"``, ``"none"`` or a Dragon ``PMIBackend``, read as the Batch
    backend reads it. None and ``"none"`` both give None: a group has no default PMI.
    """
    return _pmi_options(pmi).get("pmi")


def _load_value(msg: tuple) -> tuple:
    """Unpickle the value in a result message; one that cannot be loaded here fails its task."""
    index = _VALUE_INDEX.get(msg[0])
    if index is None:
        return msg
    fields = list(msg)
    try:
        fields[index] = cloudpickle.loads(fields[index])
    except BaseException as exc:  # noqa: BLE001 - it runs the task's own code on this thread
        fields[index] = RuntimeError(f"cannot unpickle the task's result: {exc!r}")
        fields[index - 1] = False
    return tuple(fields)


def _launch_under_peak(kwargs: dict, peak_dir: str) -> None:
    """Make an executable's Process or ProcessTemplate kwargs run it under the peak launcher.

    The launcher (this Python running ``_peak.py``) starts the target as its child, reaps it with
    ``wait4``, writes its record to ``peak_dir`` and exits as the target did. The target's
    environment, directory, policy and output settings are the launcher's, and its child inherits
    them.
    """
    launcher = _peak.launcher_argv(peak_dir)  # python -I _peak.py --out-dir DIR --
    kwargs["args"] = [*launcher[1:], kwargs["target"], *kwargs.get("args", ())]
    kwargs["target"] = launcher[0]


def _read_and_remove(path: str | None) -> str:
    if not path:
        return ""
    try:
        with open(path, errors="replace") as f:
            text = f.read()
    except OSError:
        return ""
    with contextlib.suppress(OSError):
        os.unlink(path)
    return text


def _signal_name(code: Any) -> str | None:
    """The name of the signal a negative exit code stands for; None for an exit on its own."""
    if not isinstance(code, int) or isinstance(code, bool) or code >= 0:
        return None
    try:
        return signal.Signals(-code).name
    except ValueError:
        return f"signal {-code}"


def _killed_observed(
    started: float | None, host: str | None, hosts: list | None, code: Any, timed_out: bool
) -> dict:
    """``observed`` for a task that died with its process, which took its measurement along.

    What the backend knows: when the task was sent or started, when its death was seen, where, and
    how it ended. ``host_mem`` is None: unknown, not zero.
    """
    observed: dict = {"host_mem": None, "gpu_mem": None, "started": started, "ended": time.time()}
    if host:
        observed["host"] = host
    elif hosts:
        if len(hosts) == 1:
            observed["host"] = hosts[0]
        else:
            observed["hosts"] = list(hosts)
    observed["killed"] = _signal_name(code)
    if isinstance(code, int) and not isinstance(code, bool):
        observed["exit_code"] = code
    if timed_out:
        observed["timed_out"] = True
    return observed


class DragonNativeExecutionBackend(BaseBackend):
    """Dragon backend on the native API: a placed worker pool, native processes, process groups.

    It accepts the task descriptions of ``DragonExecutionBackend``: ``function`` or
    ``executable`` with ``args``/``kwargs`` or ``arguments``, ``capture_stdio``, ``placement``
    (``{"node", "host", "gpus"}`` from AsyncFlow), and ``task_backend_specific_kwargs`` with
    ``timeout``, ``process_template`` or ``process_templates``. How each kind runs:

    - A function task without templates, or whose ``process_template`` holds only a ``policy``
      that names at most a host and GPUs, runs in the worker pool and returns its value. Async
      functions run with ``asyncio.run`` in the worker.
    - Any other function task with a ``process_template`` (including ``{}``) runs as its own
      Dragon process, and returns its value too (``DragonExecutionBackend`` returns the exit code).
    - An executable runs as one native ``Process``. Its ``return_value`` and ``exit_code`` are the
      exit code.
    - ``process_templates`` run as one ``ProcessGroup``. ``return_value`` is the list of exit
      codes and ``exit_code`` the first non-zero one.

    MPI: a ``ProcessGroup`` starts its processes with the PMI chosen by ``pmi``: ``"pmix"``
    (Open MPI and other PMIx-based MPIs), ``"cray"``, ``"none"`` or a Dragon ``PMIBackend``. Set it
    for every group with ``DragonNativeExecutionBackend(pmi=...)``, or per task with ``"pmi"`` in
    ``task_backend_specific_kwargs``, which takes precedence. With neither, the group has no PMI,
    unlike a Batch job, and the first multi-rank group logs a warning: without one, MPI ranks hang
    in ``MPI_Init`` (Cray MPICH) or each runs as a one-rank job (Open MPI). Under Cray MPICH
    (``CRAY_MPICH_DIR`` or ``CRAY_MPICH_VERSION`` set) a one-rank group logs it too. Give MPI groups
    a ``timeout`` (seconds) too: when a rank dies, Dragon does not end the group, and the other
    ranks wait for it; the first multi-rank group without one logs a warning. ``"none"`` declares
    the processes non-MPI, and neither warning is logged. Dragon loads PMIx with
    ``dlopen("libpmix.so")``, so a directory holding that name must be on ``LD_LIBRARY_PATH``; when
    it can't be loaded, the whole Dragon runtime aborts.

    Placement hosts and GPU ids are checked against Dragon's nodes before anything is submitted:
    Dragon aborts the whole runtime on an unknown host.

    Args:
        workers_per_node: Pool workers per node. Defaults to one per physical core of the node's
            CPUs in the job (``Node.cpus``). Each worker is pinned to its share of those CPUs, in
            whole physical cores while there are enough. It is a default, not a limit: any other
            number, above or below, is used as given.
        gpu_workers_per_set: Pool workers started, on demand, for each host and GPU set that
            function tasks ask for. Tasks beyond that wait for a worker of their set.
        max_process_ranks: Processes of process and group tasks that may run at once, over all
            nodes. Defaults to the runtime's CPUs. A task that needs more runs alone.
        inline_stdio: Return the output of tasks without ``capture_stdio`` as text in
            ``task["stdout"]`` and ``task["stderr"]``. When False, that output goes to the
            console. For process tasks the text goes through files in a hidden directory under
            the work directory, which must be on a file system the nodes share.
        kill_grace: Seconds between SIGTERM and SIGKILL when a process task is cancelled or times
            out. Pool workers get SIGKILL at once.
        worker_start_timeout: Seconds to wait for the pool to start, and for each replacement or
            GPU worker to be ready; one that is not is killed and counts as dead.
        pmi: The PMI of every ``process_templates`` group whose task sets none: ``"pmix"``,
            ``"cray"``, ``"none"`` or a Dragon ``PMIBackend``. An unknown name raises
            ``ValueError`` before anything starts.
        name: Backend name.
        observe_peaks: Report what each task used as ``task["observed"]`` when it is DONE or
            FAILED, with the keys of ``DragonExecutionBackend``'s: ``host_mem`` (GiB),
            ``gpu_mem`` (GiB per GPU, or None), ``started`` and ``ended`` (wall clock, where the
            task ran), ``host`` (or ``hosts``) and ``cpu_seconds``. For AsyncFlow's
            ``ResourcePolicy`` to learn memory from, and for when a task really ran: the backend
            reports RUNNING when it accepts a task. Off by default.

            - Pool function tasks: the worker measures the call. ``host_mem`` is the worker's RSS
              growth during the call, marked ``"approximate": True`` (Linux only); no
              ``gpu_mem``. A function that raises reports what was measured until it raised.
            - Executables, as one process or as the ranks of ``process_templates``, run under the
              peak launcher (:mod:`rhapsody.backends.execution._peak`), which reaps the command
              with ``wait4`` (``ru_maxrss``: the largest single process of its tree) and, with
              ``pynvml``, samples its GPU memory. A group sums its ranks.
            - Function tasks that run as their own process, or as group members, write the same
              record from inside the process: its lifetime peak RSS or its children's, and its
              CPU time during the call; no ``gpu_mem``.
            - A pool task whose worker died or was killed for its timeout, and a process task
              killed before it wrote its records, report what is known: ``started`` (when it was
              sent or started), ``ended``, ``host``, ``killed`` (the signal's name, or None) and
              ``exit_code``, plus ``timed_out`` for a timeout; ``host_mem`` is None.
            - The records go to a fresh directory per run under
              ``<work_dir>/rhapsody-peaks.<tag>/``, which must be on a file system the nodes
              share, and the launcher runs with this Python (``-I``).
        peak_source: ``"auto"`` (the default) or ``"launcher"``: both use the launcher. Dragon's
            native API reports no per-process resource usage (``Task.rusage`` is Batch's), so
            ``"dragon"`` raises ``ValueError`` with ``observe_peaks``.
        worker_oom_score_adj: The ``oom_score_adj`` pool workers raise themselves to (Linux), so
            that under memory pressure the kernel kills a worker, and fails its task, before one
            of Dragon's services, which would end the runtime. A worker keeps a higher value it
            inherited. None leaves it as Dragon starts it.
    """

    @classmethod
    def is_available(cls) -> bool:
        """Return True if Dragon's native API (dragon.native) is importable."""
        return Process is not None

    def __init__(
        self,
        workers_per_node: int | None = None,
        gpu_workers_per_set: int = 1,
        max_process_ranks: int | None = None,
        inline_stdio: bool = True,
        kill_grace: float = 2.0,
        worker_start_timeout: float = 300.0,
        pmi: str | PMIBackend | None = None,
        name: str | None = "dragon_native",
        observe_peaks: bool = False,
        peak_source: str = "auto",
        worker_oom_score_adj: int | None = 500,
    ):
        if Process is None:
            raise RuntimeError(DRAGON_NATIVE_INIT_ERROR)
        if peak_source not in PEAK_SOURCES:
            raise ValueError(f"peak_source must be one of {PEAK_SOURCES}, not {peak_source!r}")
        if observe_peaks and peak_source == "dragon":
            raise ValueError(
                "peak_source='dragon' needs Batch's per-task rusage, which Dragon's native API "
                "does not report; use 'auto' or 'launcher'"
            )
        if workers_per_node is not None and workers_per_node < 0:
            raise ValueError("workers_per_node must be >= 0")
        if gpu_workers_per_set < 1:
            raise ValueError("gpu_workers_per_set must be >= 1")
        _group_pmi(pmi)

        super().__init__(name=name)
        self.logger = _get_logger()

        self._workers_per_node = workers_per_node
        self._gpu_workers_per_set = gpu_workers_per_set
        self._max_process_ranks_arg = max_process_ranks
        self._inline_stdio = inline_stdio
        self._kill_grace = kill_grace
        self._worker_start_timeout = worker_start_timeout
        self._pmi = pmi
        self._warned_job_pmi = False
        self._warned_job_timeout = False
        self._observe_peaks = observe_peaks
        self._worker_oom_score_adj = worker_oom_score_adj
        # Records go to <work_dir>/rhapsody-peaks.<tag>/<uid>/: the tag keeps a reused work dir's
        # leftovers out of the sums; shutdown removes the roots with what cancelled tasks left.
        self._peak_tag = uuid.uuid4().hex[:8]
        self._peak_roots: set[str] = set()

        self._id = uuid.uuid4().hex[:12]
        self._backend_state = BackendMainStates.INITIALIZED
        self._callback_func: Callable = lambda t, s: None
        self._task_states = TaskStateMapper()
        self._initialized = False
        self._started = False
        self._start_lock = asyncio.Lock()
        self._stopping = False
        self._loop: asyncio.AbstractEventLoop | None = None

        # Filled by _start(): Dragon host name -> {"cpus", "gpus", "h_uid"}; client host first.
        self._nodes: dict[str, dict] = {}
        self._hosts_by_huid: dict[int, str] = {}
        self._host_order: list[str] = []
        self._client_host: str | None = None
        self._results: Any = None
        self._max_process_ranks = 0
        # The relay, its queue of operations from the client, and its queue of results to it.
        self._relay: Any = None
        self._control: Any = None
        self._inbound: Any = None
        self._control_lock = threading.Lock()
        # Set when the relay has died: no pool task can be sent or heard from again.
        self._relay_error: str | None = None

        # Everything below is guarded by _lock.
        self._lock = threading.RLock()
        self._tasks: dict[str, _Task] = {}
        self._workers: dict[int, _Worker] = {}
        self._workers_by_puid: dict[int, _Worker] = {}
        self._wids = itertools.count()
        self._idle: dict[tuple, deque[_Worker]] = defaultdict(deque)
        self._pending: dict[tuple, deque[_Task]] = defaultdict(deque)
        # Sends decided since the relay was last sent a batch.
        self._outgoing: list = []
        self._live_by_key: dict[tuple, int] = defaultdict(int)
        self._proc_queue: deque = deque()
        self._running_ranks = 0
        self._initial_left = 0
        self._ready_event = threading.Event()
        self._start_error: str | None = None
        self._stdio_dirs: set[str] = set()
        self._made_dirs: set[str] = set()
        self._resolved: dict[tuple, str] = {}

        self._timer_cv = threading.Condition()
        self._timers: list = []
        self._timer_seq = itertools.count()

        # When the result thread last found the result queue empty, counted once it had applied
        # everything it took before that: a message put before then has been handled.
        self._caught_up = 0.0

        self._outbox_lock = threading.Lock()
        self._outbox: list = []
        # The loop a flush of the outbox is scheduled on, if one is.
        self._flush_loop: asyncio.AbstractEventLoop | None = None

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._ops = ThreadPoolExecutor(max_workers=8, thread_name_prefix="dragon_native_ops")

    # ------------------------------------------------------------------
    # Construction and start
    # ------------------------------------------------------------------

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        """Register states and start the worker pool (once)."""
        if not self._initialized:
            StateMapper.register_backend_states_with_defaults(backend=self)
            StateMapper.register_backend_tasks_states_with_defaults(backend=self)
            self._initialized = True
        async with self._start_lock:
            if not self._started:
                self._loop = asyncio.get_running_loop()
                t0 = time.perf_counter()
                await self._loop.run_in_executor(None, self._start)
                self._started = True
                self.logger.info(
                    f"DragonNativeExecutionBackend: {self.num_workers} workers on "
                    f"{len(self._nodes)} node(s), started in {time.perf_counter() - t0:.2f} s"
                )
        return self

    @classmethod
    async def create(cls, **kwargs):
        """Create and start a DragonNativeExecutionBackend."""
        return await cls(**kwargs)

    @property
    def num_workers(self) -> int:
        """Number of live CPU workers in the pool, over all nodes.

        It falls below the pool's size at start when a replacement fails to start.
        """
        with self._lock:
            return sum(n for (_, gpus), n in self._live_by_key.items() if not gpus)

    def _host_policy(self, host: str, cpus=(), gpus=()):
        return Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=host,
            cpu_affinity=list(cpus),
            gpu_affinity=list(gpus),
        )

    def _start(self) -> None:
        """Query the nodes, create the result queue and start the pool.

        Runs off the loop.
        """
        self._client_host = current_node().hostname
        for h_uid in System().nodes:
            node = Node(h_uid)
            self._nodes[node.hostname] = {
                "cpus": list(node.cpus or []),
                "gpus": list(node.gpus or []),
                "num_cpus": node.num_cpus,
                "h_uid": node.h_uid,
            }
            self._hosts_by_huid[node.h_uid] = node.hostname
        self._host_order = [self._client_host] + [h for h in self._nodes if h != self._client_host]
        total_cpus = sum(len(n["cpus"]) or n["num_cpus"] for n in self._nodes.values())
        self._max_process_ranks = self._max_process_ranks_arg or max(1, total_cpus)

        # The workers' code travels by value, so they never import rhapsody.
        cloudpickle.register_pickle_by_value(_worker)
        cloudpickle.register_pickle_by_value(_measure)  # the workers' one rhapsody import
        here = self._host_policy(self._client_host)
        self._results = Queue(maxsize=_RESULT_SIZE, block_size=_BLOCK_SIZE, policy=here)
        self._control = Queue(maxsize=_CONTROL_SIZE, block_size=_BLOCK_SIZE, policy=here)
        self._inbound = Queue(maxsize=_INBOUND_SIZE, block_size=_BLOCK_SIZE, policy=here)
        try:
            relay = Process(
                _worker.relay_main,
                args=(
                    self._control,
                    self._results,
                    self._inbound,
                    _WATCH_INTERVAL,
                    _SEND_TIMEOUT,
                    _RELAY_LINGER,
                ),
                policy=here,
            )
            relay.start()
        except Exception as exc:
            self._stop()
            raise RuntimeError(
                f"DragonNativeExecutionBackend failed to start: its relay did not start: {exc!r}"
            ) from exc
        self._relay = relay
        for target, name in (
            (self._drain_results, "dragon_native_results"),
            (self._watch_timers, "dragon_native_timers"),
            (self._watch_workers, "dragon_native_workers"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

        workers = []
        with self._lock:
            for host in self._host_order:
                info = self._nodes[host]
                cores = _group_by_core(info["cpus"])
                n = self._workers_per_node
                if n is None:
                    n = len(cores) or info["num_cpus"]
                for cpus in _split_cpus(cores, n):
                    workers.append(self._new_worker(host, cpus, (), initial=True))
            self._initial_left = len(workers)
            if not workers:
                self._ready_event.set()
        self._check_descriptors(workers)
        for future in [self._ops.submit(self._launch_worker, w) for w in workers]:
            future.result()
        if not self._ready_event.wait(self._worker_start_timeout):
            self._start_error = self._start_error or (
                f"{self._initial_left} of {len(workers)} workers not ready after "
                f"{self._worker_start_timeout} s"
            )
        if self._start_error:
            error = self._start_error
            self._stop()
            raise RuntimeError(f"DragonNativeExecutionBackend failed to start: {error}")

    def _check_descriptors(self, workers: list) -> None:
        """Warn when a node's pool and process tasks may not fit the limit on open files.

        Local Services holds descriptors for every process it starts and Dragon does not raise the
        soft limit, so past it processes fail to start with ``Too many open files``, deep in a long
        error. The limit read here is this node's, usually the same across a job.
        """
        limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        pools = Counter(w.host for w in workers)
        needed = max(
            _FDS_PER_NODE
            + _FDS_PER_PYTHON_PROCESS * pools[host]
            + (len(info["cpus"]) or info["num_cpus"])
            for host, info in self._nodes.items()
        )
        if limit != resource.RLIM_INFINITY and needed > limit:
            self.logger.warning(
                f"A node may need {needed} file descriptors, for its workers and as many process "
                f"tasks as it has CPUs, and the soft limit is {limit}. Past it, processes fail to "
                "start. Raise it with ulimit -n before starting Dragon."
            )

    def _new_worker(
        self, host: str, cpus: tuple, gpus: tuple, initial: bool, retry=False
    ) -> _Worker:
        """Register a worker that is about to be launched.

        Call with _lock held.
        """
        w = _Worker(next(self._wids), host, tuple(cpus), tuple(gpus), initial, retry)
        self._workers[w.wid] = w
        self._live_by_key[w.key] += 1
        return w

    def _launch_worker(self, w: _Worker) -> None:
        """Create a worker's inbox on its node and start its process.

        A worker launched after the pool started, a replacement or a GPU worker, must report ready
        within ``worker_start_timeout``, as the pool had to at start. Runs off the loop.
        """
        if self._stopping:
            return
        if self._started:
            timer = next(self._timer_seq)
            self._push_timer(self._worker_start_timeout, timer, self._start_expired, w.wid)
        try:
            w.inbox = Queue(
                maxsize=_INBOX_SIZE, block_size=_BLOCK_SIZE, policy=self._host_policy(w.host)
            )
            # Before the worker can report ready: the relay must know its inbox by then.
            self._send_control([("inbox", w.wid, w.inbox)])
            proc = Process(
                _worker.worker_main,
                args=(w.wid, w.inbox, self._results, self._worker_oom_score_adj),
                policy=self._host_policy(w.host, w.cpus, w.gpus),
            )
            proc.start()
        except Exception as exc:
            self.logger.error(f"Cannot start worker {w.wid} on {w.host}: {exc}", exc_info=True)
            self._worker_died(w, f"could not start: {exc!r}")
            return
        with self._lock:
            w.proc = proc
            if w.state == "dead" or self._stopping:  # cancelled or shut down while it started
                kill = True
            else:
                self._workers_by_puid[proc.puid] = w
                kill = False
        if kill:
            with contextlib.suppress(Exception):
                proc.kill()

    # ------------------------------------------------------------------
    # Background threads
    # ------------------------------------------------------------------

    def _drain_results(self) -> None:
        """Take the relay's batches of results, handle each, and repeat until a ``None`` arrives.

        One batch is handled in one pass: one get, one batch of sends to the relay, and one wake
        of the asyncio loop, however many tasks it holds. ``_caught_up`` moves on to the time the
        relay found the result queue empty, once everything it took before then is handled.
        """
        while True:
            try:
                raw = self._inbound.get(timeout=_WATCH_INTERVAL)
            except queue.Empty:
                if self._stop_event.is_set():
                    return
                continue
            except Exception as exc:
                if self._stop_event.is_set():
                    return
                self.logger.exception(f"Result queue get failed: {exc}")
                time.sleep(0.1)
                continue
            if raw is None:
                return
            drained_at = None
            try:
                items, drained_at = cloudpickle.loads(raw)
                if self._handle_messages(items):
                    return
            except Exception:
                self.logger.exception("Could not handle result messages")
                self._flush_sends(ack=True)  # the relay sends nothing more until it has one
            if drained_at is not None:
                self._caught_up = drained_at

    def _handle_messages(self, items: list) -> bool:
        """Apply result messages; return True when the stop sentinel is among them."""
        stop = False
        msgs = []
        for raw in items:
            if raw is None:
                stop = True
                continue
            try:
                msgs.append(_load_value(cloudpickle.loads(raw)))
            except Exception as exc:
                self.logger.error(f"Cannot unpickle a result message: {exc!r}")
        completions: list = []
        with self._lock:
            for msg in msgs:
                try:
                    self._apply_message(msg, completions)
                except Exception:
                    self.logger.exception(f"Could not apply a {msg[0]} message")
        self._flush_sends(ack=True)
        self._emit_all(completions)
        return stop

    def _apply_message(self, msg: tuple, completions: list) -> None:
        kind = msg[0]
        if kind == _worker.DONE:
            self._on_done(msg, completions)
        elif kind == _worker.READY:
            self._on_ready(msg)
        elif kind == _worker.UNSENT:
            self._on_unsent(msg)
        elif kind == _worker.RESULT:
            _, uid, ok, value, tb = msg
            rec = self._tasks.get(uid)
            if rec is not None:
                rec.result = (ok, value, tb)
                rec.result_event.set()

    def _on_ready(self, msg) -> None:
        _, wid, hostname, pid, *rest = msg
        w = self._workers.get(wid)
        if w is None or w.state != "starting":
            return
        w.node, w.pid = hostname, pid
        w.oom_score_adj = rest[0] if rest else None
        if w.initial and self._initial_left > 0:
            self._initial_left -= 1
            if self._initial_left == 0:
                self._ready_event.set()
        self._feed(w)

    def _on_done(self, msg, completions: list) -> None:
        _, wid, uid, ok, value, tb, out, err, *rest = msg
        w = self._workers.get(wid)
        if w is None or w.state == "dead":
            return  # killed for a cancel or timeout; its task was already reported
        rec, w.task = w.task, None
        w.backoff = _RETRY_FIRST
        if rec is not None and rest and rest[0] != rec.seq:
            self.logger.debug(f"Worker {wid} numbered {uid} {rest[0]}, not {rec.seq}")
        if rec is not None and rec.uid == uid and self._tasks.get(uid) is rec:
            del self._tasks[uid]
            rec.state = "done"
            observed = None
            if rec.measured and isinstance(value, tuple) and len(value) == 2:
                # (value or error, observed); an error replaced in transit comes alone
                value, observed = value
            if ok:
                fields = {"return_value": value, "stdout": out, "stderr": err}
                completions.append((rec, "DONE", fields))
            else:
                fields = {"exception": value, "stdout": out, "stderr": err or tb or str(value)}
                completions.append((rec, "FAILED", fields))
            if isinstance(observed, dict):
                fields["observed"] = observed
        self._feed(w)

    def _on_unsent(self, msg) -> None:
        """Take back a task the relay could not put on its worker's inbox, and do what ``_send``
        did with a failed put before the relay: wait for room, or replace the worker."""
        _, wid, uid, kind, error_text, payload = msg
        w = self._workers.get(wid)
        if w is None or w.state != "busy" or w.task is None or w.task.uid != uid:
            return  # its worker was killed for a cancel or a timeout, or died: handled there
        rec, w.task = w.task, None
        rec.worker = None
        rec.timer = None  # its timeout started with a send that did not happen
        rec.state = "queued"
        rec.payload = payload
        if kind == "full":
            if rec.put_deadline is None:
                rec.put_deadline = time.monotonic() + _worker.PUT_PATIENCE
            if time.monotonic() >= rec.put_deadline:
                self._fail_unsent(rec, MemoryError(f"task {uid} could not be sent: {error_text}"))
                self._feed(w)
                return
            self._pending[rec.key].appendleft(rec)
            w.state = "paused"
            self._add_retry(w)
            return
        if kind == "timeout":
            error: Exception = TimeoutError(
                f"sending to worker {w.wid} on {w.host} took over {_SEND_TIMEOUT} s"
            )
        else:
            error = RuntimeError(f"cannot send to worker {w.wid} on {w.host}: {error_text}")
        self.logger.warning(f"Worker {w.wid} on {w.host} cannot be sent tasks: {error}")
        w.state = "dead"
        self._ops.submit(self._retire_worker, w, None)
        if rec.put_failed_on is not None:
            self._fail_unsent(rec, error)
            return
        rec.put_failed_on = w.host
        self._route(rec, first=True)

    def _watch_timers(self) -> None:
        """Time out pool tasks at their deadline, and send paused workers their next task.

        Process tasks time out in their own thread.
        """
        while True:
            expired = []
            with self._timer_cv:
                if self._stop_event.is_set():
                    return
                now = time.monotonic()
                while self._timers and self._timers[0][0] <= now:
                    expired.append(heapq.heappop(self._timers)[1:])
                if not expired:
                    wait = self._timers[0][0] - now if self._timers else None
                    self._timer_cv.wait(wait)
                    continue
            for timer, handler, arg in expired:
                try:
                    handler(arg, timer)
                except Exception:
                    self.logger.exception(f"Timer {handler.__name__} failed for {arg}")

    def _push_timer(self, delay: float, timer: int, handler: Callable, arg) -> None:
        """Call ``handler(arg, timer)`` on the timer thread in ``delay`` seconds."""
        with self._timer_cv:
            heapq.heappush(self._timers, (time.monotonic() + delay, timer, handler, arg))
            self._timer_cv.notify()

    def _add_timer(self, rec: _Task, timeout: float) -> None:
        """Arm a timeout by uid, so the heap does not keep a finished task's result alive."""
        rec.timer = next(self._timer_seq)
        self._push_timer(timeout, rec.timer, self._timeout_pool_task, rec.uid)

    def _add_retry(self, w: _Worker) -> None:
        """Feed a paused worker again after its backoff, and double the backoff."""
        delay, w.backoff = w.backoff, min(2 * w.backoff, _RETRY_MAX)
        self._push_timer(delay, next(self._timer_seq), self._retry_send, w.wid)

    def _retry_send(self, wid: int, _timer: int) -> None:
        with self._lock:
            w = self._workers.get(wid)
            if w is not None and w.state == "paused":
                self._feed(w)
        self._flush_sends()

    def _start_expired(self, wid: int, _timer: int) -> None:
        """Count a worker that is still starting at its deadline as dead, and kill it.

        Its process may be alive but stuck, say on a hung node: until it is gone, it would count as
        live, and the tasks queued for it would wait for good.
        """
        with self._lock:
            w = self._workers.get(wid)
            if w is None or w.state != "starting" or self._stopping:
                return
            self._worker_died(w, f"was not ready {self._worker_start_timeout} s after its launch")
        if w.proc is not None:
            with contextlib.suppress(Exception):
                w.proc.kill()

    def _watch_workers(self) -> None:
        """Find workers, and the relay, that died on their own, with a non-blocking join once a
        second.

        A blocking join would keep Python's garbage collector off in this whole process while it
        waits: Dragon's ``gs_request`` disables it for the duration of every request.
        """
        while not self._stop_event.wait(_WATCH_INTERVAL):
            with self._lock:
                puids = list(self._workers_by_puid)
            relay = None
            if self._relay is not None and self._relay_error is None:
                relay = self._relay.puid
                puids.append(relay)
            if not puids:
                continue
            try:
                exited, _ = multi_join(puids, timeout=0, join_all=False)
            except Exception as exc:
                if not self._stop_event.is_set():
                    self.logger.warning(f"Worker check failed: {exc!r}")
                continue
            for puid, code in exited or ():
                if puid == relay:
                    if not self._stopping:
                        self._relay_lost(f"exited with code {code}")
                    continue
                with self._lock:
                    w = self._workers_by_puid.pop(puid, None)
                if w is not None and not self._stopping:
                    self._worker_died(w, f"exited with code {code}", code)

    def _relay_lost(self, why: str) -> None:
        """Fail every pool task: none can be sent, or heard from, without the relay.

        Pool tasks submitted from now on fail at once, and so do function processes, whose results
        came through the relay too. Executables and process groups still run.
        """
        error = RuntimeError(f"the Dragon native backend's relay process {why}")
        with self._lock:
            if self._relay_error is not None:
                return
            self._relay_error = str(error)
            lost = [rec for rec in self._tasks.values() if rec.kind == "pool"]
            for rec in lost:
                del self._tasks[rec.uid]
                rec.state = "done"
                if rec.worker is not None:
                    rec.worker.task = None
            for pending in self._pending.values():
                pending.clear()
        self.logger.error(f"{error}: its pool tasks fail, and so will new ones")
        self._emit_all(
            [(rec, "FAILED", {"exception": error, "stderr": str(error)}) for rec in lost]
        )

    # ------------------------------------------------------------------
    # Pool scheduling (call with _lock held)
    # ------------------------------------------------------------------

    def _idle_pop(self, key: tuple, avoid: str | None = None) -> _Worker | None:
        """Take an idle worker for ``key``.

        For an unplaced task, hosts other than ``avoid`` come first.
        """
        host, gpus = key
        if gpus or host is not None:
            idle = self._idle.get(key)
            return idle.popleft() if idle else None
        hosts = self._host_order
        if avoid is not None:
            hosts = [h for h in hosts if h != avoid] + [avoid]
        for h in hosts:
            idle = self._idle.get((h, ()))
            if idle:
                return idle.popleft()
        return None

    def _next_for(self, w: _Worker) -> _Task | None:
        keys = (w.key,) if w.gpus else (w.key, (None, ()))
        for key in keys:
            pending = self._pending.get(key)
            if pending:
                return pending.popleft()
        return None

    def _feed(self, w: _Worker) -> None:
        """Give ``w`` its next task, or mark it idle."""
        if self._stopping:
            w.state = "idle"
            return
        rec = self._next_for(w)
        if rec is None:
            w.state = "idle"
            self._idle[w.key].append(w)
            return
        self._send(w, rec)

    def _send(self, w: _Worker, rec: _Task) -> None:
        """Give ``rec`` to ``w``: the relay puts it on ``w``'s inbox with the next batch.

        ``w`` runs ``rec`` from now on, as far as the backend knows. If the relay's put fails, its
        ``UNSENT`` message takes the task back (``_on_unsent``).
        """
        self._outgoing.append(("send", w.wid, rec.uid, rec.payload))
        w.state = "busy"
        w.task = rec
        # The worker numbers what it takes from its inbox the same way
        w.sent += 1
        rec.seq = w.sent
        rec.sent_at = time.time()
        rec.worker = w
        rec.state = "running"
        rec.payload = None
        if rec.timeout is not None:
            self._add_timer(rec, rec.timeout)

    def _flush_sends(self, ack: bool = False) -> None:
        """Send the relay, in one batch, the tasks given to workers since the last batch.

        With ``ack``, the batch also tells the relay that the client has handled the batch of
        results it took last, so it may send the next. Call without _lock held.
        """
        with self._control_lock:
            with self._lock:
                ops, self._outgoing = self._outgoing, []
            if ack:
                ops.append(("ack",))
            if ops:
                self._send_control(ops, locked=True)

    def _send_control(self, ops: list, locked: bool = False) -> None:
        """Put one batch of operations on the relay's queue; a relay that cannot take it is lost.

        A full memory pool is waited out, as workers wait for room for their results.
        """
        if self._relay_error is not None:
            return
        raw = cloudpickle.dumps(ops)
        with contextlib.nullcontext() if locked else self._control_lock:
            try:
                deadline = time.monotonic() + _worker.PUT_PATIENCE
                while True:
                    try:
                        self._control.put(raw, timeout=_SEND_TIMEOUT)
                        return
                    except _worker.PoolFull:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(_RETRY_FIRST)
            except Exception as exc:
                if not self._stopping:
                    self._relay_lost(f"cannot be sent tasks: {exc!r}")

    def _fail_unsent(self, rec: _Task, error: BaseException) -> None:
        """Fail a pool task that no worker has."""
        self._tasks.pop(rec.uid, None)
        rec.state = "done"
        self._emit(rec, "FAILED", {"exception": error, "stderr": str(error)})

    def _can_serve(self, key: tuple) -> bool:
        """Whether a live CPU worker exists for a non-GPU key."""
        host, _ = key
        if host is not None:
            return self._live_by_key[key] > 0
        return any(self._live_by_key[(h, ())] > 0 for h in self._host_order)

    def _route(self, rec: _Task, first: bool = False) -> None:
        """Send ``rec`` to an idle worker that can run it, or queue it, at the head when ``first``.

        Idle workers do not look at the queue, so a task waits there only while none of them can
        take it. A task whose inbox turned out broken comes back here, and goes to another host when
        it is not placed.
        """
        if self._relay_error is not None:
            self._fail_unsent(rec, RuntimeError(self._relay_error))
            return
        w = self._idle_pop(rec.key, rec.put_failed_on)
        if w is not None:
            self._send(w, rec)
            return
        host, gpus = rec.key
        if not gpus and not self._can_serve(rec.key):
            error = RuntimeError(f"task {rec.uid}: no pool worker on {host or 'any node'}")
            self._fail_unsent(rec, error)
            return
        if first:
            self._pending[rec.key].appendleft(rec)
        else:
            self._pending[rec.key].append(rec)
        if gpus and self._live_by_key[rec.key] < self._gpu_workers_per_set:
            w = self._new_worker(host, (), gpus, initial=False)
            self._ops.submit(self._launch_worker, w)

    def _worker_died(self, w: _Worker, why: str, code: int | None = None) -> None:
        """Handle a worker that died without being asked to: fail its task, maybe replace it.

        ``code`` is its exit code, when known (negative: the signal that killed it).
        """
        failed = []
        replace = None
        host_lost = False
        with self._lock:
            if w.state == "dead":
                return  # retired for a cancel, a timeout or its inbox, or found dead already
            was = w.state
            w.state = "dead"
            self._workers.pop(w.wid, None)
            self._live_by_key[w.key] -= 1
            if w.proc is not None:
                self._workers_by_puid.pop(w.proc.puid, None)
            with contextlib.suppress(ValueError):
                self._idle[w.key].remove(w)
            rec, w.task = w.task, None
            if rec is not None and self._tasks.get(rec.uid) is rec:
                del self._tasks[rec.uid]
                rec.state = "done"
                error = RuntimeError(
                    f"Dragon worker {w.wid} on {w.host} {why} while running {rec.uid}"
                )
                failed.append((rec, error, self._died_observed(rec, w, code)))
            if was == "starting":
                if w.initial and self._initial_left > 0:
                    self._start_error = f"worker {w.wid} on {w.host} {why} before it was ready"
                    self._ready_event.set()
                # Nothing left to serve this key: fail what waits for it.
                if self._live_by_key[w.key] == 0 and not self._stopping:
                    host_lost = not w.gpus and self._started
                    keys = [w.key]
                    if not self._can_serve((None, ())):
                        keys.append((None, ()))
                    for key in keys:
                        for pending in self._pending.pop(key, ()):
                            self._tasks.pop(pending.uid, None)
                            pending.state = "done"
                            error = RuntimeError(
                                f"no Dragon worker for {key}: worker {w.wid} {why}"
                            )
                            failed.append((pending, error, None))
            elif not self._stopping and (not w.gpus or self._pending.get(w.key)):
                replace = self._new_worker(w.host, w.cpus, w.gpus, w.initial, retry=True)
            if was == "starting" and w.retry:
                self.logger.error(f"Replacement worker {w.wid} on {w.host} {why}; not replacing it")
        self.logger.warning(f"Dragon worker {w.wid} on {w.host} {why}")
        if host_lost:
            self.logger.error(
                f"No pool worker is left on {w.host}: function tasks placed there now fail"
            )
        for rec, error, observed in failed:
            fields = {"exception": error, "stderr": str(error)}
            if observed is not None:
                fields["observed"] = observed
            self._emit(rec, "FAILED", fields)
        self._ops.submit(self._discard_worker_queue, w)
        if replace is not None:
            self._ops.submit(self._launch_worker, replace)

    def _discard_worker_queue(self, w: _Worker) -> None:
        if w.inbox is not None:
            if not self._stopping:
                self._send_control([("forget", w.wid)])
            with contextlib.suppress(Exception):
                w.inbox.destroy()

    def _retire_worker(self, w: _Worker, token: str | None) -> None:
        """Kill a worker and replace it.

        A busy one is killed for a cancel or timeout, and the children of its task, which carry
        ``token``, too; an idle one for its inbox, with no ``token``.
        """
        if w.proc is not None:
            with contextlib.suppress(Exception):
                w.proc.kill()
        if token is not None:
            self._reap(w.host, token)
        if w.proc is not None:
            with contextlib.suppress(Exception):
                w.proc.join(timeout=5.0)
        with self._lock:
            if w.proc is not None:
                self._workers_by_puid.pop(w.proc.puid, None)
            self._workers.pop(w.wid, None)
            self._live_by_key[w.key] -= 1
            replace = None
            if not self._stopping and (not w.gpus or self._pending.get(w.key)):
                replace = self._new_worker(w.host, w.cpus, w.gpus, w.initial)
        self._discard_worker_queue(w)
        if replace is not None:
            self._launch_worker(replace)

    def _timeout_pool_task(self, uid: str, timer: int) -> None:
        with self._lock:
            rec = self._tasks.get(uid)
            if rec is None or rec.timer != timer or rec.state != "running":
                return
            del self._tasks[uid]
            rec.state = "done"
            w, rec.worker = rec.worker, None
            w.state = "dead"
            w.task = None
        error = TimeoutError(
            f"task {rec.uid} exceeded its timeout of {rec.timeout} s; its worker was killed"
        )
        fields = {"exception": error, "stderr": str(error)}
        observed = self._died_observed(rec, w, -signal.SIGKILL, timed_out=True)
        if observed is not None:
            fields["observed"] = observed
        self._emit(rec, "FAILED", fields)
        self._ops.submit(self._retire_worker, w, rec.token)

    @staticmethod
    def _died_observed(
        rec: _Task, w: _Worker, code: int | None, timed_out: bool = False
    ) -> dict | None:
        """What is known of a pool task whose worker died or was killed (``observe_peaks``)."""
        if not rec.measured:
            return None
        return _killed_observed(rec.sent_at, w.node or w.host, None, code, timed_out)

    # ------------------------------------------------------------------
    # Process and group tasks
    # ------------------------------------------------------------------

    def _fits(self, rec: _Task) -> bool:
        """Whether ``rec`` can run now; a task larger than the limit runs alone."""
        return not self._running_ranks or (
            self._running_ranks + rec.ranks <= self._max_process_ranks
        )

    def _admit_process(self, rec: _Task) -> None:
        """Start a process or group task now, or queue it, first in, first out."""
        if self._proc_queue or not self._fits(rec):
            self._proc_queue.append(rec)
        else:
            self._start_process_task(rec)

    def _start_process_task(self, rec: _Task) -> None:
        """Give a process or group task its thread; a task that cannot have one fails."""
        thread = threading.Thread(
            target=self._run_process_task, args=(rec,), name=f"dragon_native_{rec.uid}", daemon=True
        )
        self._running_ranks += rec.ranks
        rec.state = "running"
        try:
            thread.start()
        except RuntimeError as exc:  # the node's limit on threads
            self._running_ranks -= rec.ranks
            self._tasks.pop(rec.uid, None)
            rec.state = "done"
            self._emit(rec, "FAILED", {"exception": exc, "stderr": str(exc)})
            return
        rec.thread = thread

    def _run_process_task(self, rec: _Task) -> None:
        try:
            if rec.kind == "group":
                state, fields = self._run_group(rec)
            else:
                state, fields = self._run_single(rec)
        except Exception as exc:
            self.logger.debug(f"Task {rec.uid} failed in the backend", exc_info=True)
            state, fields = "FAILED", {"exception": exc, "stderr": str(exc)}
        finally:
            with self._lock:
                self._running_ranks -= rec.ranks
                while self._proc_queue and not self._stopping:
                    if not self._fits(self._proc_queue[0]):
                        break
                    self._start_process_task(self._proc_queue.popleft())
        if rec.inline_files and (rec.cancelled or "stdout" not in fields):
            for path in rec.stdio:
                _read_and_remove(path)
        if rec.peak_dir is not None:
            try:
                self._collect_observed(rec, state, fields)
            except Exception:  # costs the task its observed, never its final state
                self.logger.exception(f"Could not read what {rec.uid} used")
        self._finish(rec, state, fields)

    def _collect_observed(self, rec: _Task, state: str, fields: dict) -> None:
        """Combine the records a process or group task's processes wrote into ``observed``.

        Removes the task's record directory either way. A FAILED task that did not get a record from
        each of its processes, say one killed with SIGKILL, reports what is known instead: its
        start, its end, the hosts that wrote a record, and how it ended.
        """
        records = _peak.read_records(rec.peak_dir) or []  # type: ignore[arg-type]
        observed = _peak.combine(records, rec.ranks)
        if observed is None:
            if state in ("DONE", "FAILED"):
                self.logger.debug(f"No complete peaks for {rec.uid} in {rec.peak_dir}")
            if state == "FAILED" and rec.started_at is not None:
                hosts = sorted({r["host"] for r in records if r.get("host")})
                fields["observed"] = _killed_observed(
                    rec.started_at, None, hosts, fields.get("exit_code"), rec.timed_out
                )
        elif state in ("DONE", "FAILED"):
            fields["observed"] = observed

    def _stdio_of(self, rec: _Task) -> tuple[str, str]:
        if rec.inline_files:
            return _read_and_remove(rec.stdio[0]), _read_and_remove(rec.stdio[1])
        return rec.stdio[0] or "", rec.stdio[1] or ""

    @staticmethod
    def _exit_error(rec: _Task, code: int, stderr: str) -> TaskExecutionError:
        """The error a ``Session`` raises for a non-zero exit, set on the task itself.

        AsyncFlow reads only ``exception`` and ``stderr``, so without it an executable that fails
        with nothing on stderr reports no exit code.
        """
        if rec.inline_files:
            detail = stderr.strip()[-2000:] or "nothing on stderr"
        elif rec.stdio[1]:
            detail = f"stderr in {rec.stdio[1]}"
        else:
            detail = "stderr not captured"
        return TaskExecutionError(rec.uid, detail, code)

    def _run_single(self, rec: _Task) -> tuple[str, dict]:
        proc = rec.proc
        with self._lock:
            if rec.cancelled:
                return "CANCELED", {}
        rec.started_at = time.time()
        proc.start()
        with self._lock:
            cancelled = rec.cancelled
            rec.puids = [proc.puid]
        if cancelled:
            self._kill_process_task(rec)
        try:
            code = proc.join(timeout=rec.timeout)
        except Exception:  # the task fails with the error: its process must not run on
            self._kill_process_task(rec)
            raise
        if code is None:
            with self._lock:
                rec.timed_out = not rec.cancelled
            self._kill_process_task(rec)
            code = proc.join(timeout=10.0)
        exited_at = time.monotonic()
        if rec.cancelled:
            return "CANCELED", {}
        stdout, stderr = self._stdio_of(rec)
        fields = {"stdout": stdout, "stderr": stderr, "exit_code": code}
        if rec.timed_out:
            fields["exception"] = TimeoutError(
                f"task {rec.uid} exceeded its timeout of {rec.timeout} s (exit code {code})"
            )
            return "FAILED", fields
        if not rec.is_function:
            fields["return_value"] = code
            if code == 0:
                return "DONE", fields
            fields["exception"] = self._exit_error(rec, code, stderr)
            return "FAILED", fields
        if self._result_arrives(rec, exited_at):
            ok, value, tb = rec.result  # type: ignore[misc]
            if ok:
                fields["return_value"] = value
                if code == 0:
                    return "DONE", fields
                fields["exception"] = self._exit_error(rec, code, stderr)
                return "FAILED", fields
            fields["exception"] = value
            fields["stderr"] = stderr or tb
            return "FAILED", fields
        fields["exception"] = RuntimeError(
            f"function process of task {rec.uid} exited with code {code} without a result"
        )
        fields["stderr"] = stderr or str(fields["exception"])
        return "FAILED", fields

    def _result_arrives(self, rec: _Task, exited_at: float) -> bool:
        """Whether a function process that exited before ``exited_at`` sent a result.

        The process flushed its result into the result queue before it exited. The result can
        still wait behind a backlog of others, however long, but once the result thread has been
        through everything queued before ``exited_at``, a result that has not come never will.
        """
        while not rec.result_event.wait(_WATCH_INTERVAL):
            if (
                self._caught_up > exited_at
                or rec.cancelled
                or self._stop_event.is_set()
                or self._relay_error is not None
            ):
                return rec.result_event.is_set()
        return True

    def _run_group(self, rec: _Task) -> tuple[str, dict]:
        with self._lock:
            if rec.cancelled:
                return "CANCELED", {}
        group = ProcessGroup(
            restart=False, ignore_error_on_exit=True, pmi=rec.pmi, name=f"rhapsody.{rec.uid}"
        )
        for nranks, template in rec.templates:
            group.add_process(nranks, template)
        group.init()
        try:
            rec.started_at = time.time()
            group.start()
            puids = list(group.puids)
            with self._lock:
                rec.group = group
                rec.puids = puids
                cancelled = rec.cancelled
            if cancelled:
                self._kill_process_task(rec)
            try:
                group.join(timeout=rec.timeout)
            except Exception:  # a timeout, as TimeoutError or as a manager error
                if rec.timeout is None:
                    # The task fails with the error. close() stops the group without a SIGKILL,
                    # and does not reach the ranks' children: kill and reap them first.
                    self._kill_process_task(rec)
                    raise
                with self._lock:
                    rec.timed_out = not rec.cancelled
                self._kill_process_task(rec)
                group.join(timeout=10.0)
            codes = [code for _, code in sorted(group.exit_status)]
        finally:
            self._close_group(group)
        if rec.cancelled:
            return "CANCELED", {}
        stdout, stderr = self._stdio_of(rec)
        exit_code = next((c for c in codes if c not in (0, None)), 0)
        fields = {"stdout": stdout, "stderr": stderr, "exit_code": exit_code, "return_value": codes}
        if rec.timed_out:
            fields["exception"] = TimeoutError(
                f"task {rec.uid} exceeded its timeout of {rec.timeout} s (exit codes {codes})"
            )
            return "FAILED", fields
        if exit_code == 0:
            return "DONE", fields
        fields["exception"] = self._exit_error(rec, exit_code, stderr)
        return "FAILED", fields

    def _close_group(self, group) -> None:
        """Close a ProcessGroup with a deadline; ``close()`` can loop while its manager hangs."""
        closer = threading.Thread(target=group.close, name="dragon_native_pg_close", daemon=True)
        closer.start()
        closer.join(timeout=30.0)
        if closer.is_alive():
            self.logger.warning("ProcessGroup.close() did not return within 30 s")

    def _kill_process_task(self, rec: _Task) -> None:
        """SIGTERM a task's processes, SIGKILL them after kill_grace, then kill their children.

        Signals go to each process by puid: Dragon 0.15.0's ``ProcessGroup.stop()`` never sends its
        SIGKILL. Dragon signals only the task's own processes, so a reaper on each host then kills
        every process that still carries the task's token.
        """
        puids = list(rec.puids)
        if not puids:
            return
        for puid in puids:
            with contextlib.suppress(Exception):
                process_kill(puid, sig=15)
        # The reaper's hosts, looked up after the signals: one request to Global Services each.
        hosts = set()
        for puid in puids:
            with contextlib.suppress(Exception):
                hosts.add(self._hosts_by_huid.get(Process(None, ident=puid).h_uid))
        try:
            exited, _ = multi_join(puids, timeout=self._kill_grace, join_all=True)
        except Exception:
            exited = None
        if not exited or len(exited) < len(puids):
            for puid in puids:
                with contextlib.suppress(Exception):
                    process_kill(puid, sig=9)
        for host in hosts if None not in hosts else self._host_order:
            self._reap(host, rec.token)  # type: ignore[arg-type]

    def _reap(self, host: str, token: str, sig: str = "KILL") -> None:
        """Signal every process on ``host`` that still carries ``token`` in its environment."""
        try:
            reaper = Process(
                "/bin/sh",
                args=["-c", _worker.REAP_SCRIPT, "reap", f"{_worker.TOKEN_ENV}={token}", sig],
                stdout=ProcessTemplate.DEVNULL,
                stderr=ProcessTemplate.DEVNULL,
                policy=self._host_policy(host),
            )
            reaper.start()
            reaper.join(timeout=10.0)
        except Exception as exc:
            self.logger.warning(f"Could not reap the children of {token} on {host}: {exc!r}")

    # ------------------------------------------------------------------
    # Delivery to the asyncio loop
    # ------------------------------------------------------------------

    def _finish(self, rec: _Task, state: str, fields: dict) -> None:
        with self._lock:
            if self._tasks.get(rec.uid) is not rec:
                return  # cancelled, or reported already
            del self._tasks[rec.uid]
            rec.state = "done"
        self._emit(rec, state, fields)

    def _emit(self, rec: _Task, state: str, fields: dict) -> None:
        """Queue a state change for the loop; wake the loop once per batch of changes."""
        self._emit_all([(rec, state, fields)])

    def _emit_all(self, changes: list) -> None:
        """Queue ``(rec, state, fields)`` changes for the loop, and wake it at most once."""
        if not changes:
            return
        loop = self._loop
        with self._outbox_lock:
            for rec, state, fields in changes:
                if state != "CANCELED":
                    fields.setdefault("stdout", "")
                    fields.setdefault("stderr", "")
                self._outbox.append((rec.desc, state, fields))
                # No repeat joins once a task has left _tasks, which it has before its final state.
                self._outbox.extend((desc, state, fields) for desc in rec.repeats)
            # A flush scheduled on a loop that has closed, or that is no longer the backend's,
            # may never run: schedule one on the current loop.
            flush_loop = self._flush_loop
            if flush_loop is not None and flush_loop is loop and not flush_loop.is_closed():
                return
            self._flush_loop = loop
        try:
            loop.call_soon_threadsafe(self._flush_outbox)  # type: ignore[union-attr]
        except RuntimeError:
            with self._outbox_lock:  # the next state wakes the next loop, and this one goes with it
                if self._flush_loop is loop:
                    self._flush_loop = None
            self.logger.warning(f"Event loop closed; holding {len(changes)} task state(s)")

    def _flush_outbox(self) -> None:
        """Apply queued state changes and run the callback.

        Runs on the asyncio loop.
        """
        with self._outbox_lock:
            items, self._outbox = self._outbox, []
            self._flush_loop = None
        for desc, state, fields in items:
            desc.update(fields)
            try:
                self._callback_func(desc, state)
            except Exception:
                self.logger.exception(f"Callback failed for {desc.get('uid')} ({state})")

    # ------------------------------------------------------------------
    # Task translation
    # ------------------------------------------------------------------

    def _check_policy(self, policy, uid: str) -> None:
        """Reject a host or GPU this runtime does not have: an unknown host aborts Dragon."""
        if policy is None:
            return
        host = None
        if policy.placement == Policy.Placement.HOST_NAME:
            host = policy.host_name
            if host not in self._nodes:
                raise ValueError(
                    f"task {uid}: host {host!r} is not a Dragon node; nodes: {sorted(self._nodes)}"
                )
        elif policy.placement == Policy.Placement.HOST_ID:
            host = self._hosts_by_huid.get(policy.host_id)
            if host is None:
                raise ValueError(f"task {uid}: host id {policy.host_id} is not a Dragon node")
        gpus = set(policy.gpu_affinity or ())
        if not gpus:
            return
        hosts = [host] if host else list(self._nodes)
        if not any(gpus <= set(self._nodes[h]["gpus"]) for h in hosts):
            where = f"Dragon node {host!r}" if host else "any Dragon node"
            listed = {h: self._nodes[h]["gpus"] for h in hosts}
            raise ValueError(f"task {uid}: GPUs {sorted(gpus)} are not on {where}; GPUs: {listed}")

    def _placement_policy(self, placement: dict | None, uid: str):
        """Translate AsyncFlow's ``placement`` into a host-name policy, or None."""
        if not placement or not placement.get("host"):
            return None
        policy = Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=placement["host"],
            gpu_affinity=list(placement.get("gpus") or []),
        )
        self._check_policy(policy, uid)
        return policy

    def _choose_policy(self, template_policy, placement_policy, uid: str):
        if template_policy is not None:
            if placement_policy is not None:
                self.logger.warning(f"Task {uid}: process template policy overrides placement")
            self._check_policy(template_policy, uid)
            return template_policy
        return placement_policy

    def _pool_key(self, template, placement_policy, uid: str) -> tuple | None:
        """The pool key ``(host, gpus)`` of a function task, or None if it needs its own process."""
        if template is None:
            policy = placement_policy
        elif isinstance(template, dict) and set(template) == {"policy"}:
            policy = self._choose_policy(template["policy"], placement_policy, uid)
        else:
            return None
        if policy is None:
            return (None, ())
        if policy.cpu_affinity:
            return None
        host = None
        if policy.placement == Policy.Placement.HOST_NAME:
            host = policy.host_name
        elif policy.placement == Policy.Placement.HOST_ID:
            host = self._hosts_by_huid[policy.host_id]
        gpus = tuple(policy.gpu_affinity or ())
        if gpus and host is None:
            host = next(h for h in self._host_order if set(gpus) <= set(self._nodes[h]["gpus"]))
        return (host, gpus)

    def _peak_dir(self, uid: str) -> str:
        """A fresh record directory for one run of a process or group task (``_peak``)."""
        root = os.path.abspath(os.path.join(self._work_dir, f"rhapsody-peaks.{self._peak_tag}"))
        self._peak_roots.add(root)
        return _peak.make_peak_dir(root, uid)

    def _stdio_paths(self, uid: str, capture: bool) -> tuple[str | None, str | None, bool]:
        """Output files for a process task, and whether they are inline (read back, removed)."""
        if capture:
            directory, inline = self._work_dir, False
        elif self._inline_stdio:
            directory, inline = os.path.join(self._work_dir, f".dragon_native.{self._id}"), True
            self._stdio_dirs.add(directory)
        else:
            return None, None, False
        if directory not in self._made_dirs:
            os.makedirs(directory, exist_ok=True)
            self._made_dirs.add(directory)
        return (
            os.path.join(directory, f"{uid}.stdout"),
            os.path.join(directory, f"{uid}.stderr"),
            inline,
        )

    def _resolve(self, executable: str, cwd: str | None) -> str:
        """The executable's path as Dragon finds it; an unknown name goes to ``sh`` as is."""
        if os.sep in executable:
            return executable
        key = (executable, cwd)
        found = self._resolved.get(key)
        if found is None:
            found = shutil.which(executable)
            if found is None:
                candidate = os.path.join(cwd or os.getcwd(), executable)
                found = candidate if os.path.isfile(candidate) else executable
            self._resolved[key] = found
        return found

    def _process_kwargs(self, rec: _Task, template: dict, policy, fn, args, capture: bool):
        """Process/ProcessTemplate keyword arguments for one process of a task."""
        template = {k: v for k, v in template.items() if k not in _OWN_TEMPLATE_KEYS}
        template["policy"] = policy
        template["env"] = {**(template.get("env") or {}), _worker.TOKEN_ENV: rec.token}
        user_stdio = "stdout" in template or "stderr" in template
        if user_stdio:
            out = err = None
        else:
            if rec.stdio == (None, None):
                out, err, rec.inline_files = self._stdio_paths(rec.uid, capture)
                rec.stdio = (out, err)
            out, err = rec.stdio
        if fn is not None:
            return template, out, err
        exe = self._resolve(rec.desc["executable"], template.get("cwd"))
        arguments = [str(a) for a in args]
        if out is None:
            template.update(target=exe, args=arguments)
        else:
            template.update(
                target="/bin/sh",
                args=["-c", _worker.EXEC_SCRIPT, "sh", out, err, exe, *arguments],
                stdout=ProcessTemplate.DEVNULL,
                stderr=ProcessTemplate.DEVNULL,
            )
        return template, out, err

    def build_task(self, task: dict) -> _Task:  # type: ignore[override]
        """Translate a task description into the backend's record, ready to run.

        Raises on a description the backend cannot run (unknown host or GPU, a value that cannot be
        pickled); the task then fails without reaching Dragon.
        """
        uid = task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs") or {}
        timeout = backend_kwargs.get("timeout")
        if timeout is not None:
            timeout = float(timeout)
            if timeout <= 0:
                raise ValueError(f"task {uid}: timeout must be > 0, got {timeout}")
        is_function = bool(task.get("function"))
        fn = task["function"] if is_function else None
        if not is_function and not task.get("executable"):
            raise ValueError(f"task {uid} has neither a function nor an executable")
        if is_function:
            args, kwargs = tuple(task.get("args") or ()), dict(task.get("kwargs") or {})
        else:
            args, kwargs = tuple(task.get("arguments") or ()), {}
        templates = backend_kwargs.get("process_templates")
        template = backend_kwargs.get("process_template")
        placement_policy = self._placement_policy(task.get("placement"), uid)
        capture = bool(task.get("capture_stdio"))
        token = f"{self._id}.{uid}"

        if templates is None and is_function:
            key = self._pool_key(template, placement_policy, uid)
            if key is not None:
                rec = _Task(uid, task, "pool", timeout, token)
                rec.is_function = True
                rec.key = key
                out = err = None
                if capture:
                    out = os.path.join(self._work_dir, f"{uid}.stdout")
                    err = os.path.join(self._work_dir, f"{uid}.stderr")
                body = cloudpickle.dumps(
                    (token, fn, args, kwargs, out, err, self._inline_stdio, self._observe_peaks)
                )
                rec.payload = cloudpickle.dumps((uid, body))
                rec.measured = self._observe_peaks
                return rec

        peak_dir = self._peak_dir(uid) if self._observe_peaks else None
        if templates is None:
            rec = _Task(uid, task, "process", timeout, token)
            rec.is_function = is_function
            rec.peak_dir = peak_dir
            template = dict(template or {})
            policy = self._choose_policy(template.get("policy"), placement_policy, uid)
            proc_kwargs, out, err = self._process_kwargs(rec, template, policy, fn, args, capture)
            if is_function:
                proc_kwargs["target"] = _worker.run_function_process
                proc_kwargs["args"] = (self._results, uid, fn, args, kwargs, out, err, peak_dir)
            elif peak_dir is not None:
                _launch_under_peak(proc_kwargs, peak_dir)
            rec.proc = Process(**proc_kwargs)
            return rec

        rec = _Task(uid, task, "group", timeout, token)
        rec.is_function = is_function
        rec.peak_dir = peak_dir
        rec.ranks = 0
        for nranks, cfg in templates:
            nranks = int(nranks)
            if nranks < 1:
                raise ValueError(
                    f"task {uid}: a process_templates entry asks for {nranks} processes"
                )
            cfg = dict(cfg or {})
            policy = self._choose_policy(cfg.get("policy"), placement_policy, uid)
            tmpl_kwargs, out, err = self._process_kwargs(rec, cfg, policy, fn, args, capture)
            if is_function:
                tmpl_kwargs["target"] = _worker.run_group_member
                tmpl_kwargs["args"] = (fn, args, kwargs, out, err, peak_dir)
            elif peak_dir is not None:
                _launch_under_peak(tmpl_kwargs, peak_dir)
            rec.templates.append((nranks, ProcessTemplate(**tmpl_kwargs)))
            rec.ranks += nranks
        # Dragon 0.15.0's ProcessGroup.start() never returns for a group with no members.
        if not rec.templates:
            raise ValueError(f"task {uid}: process_templates asks for no processes")
        pmi = backend_kwargs.get("pmi")
        if pmi is None:
            pmi = self._pmi
        rec.pmi = _group_pmi(pmi)
        self._warn_group(uid, rec.ranks, pmi, timeout)
        return rec

    def _warn_group(self, uid: str, nranks: int, pmi, timeout: float | None) -> None:
        """Warn once each for the first group that needs a PMI and the first without a timeout.

        Neither is refused. Several ranks need a PMI to wire up. A rank alone runs without one,
        except under Cray MPICH (``CRAY_MPICH_ENV``), where it can hang in ``MPI_Init`` too; the
        Batch backend also warns for a one-rank function job, because of its own default PMI, which
        a group here does not have. ``pmi="none"`` declares the processes non-MPI, so neither
        warning applies.
        """
        if isinstance(pmi, str) and pmi.lower() == "none":
            return
        cray_mpich = [v for v in CRAY_MPICH_ENV if os.environ.get(v)]
        if pmi is None and (nranks > 1 or cray_mpich) and not self._warned_job_pmi:
            self._warned_job_pmi = True
            ranks = f"{nranks} rank{'' if nranks == 1 else 's'}"
            detected = f"Cray MPICH environment detected ({', '.join(cray_mpich)}). "
            self.logger.warning(
                f"Job {uid} has {ranks} and no PMI, so its processes start without one. "
                f"{detected if cray_mpich else ''}"
                "MPI processes then fail: with Cray MPICH every rank hangs in MPI_Init, and with "
                "Open MPI each rank may run as a one-rank job. Set pmi ('pmix' or 'cray', or "
                "'none' if the processes are not MPI) on DragonNativeExecutionBackend or in the "
                "task's task_backend_specific_kwargs. Logged once."
            )
        # A rank alone has no other rank to wait for it in a collective when it dies.
        if nranks > 1 and timeout is None and not self._warned_job_timeout:
            self._warned_job_timeout = True
            self.logger.warning(
                f"Job {uid} has {nranks} ranks and no timeout. If it is an MPI job and a rank "
                "dies, Dragon does not end it, and the other ranks can wait for that rank for "
                "good. Set timeout (seconds) in the task's task_backend_specific_kwargs, or pmi "
                "'none' if the processes are not MPI. Logged once."
            )

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit tasks.

        Pool tasks are sent to a free worker or queued for one. A task whose uid belongs to a task
        still in the backend joins that task: it does not run, and it gets that task's states,
        RUNNING now and the final state with its result when the task ends. A ``Session`` keeps one
        future per uid, so failing the repeat would fail the first task's future. Once the task has
        ended, its uid runs again.
        """
        if self._backend_state == BackendMainStates.SHUTDOWN:
            raise RuntimeError("Cannot submit during shutdown")
        if not self._started:
            await self._async_init()
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
        self._loop = asyncio.get_running_loop()

        for task in tasks:
            with self._lock:
                running = self._tasks.get(task["uid"])
                joins = False
                if running is not None:
                    joins = all(task is not d for d in (running.desc, *running.repeats))
                    if joins:
                        running.repeats.append(task)
            if running is not None:
                # Its final state goes through the loop, after this RUNNING.
                if joins:
                    self.logger.debug(f"Task {task['uid']} joins the unfinished task of its uid")
                    self._callback_func(task, "RUNNING")
                continue
            try:
                rec = self.build_task(task)
            except Exception as exc:
                self.logger.error(f"Failed to create task {task.get('uid')}: {exc}", exc_info=True)
                task["exception"] = exc
                task["stdout"] = ""
                task["stderr"] = str(exc)
                self._callback_func(task, "FAILED")
                continue
            self._callback_func(task, "RUNNING")
            with self._lock:
                self._tasks[rec.uid] = rec
                if rec.kind == "pool":
                    self._route(rec)
                else:
                    self._admit_process(rec)
        self._flush_sends()

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task: a queued one is dropped, a running one is killed.

        A pool task's worker gets SIGKILL and is replaced; a process task gets SIGTERM, then
        SIGKILL after ``kill_grace``. The task's leftover children are killed too. Returns False
        when the task is unknown or has finished.
        """
        with self._lock:
            rec = self._tasks.pop(uid, None)
            if rec is None:
                return False
            rec.cancelled = True
            worker = None
            if rec.state == "queued":
                pending = self._pending[rec.key] if rec.kind == "pool" else self._proc_queue
                with contextlib.suppress(ValueError):
                    pending.remove(rec)
            elif rec.kind == "pool":
                worker, rec.worker = rec.worker, None
                worker.state = "dead"
                worker.task = None
            rec.state = "done"
            started = bool(rec.puids)
        for desc in (rec.desc, *rec.repeats):
            self._callback_func(desc, "CANCELED")
        if worker is not None:
            self._ops.submit(self._retire_worker, worker, rec.token)
        elif rec.kind != "pool" and started:
            self._ops.submit(self._kill_process_task, rec)
        return True

    async def shutdown(self) -> None:
        """Cancel what still runs, stop the pool and release Dragon resources, with deadlines."""
        if self._backend_state == BackendMainStates.SHUTDOWN:
            return
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.info("Shutting down DragonNativeExecutionBackend")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._stop)
        self._flush_outbox()
        self.logger.info("DragonNativeExecutionBackend shutdown complete")

    def _stop(self) -> None:
        """Stop everything, each step with a deadline.

        Runs off the loop.
        """
        with self._lock:
            self._stopping = True
            records = list(self._tasks.values())
            self._tasks.clear()
            self._pending.clear()
            self._proc_queue.clear()
            busy = []
            for rec in records:
                rec.cancelled = True
                if rec.kind == "pool" and rec.worker is not None:
                    busy.append((rec.worker, rec.token))
                    rec.worker.state = "dead"
                    rec.worker.task = None
            workers = list(self._workers.values())
        if self._loop is not None:
            self._emit_all([(rec, "CANCELED", {}) for rec in records])

        kills = [self._ops.submit(self._kill_worker_now, w, token) for w, token in busy]
        kills += [
            self._ops.submit(self._kill_process_task, rec)
            for rec in records
            if rec.kind != "pool" and rec.puids
        ]
        for w in workers:
            if w.state != "dead" and w.inbox is not None:
                with contextlib.suppress(Exception):
                    w.inbox.put(None, timeout=1.0)
        deadline = time.monotonic() + self._kill_grace + _STOP_KILLS
        for future in kills:
            with contextlib.suppress(Exception):
                future.result(timeout=max(0.0, deadline - time.monotonic()))

        puids = [w.proc.puid for w in workers if w.proc is not None]
        if puids:
            try:
                exited, _ = multi_join(puids, timeout=5.0, join_all=True)
            except Exception:
                exited = None
            if not exited or len(exited) < len(puids):
                for w in workers:
                    if w.proc is not None:
                        with contextlib.suppress(Exception):
                            w.proc.kill()
                with contextlib.suppress(Exception):
                    multi_join(puids, timeout=5.0, join_all=True)

        deadline = time.monotonic() + _STOP_THREADS
        for rec in records:
            if rec.thread is not None:
                rec.thread.join(timeout=max(0.0, deadline - time.monotonic()))

        self._stop_event.set()
        with self._timer_cv:
            self._timer_cv.notify_all()
        # The relay forwards what is left, then the None that ends the result thread.
        for q in (self._control, self._results):
            if q is not None:
                with contextlib.suppress(Exception):
                    q.put(None, timeout=1.0)
        for thread in self._threads:
            thread.join(timeout=5.0)
            if thread.is_alive():
                self.logger.warning(f"Thread {thread.name} did not stop within 5 s")
        if self._relay is not None:
            with contextlib.suppress(Exception):
                if self._relay.join(timeout=5.0) is None:
                    self._relay.kill()

        for w in workers:
            self._discard_worker_queue(w)
        for q in (self._results, self._control, self._inbound):
            if q is not None:
                with contextlib.suppress(Exception):
                    q.destroy()
        for directory in self._stdio_dirs:
            shutil.rmtree(directory, ignore_errors=True)
        for directory in self._peak_roots:
            shutil.rmtree(directory, ignore_errors=True)
        self._peak_roots.clear()
        self._ops.shutdown(wait=False, cancel_futures=True)

    def _kill_worker_now(self, w: _Worker, token: str) -> None:
        if w.proc is not None:
            with contextlib.suppress(Exception):
                w.proc.kill()
        self._reap(w.host, token)

    async def state(self) -> str:  # type: ignore[override]
        """Backend state: INITIALIZED, RUNNING or SHUTDOWN."""
        return self._backend_state.value

    def task_state_cb(self, task: dict, state: str) -> None:
        self._callback_func(task, state)

    def get_task_states_map(self):
        return self._task_states

    def topology(self) -> list[dict[str, Any]]:
        """One entry per Dragon node: its CPUs in the job and its GPUs."""
        return [
            {"id": host, "cores": len(info["cpus"]) or info["num_cpus"], "gpus": len(info["gpus"])}
            for host, info in self._nodes.items()
        ]
