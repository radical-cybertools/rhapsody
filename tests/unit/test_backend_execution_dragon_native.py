"""DragonNativeExecutionBackend against a fake Dragon, so these tests run without a Dragon runtime.

The fake keeps the parts of Dragon's native API the backend uses: ``Process`` (a thread for a
Python target, a real ``/bin/sh`` subprocess for an executable), ``ProcessGroup``, ``Queue``,
``Policy``, ``Node``/``System`` and Global Services' ``multi_join``/``kill``. Pool workers run the
real worker loop of ``_dragon_native_worker`` in threads, with inline output capture off, since
that redirects the process's own file descriptors.

Real-runtime tests are at the end of the module; they need ``dragon python -m pytest``.
"""

import asyncio
import enum
import itertools
import os
import queue
import signal
import subprocess
import sys
import threading
import time
import types
from dataclasses import dataclass
from dataclasses import field

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

from rhapsody.api.errors import TaskExecutionError  # noqa: E402
from rhapsody.backends.discovery import BackendRegistry  # noqa: E402
from rhapsody.backends.execution import _dragon_native_worker as worker_module  # noqa: E402
from rhapsody.backends.execution import dragon_native  # noqa: E402
from rhapsody.backends.execution.dragon_native import DragonNativeExecutionBackend  # noqa: E402
from rhapsody.backends.execution.dragon_native import _group_by_core  # noqa: E402
from rhapsody.backends.execution.dragon_native import _split_cpus  # noqa: E402

# ============================================================================
# Fake Dragon
# ============================================================================


class FakePlacement(enum.IntEnum):
    LOCAL = -5
    ANYWHERE = -4
    HOST_NAME = -3
    HOST_ID = -2
    DEFAULT = -1


@dataclass
class FakePolicy:
    Placement = FakePlacement

    placement: int = FakePlacement.DEFAULT
    host_name: str = ""
    host_id: int = -1
    cpu_affinity: list = field(default_factory=list)
    gpu_affinity: list = field(default_factory=list)


class FakePMI(enum.Enum):
    CRAY = "cray"
    PMIX = "pmix"


HOSTS = {
    "n0": {"h_uid": 10, "cpus": [0, 1, 2, 3], "gpus": [0, 1]},
    "n1": {"h_uid": 11, "cpus": [4, 5, 6, 7], "gpus": []},
}


class FakeDragon:
    """One fake runtime: its processes, and classes bound to it."""

    def __init__(self):
        self.procs = {}
        self.started = []
        self.reaped = []
        self.groups = []
        self.puids = itertools.count(1000)
        self.lock = threading.Lock()
        dragon = self

        class FakeNode:
            def __init__(self, ident):
                host = (
                    ident
                    if isinstance(ident, str)
                    else next(h for h, info in HOSTS.items() if info["h_uid"] == ident)
                )
                info = HOSTS[host]
                self.hostname = host
                self.h_uid = info["h_uid"]
                self.cpus = list(info["cpus"])
                self.gpus = list(info["gpus"]) or None
                self.num_cpus = info.get("num_cpus", len(info["cpus"]))

        class FakeSystem:
            nodes = [info["h_uid"] for info in HOSTS.values()]

        class FakeProcess:
            def __init__(
                self,
                target,
                args=None,
                kwargs=None,
                cwd=".",
                env=None,
                ident=None,
                stdin=None,
                stdout=None,
                stderr=None,
                policy=None,
                options=None,
            ):
                if ident is not None:
                    self.__dict__ = dragon.procs[ident].__dict__
                    return
                self.target, self.args, self.kwargs = target, tuple(args or ()), dict(kwargs or {})
                self.cwd, self.env, self.policy = cwd, env, policy
                self.stdout, self.stderr = stdout, stderr
                self.done = threading.Event()
                self.code = None
                self.popen = None
                self.puid = None

            @property
            def h_uid(self):
                host = getattr(self.policy, "host_name", "") or "n0"
                return HOSTS[host]["h_uid"]

            @property
            def host(self):
                return getattr(self.policy, "host_name", "") or "n0"

            def start(self):
                self.puid = next(dragon.puids)
                with dragon.lock:
                    dragon.procs[self.puid] = self
                    dragon.started.append(self)
                if callable(self.target):
                    threading.Thread(target=self._run_python, daemon=True).start()
                elif self.target == "/bin/sh" and self.args[1] == worker_module.REAP_SCRIPT:
                    dragon.reaped.append((self.host, self.args[3], self.args[4]))
                    self._exit(0)
                else:
                    env = {**os.environ, **(self.env or {})}
                    out = subprocess.DEVNULL if self.stdout is not None else None
                    self.popen = subprocess.Popen(
                        [self.target, *self.args], env=env, cwd=self.cwd, stdout=out, stderr=out
                    )
                    threading.Thread(
                        target=lambda: self._exit(self.popen.wait()), daemon=True
                    ).start()

            def _run_python(self):
                try:
                    self.target(*self.args, **self.kwargs)
                    code = 0
                except BaseException:  # noqa: BLE001
                    code = 1
                self._exit(code)

            def _exit(self, code):
                if not self.done.is_set():
                    self.code = code
                    self.done.set()

            def signal(self, sig):
                if self.popen is not None:
                    with_suppress(lambda: self.popen.send_signal(sig))
                else:
                    if self.target is worker_module.worker_main:
                        self.args[1].put(None)  # let the worker thread end after its task
                    self._exit(-sig)

            def join(self, timeout=None):
                return self.code if self.done.wait(timeout) else None

            def kill(self):
                self.signal(signal.SIGKILL)

            def terminate(self):
                self.signal(signal.SIGTERM)

        class FakeProcessTemplate:
            DEVNULL = "DEVNULL"

            def __init__(self, target, **kwargs):
                self.target = target
                self.kwargs = kwargs

        class FakeProcessGroup:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.templates = []
                self.members = []
                dragon.groups.append(self)

            def add_process(self, nproc, template):
                self.templates.append((nproc, template))

            def init(self):
                pass

            def start(self):
                for nproc, tmpl in self.templates:
                    for _ in range(nproc):
                        p = FakeProcess(tmpl.target, **tmpl.kwargs)
                        p.start()
                        self.members.append(p)

            @property
            def puids(self):
                return [p.puid for p in self.members]

            def join(self, timeout=None):
                deadline = None if timeout is None else time.monotonic() + timeout
                for p in self.members:
                    left = None if deadline is None else max(0.0, deadline - time.monotonic())
                    if p.join(left) is None:
                        raise TimeoutError()

            @property
            def exit_status(self):
                return [(p.puid, p.code) for p in self.members]

            def close(self):
                pass

        class FakeQueue:
            def __init__(self, maxsize=100, block_size=None, policy=None):
                self.q = queue.Queue()
                self.policy = policy
                self.destroyed = False

            def put(self, obj, block=True, timeout=None):
                self.q.put(obj)

            def get(self, block=True, timeout=None):
                if not block:
                    return self.q.get_nowait()
                return self.q.get(timeout=timeout)

            def destroy(self):
                self.destroyed = True

        def multi_join(puids, timeout=None, join_all=False):
            deadline = time.monotonic() + (timeout or 0)
            while True:
                exited = [(p, dragon.procs[p].code) for p in puids if dragon.procs[p].done.is_set()]
                if (len(exited) == len(puids)) if join_all else exited:
                    return exited, {}
                if time.monotonic() >= deadline:
                    return None, {}
                time.sleep(0.01)

        def process_kill(puid, sig=signal.SIGKILL, hide_stderr=False):
            dragon.procs[puid].signal(sig)

        self.Node, self.System, self.Process = FakeNode, FakeSystem, FakeProcess
        self.ProcessTemplate, self.ProcessGroup, self.Queue = (
            FakeProcessTemplate,
            FakeProcessGroup,
            FakeQueue,
        )
        self.multi_join, self.process_kill = multi_join, process_kill

    def workers(self):
        return [p for p in self.started if p.target is worker_module.worker_main]


def with_suppress(fn):
    try:
        fn()
    except OSError:
        pass


@pytest.fixture
def dragon(monkeypatch):
    fake = FakeDragon()
    for name in ("Node", "System", "Process", "ProcessTemplate", "ProcessGroup", "Queue"):
        monkeypatch.setattr(dragon_native, name, getattr(fake, name))
    monkeypatch.setattr(dragon_native, "Policy", FakePolicy)
    facts = types.ModuleType("dragon.infrastructure.facts")
    facts.PMIBackend = FakePMI
    monkeypatch.setitem(sys.modules, "dragon.infrastructure.facts", facts)
    monkeypatch.setattr(dragon_native, "multi_join", fake.multi_join)
    monkeypatch.setattr(dragon_native, "process_kill", fake.process_kill)
    monkeypatch.setattr(dragon_native, "current_node", lambda: fake.Node("n0"))
    monkeypatch.setattr(dragon_native, "_WATCH_INTERVAL", 0.05)
    monkeypatch.setattr(dragon_native, "_read_siblings", lambda cpu: "")
    return fake


@pytest.fixture
async def backend(dragon, tmp_path):
    """A started backend with 2 workers per node and inline capture off."""
    be = DragonNativeExecutionBackend(workers_per_node=2, inline_stdio=False, kill_grace=0.5)
    be._work_dir = str(tmp_path)
    await be
    yield be
    await be.shutdown()


class Recorder:
    """Callback that records (uid, state, thread) and lets a test await a terminal state."""

    terminal = {"DONE", "FAILED", "CANCELED"}

    def __init__(self, backend):
        self.records = []
        self.events = {}
        backend.register_callback(self)

    def __call__(self, task, state):
        self.records.append((task["uid"], state, threading.current_thread()))
        if state in self.terminal:
            self.events.setdefault(task["uid"], asyncio.Event()).set()

    async def wait(self, uid, timeout=10.0):
        await asyncio.wait_for(self.events.setdefault(uid, asyncio.Event()).wait(), timeout)
        return [s for u, s, _ in self.records if u == uid and s in self.terminal]


_uids = itertools.count()


def fn_task(fn, *args, **extra):
    return {"uid": f"task.{next(_uids):06d}", "function": fn, "args": args, "kwargs": {}, **extra}


def exe_task(exe, *arguments, **extra):
    return {
        "uid": f"task.{next(_uids):06d}",
        "function": None,
        "executable": exe,
        "arguments": list(arguments),
        **extra,
    }


def on(host, **kw):
    return {
        "process_template": {
            "policy": FakePolicy(placement=FakePlacement.HOST_NAME, host_name=host, **kw)
        }
    }


GATE = threading.Event()


def blocked(_=None):
    """A task body that holds its worker until the test opens GATE."""
    GATE.wait(30)
    return "released"


@pytest.fixture(autouse=True)
def _gate():
    GATE.clear()
    yield
    GATE.set()


async def until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


# ============================================================================
# Construction and the pool
# ============================================================================


def test_registered_under_its_own_name():
    """The backend is discovered as ``dragon_native``, beside ``dragon``."""
    assert BackendRegistry.get_backend_class("dragon_native") is DragonNativeExecutionBackend
    assert "dragon" in BackendRegistry.list_backends()


def test_constructor_without_dragon_raises(monkeypatch):
    monkeypatch.setattr(dragon_native, "Process", None)
    with pytest.raises(RuntimeError):
        DragonNativeExecutionBackend()


@pytest.mark.parametrize(
    "cores, n, expected",
    [
        ([(0,), (1,), (2,), (3,)], 2, [(0, 1), (2, 3)]),
        ([(0,), (1,), (2,), (3,), (4,)], 2, [(0, 1, 2), (3, 4)]),
        ([(0,), (1,)], 4, [(0,), (1,), (0,), (1,)]),
        ([], 2, [(), ()]),
        ([(0,), (1,)], 0, []),
        # Whole cores while there are enough: no two workers share one.
        ([(0, 4), (1, 5), (2, 6), (3, 7)], 4, [(0, 4), (1, 5), (2, 6), (3, 7)]),
        ([(0, 4), (1, 5), (2, 6), (3, 7)], 2, [(0, 4, 1, 5), (2, 6, 3, 7)]),
        ([(0, 4), (1, 5), (2, 6)], 2, [(0, 4, 1, 5), (2, 6)]),
        # More workers than cores: hardware threads, then one CPU each.
        ([(0, 2), (1, 3)], 3, [(0, 2), (1,), (3,)]),
        ([(0, 2), (1, 3)], 4, [(0,), (2,), (1,), (3,)]),
        ([(0, 2)], 3, [(0,), (2,), (0,)]),
    ],
)
def test_split_cpus(cores, n, expected):
    assert _split_cpus(cores, n) == expected


def test_group_by_core_pairs_hardware_threads_listed_apart(monkeypatch):
    """CPUs 0-3 and 256-259 are four cores twice over, so two workers get two cores each."""
    siblings = {c: f"{c},{c + 256}" for c in range(4)}
    siblings.update({c + 256: f"{c},{c + 256}" for c in range(4)})
    monkeypatch.setattr(dragon_native, "_read_siblings", siblings.__getitem__)
    cpus = [0, 1, 2, 3, 256, 257, 258, 259]
    cores = _group_by_core(cpus)
    assert cores == [(0, 256), (1, 257), (2, 258), (3, 259)]
    assert _split_cpus(cores, 2) == [(0, 256, 1, 257), (2, 258, 3, 259)]


def test_group_by_core_sorts_and_keeps_cpus_with_no_known_siblings_apart(monkeypatch):
    """Node.cpus comes from a set, so its order means nothing."""
    monkeypatch.setattr(dragon_native, "_read_siblings", lambda cpu: "")
    assert _group_by_core([3, 1, 2]) == [(1,), (2,), (3,)]


def test_read_siblings_of_a_cpu_the_node_does_not_have():
    assert dragon_native._read_siblings(10**6) == ""


def _smt_siblings(cpu: int) -> str:
    """Two-way SMT on the fake nodes: each core is a CPU and the same CPU plus 2."""
    low = cpu - 2 if cpu % 4 > 1 else cpu
    return f"{low},{low + 2}"


async def test_pool_is_sized_from_node_cpus_not_cpu_count(dragon, monkeypatch, tmp_path):
    """Without workers_per_node, each node gets one worker per core of the job, pinned to it."""
    monkeypatch.setattr(os, "cpu_count", lambda: 512)
    be = DragonNativeExecutionBackend(inline_stdio=False)
    await be
    try:
        assert be.num_workers == 8
        placed = sorted(
            (w.policy.host_name, tuple(w.policy.cpu_affinity)) for w in dragon.workers()
        )
        assert placed == [("n0", (c,)) for c in range(4)] + [("n1", (c,)) for c in range(4, 8)]
        assert be.topology() == [
            {"id": "n0", "cores": 4, "gpus": 2},
            {"id": "n1", "cores": 4, "gpus": 0},
        ]
    finally:
        await be.shutdown()


async def test_default_pool_is_one_worker_per_physical_core(dragon, monkeypatch):
    """A node of 4 CPUs on 2 cores gets 2 workers, and the process limit stays on CPUs."""
    monkeypatch.setattr(dragon_native, "_read_siblings", _smt_siblings)
    be = DragonNativeExecutionBackend(inline_stdio=False)
    await be
    try:
        assert be.num_workers == 4
        placed = sorted(
            (w.policy.host_name, tuple(w.policy.cpu_affinity)) for w in dragon.workers()
        )
        assert placed == [("n0", (0, 2)), ("n0", (1, 3)), ("n1", (4, 6)), ("n1", (5, 7))]
        assert be._max_process_ranks == 8
        assert be.topology()[0]["cores"] == 4
    finally:
        await be.shutdown()


async def test_default_pool_follows_the_jobs_cpus_not_the_machine(dragon, monkeypatch):
    """Sysfs lists the whole machine: only the job's CPUs of each core count."""
    monkeypatch.setitem(HOSTS, "n1", {"h_uid": 11, "cpus": [4, 5], "gpus": []})
    monkeypatch.setattr(dragon_native, "_read_siblings", _smt_siblings)
    be = DragonNativeExecutionBackend(inline_stdio=False)
    await be
    try:
        placed = sorted(
            (w.policy.host_name, tuple(w.policy.cpu_affinity)) for w in dragon.workers()
        )
        assert placed == [("n0", (0, 2)), ("n0", (1, 3)), ("n1", (4,)), ("n1", (5,))]
    finally:
        await be.shutdown()


@pytest.mark.parametrize(
    "workers, expected",
    [
        (1, [(0, 2, 1, 3)]),
        (3, [(0, 2), (1,), (3,)]),
        (4, [(0,), (2,), (1,), (3,)]),
    ],
)
async def test_workers_per_node_wins_over_the_core_count(dragon, monkeypatch, workers, expected):
    """An explicit pool may be smaller or larger than the core count, here 2 cores on n0."""
    monkeypatch.setattr(dragon_native, "_read_siblings", _smt_siblings)
    be = DragonNativeExecutionBackend(workers_per_node=workers, inline_stdio=False)
    await be
    try:
        placed = [
            tuple(w.policy.cpu_affinity) for w in dragon.workers() if w.policy.host_name == "n0"
        ]
        assert sorted(placed) == sorted(expected)
    finally:
        await be.shutdown()


async def test_default_pool_on_a_node_with_no_cpu_list(dragon, monkeypatch):
    """With no CPU list there are no cores to count: one unpinned worker per reported CPU."""
    monkeypatch.setitem(HOSTS, "n1", {"h_uid": 11, "cpus": [], "num_cpus": 3, "gpus": []})
    be = DragonNativeExecutionBackend(inline_stdio=False)
    await be
    try:
        on_n1 = [w for w in dragon.workers() if w.policy.host_name == "n1"]
        assert len(on_n1) == 3
        assert not any(w.policy.cpu_affinity for w in on_n1)
    finally:
        await be.shutdown()


async def test_each_inbox_is_on_its_workers_node_and_results_on_the_clients(backend):
    """Inbox queues sit on their worker's node; the result queue sits on the client's node."""
    for w in backend._workers.values():
        assert w.inbox.policy.host_name == w.host
    assert backend._results.policy.host_name == "n0"


# ============================================================================
# Function tasks in the pool
# ============================================================================


def square(x):
    return x * x


async def asquare(x):
    await asyncio.sleep(0)
    return x * x


def boom():
    raise ValueError("boom-from-task")


async def test_sync_and_async_functions_return_their_values(backend):
    rec = Recorder(backend)
    tasks = [fn_task(square, 7), fn_task(asquare, 8)]
    await backend.submit_tasks(tasks)
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]
    assert [t["return_value"] for t in tasks] == [49, 64]


async def test_states_are_delivered_on_the_event_loop_thread(backend):
    """RUNNING and DONE both reach the callback on the loop's thread, never a backend thread."""
    rec = Recorder(backend)
    task = fn_task(square, 3)
    await backend.submit_tasks([task])
    await rec.wait(task["uid"])
    assert [s for _, s, _ in rec.records] == ["RUNNING", "DONE"]
    assert {t for _, _, t in rec.records} == {threading.current_thread()}


async def test_failing_function_reports_exception_and_traceback(backend):
    rec = Recorder(backend)
    task = fn_task(boom)
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert isinstance(task["exception"], ValueError)
    assert "boom-from-task" in task["stderr"] and "Traceback" in task["stderr"]


async def test_unpicklable_result_fails_only_its_task(backend):
    rec = Recorder(backend)
    bad, good = fn_task(lambda: threading.Lock()), fn_task(square, 2)
    await backend.submit_tasks([bad, good])
    assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert await rec.wait(good["uid"]) == ["DONE"]
    assert "cannot pickle" in str(bad["exception"])


class TwoArgError(Exception):
    """Pickles fine, then raises on load: unpickling calls it with one argument."""

    def __init__(self, path, code):
        super().__init__(f"{path}: {code}")


def raises_two_arg_error():
    raise TwoArgError("/tmp/x", 3)


def _cannot_rebuild():
    raise RuntimeError("cannot rebuild on the client")


class NoLoad:
    def __reduce__(self):
        return (_cannot_rebuild, ())


def returns_no_load():
    return NoLoad()


async def test_result_the_client_cannot_load_fails_only_its_task(backend):
    """Each such task used to hang and strand its worker, until the whole pool was gone."""
    rec = Recorder(backend)
    bad = [fn_task(raises_two_arg_error) for _ in range(2)] + [fn_task(returns_no_load)]
    bad += [fn_task(raises_two_arg_error)]
    good = fn_task(square, 2)
    await backend.submit_tasks([*bad, good])
    for task in bad:
        assert await rec.wait(task["uid"]) == ["FAILED"]
        assert "cannot unpickle the task's result" in str(task["exception"])
    assert await rec.wait(good["uid"]) == ["DONE"]
    assert "TwoArgError" in bad[0]["stderr"]


async def test_process_function_result_the_client_cannot_load_fails_at_once(backend):
    rec = Recorder(backend)
    task = fn_task(raises_two_arg_error, task_backend_specific_kwargs={"process_template": {}})
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert "cannot unpickle the task's result" in str(task["exception"])


async def test_function_a_worker_cannot_load_fails_only_its_task(backend):
    rec = Recorder(backend)
    bad, good = fn_task(NoLoad()), fn_task(square, 2)
    await backend.submit_tasks([bad, good])
    assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert "cannot rebuild" in str(bad["exception"])
    assert await rec.wait(good["uid"]) == ["DONE"]


def calls_sys_exit():
    sys.exit(3)


async def test_sys_exit_comes_back_as_an_error_that_cannot_end_the_client(backend):
    """A SystemExit set on the task would be raised in the client when it awaits the task."""
    rec = Recorder(backend)
    task = fn_task(calls_sys_exit)
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert type(task["exception"]) is RuntimeError
    assert "SystemExit(3)" in str(task["exception"])


async def test_bad_message_does_not_end_delivery(backend):
    rec = Recorder(backend)
    backend._results.put(cloudpickle.dumps((worker_module.DONE, "too short")))
    backend._results.put(b"not a pickle")
    task = fn_task(square, 2)
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["DONE"]


async def test_placed_function_runs_on_a_worker_of_that_host(backend, dragon):
    """A placement, or a policy-only process template, sends a function to that host's pool."""
    rec = Recorder(backend)
    by_placement = fn_task(blocked, placement={"node": 1, "host": "n1", "gpus": []})
    by_policy = fn_task(blocked, task_backend_specific_kwargs=on("n1"))
    await backend.submit_tasks([by_placement, by_policy])
    hosts = {backend._tasks[t["uid"]].worker.host for t in (by_placement, by_policy)}
    assert hosts == {"n1"}
    assert all(p.target is worker_module.worker_main for p in dragon.started)
    GATE.set()
    for task in (by_placement, by_policy):
        assert await rec.wait(task["uid"]) == ["DONE"]


async def test_unplaced_functions_fill_the_clients_node_first(backend):
    rec = Recorder(backend)
    tasks = [fn_task(blocked) for _ in range(3)]
    await backend.submit_tasks(tasks)
    hosts = [backend._tasks[t["uid"]].worker.host for t in tasks]
    assert hosts == ["n0", "n0", "n1"]
    GATE.set()
    for task in tasks:
        await rec.wait(task["uid"])


async def test_tasks_wait_for_a_free_worker_of_their_host(backend):
    """With both n1 workers busy, a third n1 task waits instead of going to n0."""
    rec = Recorder(backend)
    tasks = [fn_task(blocked, placement={"host": "n1"}) for _ in range(3)]
    await backend.submit_tasks(tasks)
    assert backend._tasks[tasks[2]["uid"]].state == "queued"
    GATE.set()
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]


async def test_gpu_task_starts_a_worker_for_its_gpu_set(backend, dragon):
    rec = Recorder(backend)
    task = fn_task(square, 4, placement={"host": "n0", "gpus": [1]})
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["DONE"]
    gpu_workers = [w for w in dragon.workers() if w.policy.gpu_affinity]
    assert [(w.policy.host_name, w.policy.gpu_affinity) for w in gpu_workers] == [("n0", [1])]
    again = fn_task(square, 5, placement={"host": "n0", "gpus": [1]})
    await backend.submit_tasks([again])
    assert await rec.wait(again["uid"]) == ["DONE"]
    assert len([w for w in dragon.workers() if w.policy.gpu_affinity]) == 1


@pytest.mark.parametrize(
    "extra, match",
    [
        ({"placement": {"host": "no-such-host"}}, "not a Dragon node"),
        ({"placement": {"host": "n1", "gpus": [0]}}, "GPUs"),
        ({"task_backend_specific_kwargs": on("nowhere")}, "not a Dragon node"),
        (
            {
                "task_backend_specific_kwargs": {
                    "process_template": {
                        "policy": FakePolicy(placement=FakePlacement.HOST_NAME, host_name="nowhere")
                    },
                    "timeout": 5,
                }
            },
            "not a Dragon node",
        ),
        ({"task_backend_specific_kwargs": {"timeout": 0}}, "timeout"),
        ({"task_backend_specific_kwargs": {"process_templates": []}}, "no processes"),
        ({"task_backend_specific_kwargs": {"process_templates": [(0, {})]}}, "asks for 0"),
        (
            {"task_backend_specific_kwargs": {"process_templates": [(2, {}), (-1, {})]}},
            "asks for -1",
        ),
    ],
)
async def test_bad_placement_fails_only_its_task_before_dragon_sees_it(
    backend, dragon, extra, match
):
    """Dragon aborts the whole runtime on an unknown host, so the backend must check first."""
    rec = Recorder(backend)
    bad, good = fn_task(square, 1, **extra), fn_task(square, 2)
    started = len(dragon.started)
    await backend.submit_tasks([bad, good])
    assert [s for u, s, _ in rec.records if u == bad["uid"]] == ["FAILED"]
    assert match in str(bad["exception"])
    assert await rec.wait(good["uid"]) == ["DONE"]
    assert len(dragon.started) == started


async def test_failed_send_fails_only_its_task(backend, monkeypatch):
    rec = Recorder(backend)

    def broken_put(obj, block=True, timeout=None):
        raise RuntimeError("inbox is gone")

    with monkeypatch.context() as patch:
        for w in backend._workers.values():
            patch.setattr(w.inbox, "put", broken_put)
        bad = fn_task(square, 1)
        await backend.submit_tasks([bad])
        assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert "inbox is gone" in bad["stderr"]
    good = fn_task(square, 2)
    await backend.submit_tasks([good])
    assert await rec.wait(good["uid"]) == ["DONE"]


async def test_function_with_no_pool_fails_at_once(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=0, inline_stdio=False)
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        task = fn_task(square, 2)
        await be.submit_tasks([task])
        assert await rec.wait(task["uid"]) == ["FAILED"]
        assert "no pool worker" in str(task["exception"])
    finally:
        await be.shutdown()


# ============================================================================
# Routing
# ============================================================================


@pytest.mark.parametrize(
    "backend_kwargs, kind",
    [
        ({}, "pool"),
        ({"process_template": {"policy": None}}, "pool"),
        ("on-n1", "pool"),
        ({"process_template": {}}, "process"),
        ({"process_template": {"cwd": "/tmp"}}, "process"),
        ("cpu-affinity", "process"),
        ({"process_templates": [(2, {})]}, "group"),
    ],
)
async def test_function_routing(backend, backend_kwargs, kind):
    if backend_kwargs == "on-n1":
        backend_kwargs = on("n1")
    elif backend_kwargs == "cpu-affinity":
        backend_kwargs = on("n1", cpu_affinity=[4])
    rec = backend.build_task(fn_task(square, 2, task_backend_specific_kwargs=backend_kwargs))
    assert rec.kind == kind


async def test_executable_runs_through_sh_exec_with_its_token(backend):
    """The executable's output goes to files, never to Local Services' pipes."""
    rec = backend.build_task(exe_task("/bin/echo", "hi", capture_stdio=True))
    proc = rec.proc
    assert proc.target == "/bin/sh"
    assert proc.args[:3] == ("-c", worker_module.EXEC_SCRIPT, "sh")
    assert proc.args[3:5] == rec.stdio
    assert proc.args[5:] == ("/bin/echo", "hi")
    assert proc.stdout == proc.stderr == "DEVNULL"
    assert proc.env[worker_module.TOKEN_ENV] == rec.token


async def test_user_stdout_in_template_is_passed_through(backend):
    rec = backend.build_task(
        exe_task(
            "/bin/echo",
            "hi",
            task_backend_specific_kwargs={
                "process_template": {"stdout": "DEVNULL", "env": {"A": "1"}}
            },
        )
    )
    assert rec.proc.target == "/bin/echo" and rec.proc.stdout == "DEVNULL"
    assert rec.proc.env == {"A": "1", worker_module.TOKEN_ENV: rec.token}


async def test_placement_by_host_id(backend):
    known = FakePolicy(placement=FakePlacement.HOST_ID, host_id=11)
    task = fn_task(square, 1, task_backend_specific_kwargs={"process_template": {"policy": known}})
    assert backend.build_task(task).key == ("n1", ())
    unknown = FakePolicy(placement=FakePlacement.HOST_ID, host_id=99)
    task = fn_task(
        square, 1, task_backend_specific_kwargs={"process_template": {"policy": unknown}}
    )
    with pytest.raises(ValueError, match="host id 99"):
        backend.build_task(task)


async def test_template_policy_wins_over_placement(backend):
    rec = backend.build_task(
        fn_task(square, 1, placement={"host": "n0"}, task_backend_specific_kwargs=on("n1"))
    )
    assert rec.key == ("n1", ())


async def test_process_templates_build_one_template_per_entry(backend):
    templates = [
        (2, {}),
        (1, {"policy": FakePolicy(placement=FakePlacement.HOST_NAME, host_name="n1")}),
    ]
    rec = backend.build_task(
        exe_task("true", task_backend_specific_kwargs={"process_templates": templates})
    )
    assert rec.ranks == 3
    assert [n for n, _ in rec.templates] == [2, 1]
    assert rec.templates[1][1].kwargs["policy"].host_name == "n1"


# ============================================================================
# Process and group tasks
# ============================================================================


async def test_executable_exit_code_and_inline_output(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=1, kill_grace=0.5)
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        ok = exe_task("/bin/echo", "hello")
        bad = exe_task("/bin/sh", "-c", "echo out; echo err >&2; exit 3")
        missing = exe_task("no-such-executable-here")
        await be.submit_tasks([ok, bad, missing])
        assert await rec.wait(ok["uid"]) == ["DONE"]
        assert (ok["stdout"], ok["exit_code"], ok["return_value"]) == ("hello\n", 0, 0)
        assert await rec.wait(bad["uid"]) == ["FAILED"]
        assert (bad["stdout"], bad["stderr"], bad["exit_code"]) == ("out\n", "err\n", 3)
        # AsyncFlow reads only exception and stderr: the exit code must be in the exception.
        assert isinstance(bad["exception"], TaskExecutionError)
        assert bad["exception"].exit_code == 3 and "err" in str(bad["exception"])
        assert await rec.wait(missing["uid"]) == ["FAILED"]
        assert missing["exit_code"] == 127 and "not found" in missing["stderr"]
        inline_dir = os.path.join(str(tmp_path), f".dragon_native.{be._id}")
        assert os.listdir(inline_dir) == []
    finally:
        await be.shutdown()
    assert not os.path.exists(inline_dir)


async def test_executable_capture_stdio_returns_paths(backend, tmp_path):
    rec = Recorder(backend)
    task = exe_task("/bin/echo", "captured", capture_stdio=True)
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["DONE"]
    assert task["stdout"] == os.path.join(str(tmp_path), f"{task['uid']}.stdout")
    assert open(task["stdout"]).read() == "captured\n"


async def test_function_process_that_exits_non_zero_after_its_result_fails(backend):
    """A Session would raise on the exit code, so DONE would be DONE and failed at once."""
    rec = backend.build_task(
        fn_task(square, 2, task_backend_specific_kwargs={"process_template": {}})
    )
    rec.result = (True, 4, None)
    rec.result_event.set()

    class DiedAtExit:
        puid = 7

        def start(self):
            pass

        def join(self, timeout=None):
            return 139

    rec.proc = DiedAtExit()
    state, fields = backend._run_single(rec)
    assert (state, fields["return_value"], fields["exit_code"]) == ("FAILED", 4, 139)
    assert fields["exception"].exit_code == 139


async def test_function_with_process_template_returns_its_value(backend):
    rec = Recorder(backend)
    ok = fn_task(square, 9, task_backend_specific_kwargs={"process_template": {}})
    bad = fn_task(boom, task_backend_specific_kwargs={"process_template": {}})
    await backend.submit_tasks([ok, bad])
    assert await rec.wait(ok["uid"]) == ["DONE"]
    assert ok["return_value"] == 81
    assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert isinstance(bad["exception"], ValueError)


async def test_group_returns_exit_codes(backend):
    rec = Recorder(backend)
    ok = exe_task("true", task_backend_specific_kwargs={"process_templates": [(2, {}), (1, {})]})
    bad = exe_task(
        "/bin/sh", "-c", "exit 4", task_backend_specific_kwargs={"process_templates": [(2, {})]}
    )
    await backend.submit_tasks([ok, bad])
    assert await rec.wait(ok["uid"]) == ["DONE"], (ok.get("exception"), ok.get("stderr"))
    assert ok["return_value"] == [0, 0, 0] and ok["exit_code"] == 0
    assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert bad["return_value"] == [4, 4] and bad["exit_code"] == 4
    assert bad["exception"].exit_code == 4 and "stderr not captured" in str(bad["exception"])


async def test_function_group_returns_exit_codes(backend):
    rec = Recorder(backend)
    task = fn_task(square, 2, task_backend_specific_kwargs={"process_templates": [(2, {})]})
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["DONE"]
    assert task["return_value"] == [0, 0]


async def test_group_timeout_and_cancel(backend):
    rec = Recorder(backend)
    templates = {"process_templates": [(2, {})], "pmi": "none"}
    timed_out = exe_task(
        "/bin/sleep", "30", task_backend_specific_kwargs={**templates, "timeout": 0.3}
    )
    cancelled = exe_task("/bin/sleep", "30", task_backend_specific_kwargs=templates)
    await backend.submit_tasks([timed_out, cancelled])
    assert await rec.wait(timed_out["uid"]) == ["FAILED"]
    assert isinstance(timed_out["exception"], TimeoutError)
    assert timed_out["return_value"] == [-signal.SIGTERM, -signal.SIGTERM]
    await until(lambda: backend._tasks[cancelled["uid"]].puids)
    assert await backend.cancel_task(cancelled["uid"]) is True
    assert await rec.wait(cancelled["uid"]) == ["CANCELED"]


async def test_process_that_ignores_sigterm_gets_sigkill(backend):
    rec = Recorder(backend)
    task = exe_task(
        "/bin/sh",
        "-c",
        "trap '' TERM; exec sleep 30",
        task_backend_specific_kwargs={"timeout": 0.3},
    )
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert task["exit_code"] == -signal.SIGKILL


def group_task(nranks=4, **kwargs):
    return exe_task(
        "true", task_backend_specific_kwargs={"process_templates": [(nranks, {})], **kwargs}
    )


async def test_group_pmi_comes_from_the_task_then_the_backend(dragon, tmp_path):
    """A task's pmi takes precedence over the backend's; "none" and no choice give no PMI."""
    be = DragonNativeExecutionBackend(workers_per_node=1, pmi="pmix")
    be._work_dir = str(tmp_path)
    await be
    try:
        assert be.build_task(group_task(timeout=5)).pmi is FakePMI.PMIX
        assert be.build_task(group_task(pmi="Cray", timeout=5)).pmi is FakePMI.CRAY
        assert be.build_task(group_task(pmi=FakePMI.CRAY, timeout=5)).pmi is FakePMI.CRAY
        assert be.build_task(group_task(pmi=None, timeout=5)).pmi is FakePMI.PMIX
        assert be.build_task(group_task(pmi="none")).pmi is None
    finally:
        await be.shutdown()


async def test_group_runs_with_its_pmi(backend, dragon):
    rec = Recorder(backend)
    mpi = group_task(2, pmi="pmix", timeout=5)
    plain = group_task(2, pmi="none")
    await backend.submit_tasks([mpi, plain])
    assert await rec.wait(mpi["uid"]) == ["DONE"]
    assert await rec.wait(plain["uid"]) == ["DONE"]
    pmis = {g.kwargs["name"]: g.kwargs["pmi"] for g in dragon.groups}
    assert pmis == {f"rhapsody.{mpi['uid']}": FakePMI.PMIX, f"rhapsody.{plain['uid']}": None}


async def test_unknown_pmi_raises_before_the_backend_starts(dragon):
    with pytest.raises(ValueError, match="Unknown pmi 'mpich'"):
        DragonNativeExecutionBackend(pmi="mpich")
    assert dragon.started == []


async def test_unknown_task_pmi_fails_only_that_task(backend):
    rec = Recorder(backend)
    bad = group_task(pmi="pmi2", timeout=5)
    good = group_task(pmi="pmix", timeout=5)
    await backend.submit_tasks([bad, good])
    assert await rec.wait(bad["uid"]) == ["FAILED"]
    assert "Unknown pmi 'pmi2'" in str(bad["exception"])
    assert await rec.wait(good["uid"]) == ["DONE"]


@pytest.fixture
def no_cray_mpich(monkeypatch):
    for name in dragon_native.CRAY_MPICH_ENV:
        monkeypatch.delenv(name, raising=False)


async def test_group_warnings_are_logged_once(backend, caplog, no_cray_mpich):
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    for _ in range(2):
        backend.build_task(group_task())
    pmi = [r for r in caplog.records if "no PMI" in r.getMessage()]
    timeout = [r for r in caplog.records if "no timeout" in r.getMessage()]
    assert len(pmi) == 1 and len(timeout) == 1
    assert "4 ranks" in pmi[0].getMessage()


@pytest.mark.parametrize(
    "task",
    [
        {"nranks": 1},
        {"pmi": "none"},
        {"pmi": "cray", "timeout": 5},
    ],
    ids=["one rank", "pmi none", "pmi and timeout"],
)
async def test_group_without_reason_to_warn_logs_nothing(backend, caplog, no_cray_mpich, task):
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    backend.build_task(group_task(**task))
    assert not [r for r in caplog.records if "ranks and no" in r.getMessage()]


@pytest.mark.parametrize("nranks, ranks", [(1, "1 rank and"), (4, "4 ranks and")])
async def test_group_under_cray_mpich_warns_even_with_one_rank(
    backend, caplog, monkeypatch, no_cray_mpich, nranks, ranks
):
    """With no PMI by default, Cray MPICH makes a group's PMI matter even for a rank alone."""
    monkeypatch.setenv("CRAY_MPICH_VERSION", "8.1")
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    backend.build_task(group_task(nranks))
    pmi = [r.getMessage() for r in caplog.records if "no PMI" in r.getMessage()]
    assert len(pmi) == 1 and ranks in pmi[0]
    assert "Cray MPICH environment detected (CRAY_MPICH_VERSION)" in pmi[0]


async def test_one_rank_under_cray_mpich_with_a_pmi_logs_nothing(backend, caplog, monkeypatch):
    monkeypatch.setenv("CRAY_MPICH_DIR", "/opt/cray")
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    backend.build_task(group_task(1, pmi="cray"))
    backend.build_task(group_task(1, pmi="none"))
    assert not [r for r in caplog.records if "no PMI" in r.getMessage()]


async def test_pmi_none_does_not_use_up_the_warnings(backend, caplog, no_cray_mpich):
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    backend.build_task(group_task(pmi="none"))
    backend.build_task(group_task())
    assert len([r for r in caplog.records if "ranks and no" in r.getMessage()]) == 2


async def test_process_tasks_wait_behind_max_process_ranks(dragon, tmp_path):
    be = DragonNativeExecutionBackend(
        workers_per_node=1, inline_stdio=False, max_process_ranks=2, kill_grace=0.2
    )
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        tasks = [exe_task("/bin/sleep", "30") for _ in range(3)]
        await be.submit_tasks(tasks)
        assert [be._tasks[t["uid"]].state for t in tasks] == ["running", "running", "queued"]
        await until(lambda: be._tasks[tasks[0]["uid"]].puids)
        await be.cancel_task(tasks[0]["uid"])
        await until(lambda: be._tasks[tasks[2]["uid"]].state == "running")
    finally:
        await be.shutdown()


async def test_waiting_process_tasks_keep_their_order(dragon, tmp_path):
    """A new task cannot overtake a queued job, or small tasks could keep it waiting for good."""
    be = DragonNativeExecutionBackend(
        workers_per_node=1, inline_stdio=False, max_process_ranks=2, kill_grace=0.2
    )
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        first, second = exe_task("/bin/sleep", "30"), exe_task("/bin/sleep", "30")
        job = group_task(2, pmi="none")
        await be.submit_tasks([first, second, job])
        await until(lambda: be._tasks[first["uid"]].puids and be._tasks[second["uid"]].puids)
        await be.cancel_task(first["uid"])
        await until(lambda: be._running_ranks == 1)
        small = exe_task("/bin/sleep", "30")
        await be.submit_tasks([small])
        assert be._tasks[job["uid"]].state == "queued"
        assert be._tasks[small["uid"]].state == "queued"
        await be.cancel_task(second["uid"])
        assert await rec.wait(job["uid"]) == ["DONE"]
        await until(lambda: be._tasks[small["uid"]].state == "running")
    finally:
        await be.shutdown()


async def test_task_that_cannot_get_a_thread_fails_alone(backend, monkeypatch):
    """At the node's thread limit, the task fails, the rest of the list runs, and shutdown works."""
    rec = Recorder(backend)
    start = threading.Thread.start

    def limited(thread):
        if thread.name.startswith("dragon_native_task"):
            raise RuntimeError("can't start new thread")
        start(thread)

    tasks = [exe_task("true"), exe_task("true")]
    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", limited)
        await backend.submit_tasks(tasks)
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["FAILED"]
        assert "can't start new thread" in task["stderr"]
    assert backend._running_ranks == 0 and backend._tasks == {}
    after = exe_task("true")
    await backend.submit_tasks([after])
    assert await rec.wait(after["uid"]) == ["DONE"]


# ============================================================================
# Short tasks and the busy cap
# ============================================================================


def nap(seconds):
    time.sleep(seconds)
    return seconds


@pytest.fixture
async def capped(dragon, tmp_path, monkeypatch):
    """4 workers per node, at most 2 busy per node while its tasks are short; a window of 4."""
    monkeypatch.setattr(dragon_native, "_TASK_WINDOW", 4)
    monkeypatch.setattr(dragon_native, "_STALL", 30.0)
    be = DragonNativeExecutionBackend(
        workers_per_node=4, short_task_workers=2, inline_stdio=False, kill_grace=0.5
    )
    be._work_dir = str(tmp_path)
    await be
    yield be
    await be.shutdown()


def make_short(backend, host):
    with backend._lock:
        for _ in range(dragon_native._TASK_WINDOW):
            backend._record(host, 0.0)


def running_on(backend, host):
    return sum(1 for w in backend._workers.values() if w.host == host and w.state == "busy")


async def test_short_task_workers_must_be_positive(dragon):
    with pytest.raises(ValueError, match="short_task_workers"):
        DragonNativeExecutionBackend(short_task_workers=0)


def test_a_host_is_full_while_more_than_half_its_window_is_short(capped):
    capped._busy["n1"] = 2
    assert not capped._full("n1")  # no tasks yet: every worker may run
    with capped._lock:
        for seconds in (1.0, 1.0, 0.0, 0.0):
            capped._record("n1", seconds)
    assert not capped._full("n1")  # half short is not enough
    with capped._lock:
        capped._record("n1", 0.0)  # pushes out a long one: three short of four
    assert capped._full("n1")
    capped._busy["n1"] = 1
    assert not capped._full("n1")


async def test_workers_report_how_long_their_tasks_ran(capped, monkeypatch):
    monkeypatch.setattr(dragon_native, "_SHORT_TASK", 0.05)
    rec = Recorder(capped)
    tasks = [
        fn_task(nap, 0.0, placement={"host": "n1"}),
        fn_task(nap, 0.2, placement={"host": "n1"}),
    ]
    await capped.submit_tasks(tasks)
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]
    assert sorted(capped._recent["n1"]) == [False, True]


async def test_short_tasks_run_on_at_most_short_task_workers_of_a_node(capped):
    rec = Recorder(capped)
    make_short(capped, "n1")
    tasks = [fn_task(blocked, placement={"host": "n1"}) for _ in range(4)]
    await capped.submit_tasks(tasks)
    assert running_on(capped, "n1") == 2
    assert [capped._tasks[t["uid"]].state for t in tasks].count("queued") == 2
    GATE.set()
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]
    assert capped._busy["n1"] == 0


async def test_unplaced_short_tasks_go_to_a_node_with_room(capped):
    """The client's node is filled first, up to its cap, and then the next node."""
    rec = Recorder(capped)
    make_short(capped, "n0")
    tasks = [fn_task(blocked) for _ in range(4)]
    await capped.submit_tasks(tasks)
    hosts = sorted(capped._tasks[t["uid"]].worker.host for t in tasks)
    assert hosts == ["n0", "n0", "n1", "n1"]
    GATE.set()
    for task in tasks:
        await rec.wait(task["uid"])


async def test_long_tasks_lift_the_cap(capped, monkeypatch):
    """Once a node's tasks run long, the waiting tasks go to its idle workers too."""
    monkeypatch.setattr(dragon_native, "_SHORT_TASK", 0.05)
    rec = Recorder(capped)
    make_short(capped, "n1")
    tasks = [fn_task(nap, 0.3, placement={"host": "n1"}) for _ in range(8)]
    await capped.submit_tasks(tasks)
    assert running_on(capped, "n1") == 2
    await until(lambda: running_on(capped, "n1") == 4)
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]
    assert capped._busy["n1"] == 0


async def test_a_node_whose_running_tasks_all_wait_lifts_its_limit(capped, monkeypatch):
    """Tasks that wait on tasks held back behind them would otherwise wait for good."""
    monkeypatch.setattr(dragon_native, "_STALL", 0.2)
    rec = Recorder(capped)
    make_short(capped, "n1")
    tasks = [fn_task(blocked, placement={"host": "n1"}) for _ in range(4)]
    await capped.submit_tasks(tasks)
    assert running_on(capped, "n1") == 2
    await until(lambda: running_on(capped, "n1") == 4)
    GATE.set()
    for task in tasks:
        assert await rec.wait(task["uid"]) == ["DONE"]
    assert capped._busy["n1"] == 0


async def test_no_cap_runs_every_worker(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=4, short_task_workers=None)
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        make_short(be, "n1")
        tasks = [fn_task(blocked, placement={"host": "n1"}) for _ in range(4)]
        await be.submit_tasks(tasks)
        assert running_on(be, "n1") == 4
        GATE.set()
        for task in tasks:
            await rec.wait(task["uid"])
    finally:
        await be.shutdown()


async def test_cancel_timeout_and_death_make_room_under_the_cap(capped, dragon):
    """Each frees its worker's place at once, and the count of busy workers stays exact."""
    rec = Recorder(capped)
    make_short(capped, "n1")
    cancelled = fn_task(blocked, placement={"host": "n1"})
    timed_out = fn_task(
        blocked, task_backend_specific_kwargs={"timeout": 0.3}, placement={"host": "n1"}
    )
    waiting = [fn_task(blocked, placement={"host": "n1"}) for _ in range(2)]
    await capped.submit_tasks([cancelled, timed_out, *waiting])
    assert [capped._tasks[t["uid"]].state for t in waiting] == ["queued", "queued"]

    assert await capped.cancel_task(cancelled["uid"]) is True
    assert capped._tasks[waiting[0]["uid"]].state == "running"
    assert await rec.wait(timed_out["uid"]) == ["FAILED"]
    await until(lambda: capped._tasks[waiting[1]["uid"]].state == "running")
    assert capped._busy["n1"] == running_on(capped, "n1") == 2

    capped._tasks[waiting[0]["uid"]].worker.proc._exit(-11)
    assert await rec.wait(waiting[0]["uid"]) == ["FAILED"]
    assert capped._busy["n1"] == running_on(capped, "n1") == 1
    GATE.set()
    assert await rec.wait(waiting[1]["uid"]) == ["DONE"]
    assert capped._busy["n1"] == 0


# ============================================================================
# Cancel, timeout, dead workers, shutdown
# ============================================================================


async def test_cancel_queued_pool_task_never_runs(backend):
    rec = Recorder(backend)
    busy = [fn_task(blocked) for _ in range(4)]
    queued = fn_task(square, 3)
    await backend.submit_tasks(busy + [queued])
    assert backend._tasks[queued["uid"]].state == "queued"
    assert await backend.cancel_task(queued["uid"]) is True
    assert await rec.wait(queued["uid"]) == ["CANCELED"]
    GATE.set()
    for task in busy:
        await rec.wait(task["uid"])
    assert "return_value" not in queued
    assert await backend.cancel_task(queued["uid"]) is False


async def test_cancel_running_pool_task_kills_and_replaces_its_worker(backend, dragon):
    """The worker gets SIGKILL, the task's children are reaped, and a new worker takes its place.

    The killed worker's late result must not reach the callback.
    """
    rec = Recorder(backend)
    task = fn_task(blocked)
    await backend.submit_tasks([task])
    w = backend._tasks[task["uid"]].worker
    proc = w.proc
    assert await backend.cancel_task(task["uid"]) is True
    assert await rec.wait(task["uid"]) == ["CANCELED"]
    await until(lambda: proc.code == -signal.SIGKILL)
    await until(lambda: len(dragon.workers()) == 5 and backend._live_by_key[w.key] == 2)
    assert dragon.reaped[-1] == (
        w.host,
        f"{worker_module.TOKEN_ENV}={backend._id}.{task['uid']}",
        "KILL",
    )
    GATE.set()
    await asyncio.sleep(0.2)
    assert [s for u, s, _ in rec.records if u == task["uid"]] == ["RUNNING", "CANCELED"]
    after = fn_task(square, 6, placement={"host": w.host})
    await backend.submit_tasks([after])
    assert await rec.wait(after["uid"]) == ["DONE"]


async def test_pool_timeout_fails_the_task_and_kills_its_worker(backend):
    rec = Recorder(backend)
    procs = [w.proc for w in backend._workers.values()]
    task = fn_task(blocked, task_backend_specific_kwargs={"timeout": 0.2})
    await backend.submit_tasks([task])
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert isinstance(task["exception"], TimeoutError)
    await until(lambda: [p.code for p in procs].count(-signal.SIGKILL) == 1)


async def test_timer_heap_does_not_keep_finished_tasks(backend):
    """At 1,000 tasks/s and a 3,600 s timeout it would hold 3.6 million results."""
    rec = Recorder(backend)
    tasks = [fn_task(square, 2, task_backend_specific_kwargs={"timeout": 3600}) for _ in range(4)]
    await backend.submit_tasks(tasks)
    for task in tasks:
        await rec.wait(task["uid"])
    assert len(backend._timers) == 4
    assert all(isinstance(uid, str) for _, _, uid in backend._timers)


async def test_old_timer_does_not_time_out_a_new_task_of_that_uid(backend):
    rec = Recorder(backend)
    first = fn_task(square, 2, task_backend_specific_kwargs={"timeout": 3600})
    await backend.submit_tasks([first])
    await rec.wait(first["uid"])
    _, timer, uid = backend._timers[0]
    await backend.submit_tasks([{**fn_task(blocked), "uid": uid}])
    backend._timeout_pool_task(uid, timer)
    assert backend._tasks[uid].state == "running"
    GATE.set()
    await until(lambda: uid not in backend._tasks)


async def test_executable_cancel_and_timeout(backend, tmp_path):
    rec = Recorder(backend)
    cancelled = exe_task("/bin/sleep", "30")
    timed_out = exe_task("/bin/sleep", "30", task_backend_specific_kwargs={"timeout": 0.3})
    t0 = time.monotonic()
    await backend.submit_tasks([cancelled, timed_out])
    await until(lambda: backend._tasks[cancelled["uid"]].puids)
    assert await backend.cancel_task(cancelled["uid"]) is True
    assert await rec.wait(cancelled["uid"]) == ["CANCELED"]
    assert await rec.wait(timed_out["uid"]) == ["FAILED"]
    assert isinstance(timed_out["exception"], TimeoutError)
    assert timed_out["exit_code"] == -signal.SIGTERM
    assert time.monotonic() - t0 < 5


async def test_dead_worker_fails_its_task_and_is_replaced(backend, dragon):
    rec = Recorder(backend)
    task = fn_task(blocked)
    await backend.submit_tasks([task])
    w = backend._tasks[task["uid"]].worker
    w.proc._exit(-11)  # the worker died, say of SIGSEGV
    assert await rec.wait(task["uid"]) == ["FAILED"]
    assert "exited with code -11" in str(task["exception"])
    await until(lambda: len(dragon.workers()) == 5)


async def test_unplaced_tasks_fail_when_no_worker_is_left(dragon, tmp_path, monkeypatch):
    be = DragonNativeExecutionBackend(workers_per_node=1, inline_stdio=False)
    be._work_dir = str(tmp_path)
    await be
    try:
        rec = Recorder(be)
        a, b, waiting = fn_task(blocked), fn_task(blocked), fn_task(square, 2)
        await be.submit_tasks([a, b, waiting])
        assert be._tasks[waiting["uid"]].state == "queued"

        def broken_start(self):
            raise RuntimeError("no memory")

        with monkeypatch.context() as patch:
            patch.setattr(dragon.Process, "start", broken_start)
            for task in (a, b):
                be._tasks[task["uid"]].worker.proc._exit(-11)
            assert await rec.wait(waiting["uid"]) == ["FAILED"]
        assert "no Dragon worker" in str(waiting["exception"])
    finally:
        GATE.set()
        await be.shutdown()


@pytest.mark.parametrize("soft, warned", [(45, True), (4096, False)])
async def test_start_warns_when_a_node_may_run_out_of_descriptors(
    dragon, tmp_path, monkeypatch, caplog, soft, warned
):
    """Two workers and four CPUs per node take 3 * 2 + 4 descriptors, on top of Dragon's own."""
    monkeypatch.setattr(dragon_native.resource, "getrlimit", lambda which: (soft, 1 << 20))
    caplog.set_level("WARNING", logger=dragon_native._get_logger().name)
    be = DragonNativeExecutionBackend(workers_per_node=2, inline_stdio=False)
    be._work_dir = str(tmp_path)
    await be
    await be.shutdown()
    assert any("ulimit -n" in r.getMessage() for r in caplog.records) is warned


async def test_shutdown_gives_all_task_threads_one_deadline(backend, monkeypatch):
    """One after another, stuck threads would cost the deadline each: hours at scale."""
    monkeypatch.setattr(dragon_native, "_STOP_THREADS", 0.4)
    for i in range(5):
        rec = dragon_native._Task(f"task.stuck{i}", {}, "process", None, "token")
        rec.thread = threading.Thread(target=GATE.wait, args=(30,), daemon=True)
        rec.thread.start()
        backend._tasks[rec.uid] = rec
    t0 = time.monotonic()
    await backend.shutdown()
    assert time.monotonic() - t0 < 1.5


async def test_shutdown_cancels_running_and_queued_tasks_and_stops_the_pool(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=1, inline_stdio=False, kill_grace=0.2)
    be._work_dir = str(tmp_path)
    await be
    rec = Recorder(be)
    tasks = [fn_task(blocked) for _ in range(3)] + [exe_task("/bin/sleep", "30")]
    await be.submit_tasks(tasks)
    await until(lambda: be._tasks[tasks[3]["uid"]].puids)
    t0 = time.monotonic()
    await be.shutdown()
    assert time.monotonic() - t0 < 10
    for task in tasks:
        assert [s for u, s, _ in rec.records if u == task["uid"]] == ["RUNNING", "CANCELED"]
    assert all(p.done.is_set() for p in dragon.workers())
    assert be._results.destroyed
    assert not any(t.is_alive() for t in be._threads)
    with pytest.raises(RuntimeError):
        await be.submit_tasks([fn_task(square, 1)])


async def test_worker_that_never_starts_fails_the_start(dragon, monkeypatch):
    def broken_start(self):
        raise RuntimeError("no such policy")

    monkeypatch.setattr(dragon.Process, "start", broken_start)
    be = DragonNativeExecutionBackend(workers_per_node=1)
    with pytest.raises(RuntimeError, match="failed to start"):
        await be


async def test_concurrent_first_submits_start_one_pool(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=1, inline_stdio=False)
    be._work_dir = str(tmp_path)
    rec = Recorder(be)
    first, second = fn_task(square, 1), fn_task(square, 2)
    try:
        await asyncio.gather(be.submit_tasks([first]), be.submit_tasks([second]))
        assert len(dragon.workers()) == 2
        assert await rec.wait(first["uid"]) == ["DONE"]
        assert await rec.wait(second["uid"]) == ["DONE"]
    finally:
        await be.shutdown()


def test_backend_started_on_another_loop_delivers_on_the_submitting_one(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=1, inline_stdio=False)
    be._work_dir = str(tmp_path)
    asyncio.run(be._async_init())

    async def main():
        rec = Recorder(be)
        task = fn_task(square, 2)
        await be.submit_tasks([task])
        assert await rec.wait(task["uid"]) == ["DONE"]
        await be.shutdown()

    asyncio.run(main())


def test_state_for_a_closed_loop_reaches_the_next_loop(dragon, tmp_path):
    be = DragonNativeExecutionBackend(workers_per_node=1, inline_stdio=False)
    be._work_dir = str(tmp_path)
    asyncio.run(be._async_init())
    held = fn_task(square, 1)
    be._emit(dragon_native._Task(held["uid"], held, "pool", None, "t"), "DONE", {"return_value": 1})

    async def main():
        rec = Recorder(be)
        task = fn_task(square, 2)
        await be.submit_tasks([task])
        assert await rec.wait(task["uid"]) == ["DONE"]
        assert await rec.wait(held["uid"]) == ["DONE"]
        await be.shutdown()

    asyncio.run(main())


# ============================================================================
# Task callback contract
# ============================================================================


@pytest.mark.result_contract
async def test_stdout_and_stderr_are_strings_after_done_and_failed(backend):
    """On every path, with no output and when the backend itself failed the task."""
    rec = Recorder(backend)
    done = fn_task(square, 2)
    failed = fn_task(boom)
    dead = fn_task(blocked)
    invalid = fn_task(square, 2, placement={"host": "no-such-host"})
    executable = exe_task("true")
    await backend.submit_tasks([done, failed, dead, invalid, executable])
    backend._tasks[dead["uid"]].worker.proc._exit(-9)
    for task in (done, failed, dead, invalid, executable):
        await rec.wait(task["uid"])
        assert isinstance(task["stdout"], str) and isinstance(task["stderr"], str), task


@pytest.mark.result_contract
async def test_a_task_gets_one_terminal_state(backend):
    rec = Recorder(backend)
    tasks = [fn_task(square, n) for n in range(8)] + [exe_task("true")]
    await backend.submit_tasks(tasks)
    for task in tasks:
        assert len(await rec.wait(task["uid"])) == 1


# ============================================================================
# Worker-side code
# ============================================================================


def test_worker_call_runs_sync_and_async():
    assert worker_module.call(square, (3,), {}) == 9
    assert worker_module.call(asquare, (4,), {}) == 16


def test_worker_pack_replaces_an_unpicklable_value():
    msg = cloudpickle.loads(
        worker_module.pack(("done", 1, "u", True, threading.Lock(), None, "", ""), 4)
    )
    assert msg[3] is False and "cannot pickle" in str(cloudpickle.loads(msg[4]))


def test_worker_pack_keeps_the_value_apart_from_the_message():
    """The client must be able to load the message even when it cannot load the value."""
    msg = cloudpickle.loads(worker_module.pack(("result", "u", True, NoLoad(), None), 3))
    assert msg[:3] == ("result", "u", True) and isinstance(msg[3], bytes)
    assert dragon_native._load_value(msg)[2:4] != (True, None)
    ok, value = dragon_native._load_value(msg)[2:4]
    assert ok is False and "cannot unpickle" in str(value)


def test_worker_run_task_captures_output_inline():
    """Descriptors 1 and 2 are redirected, so C code and child processes are captured too.

    The test writes to descriptor 1 directly: pytest replaces ``sys.stdout`` with its own object.
    """

    def chatty():
        os.write(1, b"to stdout\n")
        os.system("echo from-a-child >&2")
        return 5

    ok, value, tb, out, err = worker_module.run_task("tok", chatty, (), {}, None, None, True)
    assert (ok, value, out, err) == (True, 5, "to stdout\n", "from-a-child\n")
    assert worker_module.TOKEN_ENV not in os.environ


class PoolFullError(Exception):
    pass


class FullPool:
    """A result queue that raises ``error`` on its first ``failures`` puts, and on large items."""

    def __init__(self, error, failures=0, limit=10**6):
        self.error, self.failures, self.limit, self.items = error, failures, limit, []

    def put(self, raw):
        if self.failures > 0 or len(raw) > self.limit:
            self.failures -= 1
            raise self.error
        self.items.append(cloudpickle.loads(raw))


def done_message(value):
    return (worker_module.DONE, 1, "u", True, value, None, "out", "err", 0.0)


@pytest.fixture
def pool_full(monkeypatch):
    monkeypatch.setattr(worker_module, "PoolFull", PoolFullError)


def test_worker_waits_for_room_on_a_full_pool(pool_full):
    """A finished task used to fail, and its worker die, when other results filled the pool."""
    results = FullPool(PoolFullError("pool is full"), failures=3)
    worker_module.send(results, done_message(5), 4)
    msg = dragon_native._load_value(results.items[0])
    assert msg == (worker_module.DONE, 1, "u", True, 5, None, "out", "err", 0.0)


def test_worker_reports_a_pool_that_stays_full(pool_full, monkeypatch):
    monkeypatch.setattr(worker_module, "PUT_PATIENCE", 0.05)
    results = FullPool(PoolFullError("pool is full"), limit=1000)
    worker_module.send(results, done_message(b"x" * 2000), 4)
    msg = dragon_native._load_value(results.items[0])
    assert msg[3] is False and "pool is full" in str(msg[4])
    assert msg[6:] == ("", "", 0.0)


def test_worker_reports_a_result_that_can_never_fit_at_once(pool_full):
    results = FullPool(RuntimeError("REQUESTED_SIZE_TOO_LARGE"), failures=1)
    t0 = time.monotonic()
    worker_module.send(results, (worker_module.RESULT, "u", True, 5, None), 3)
    assert time.monotonic() - t0 < 1
    msg = dragon_native._load_value(results.items[0])
    assert msg[2] is False and "REQUESTED_SIZE_TOO_LARGE" in str(msg[3])


def test_worker_run_task_appends_captured_output_to_files(tmp_path):
    def chatty():
        os.write(1, b"to stdout\n")
        os.write(2, b"to stderr\n")
        return 5

    out, err = str(tmp_path / "sub" / "u.stdout"), str(tmp_path / "sub" / "u.stderr")
    ok, value, tb, out_path, err_path = worker_module.run_task(
        "tok", chatty, (), {}, out, err, False
    )
    assert (ok, value, out_path, err_path) == (True, 5, out, err)
    assert open(out).read() == "to stdout\n" and open(err).read() == "to stderr\n"


def test_worker_run_task_with_a_path_it_cannot_write_fails_only_the_task():
    """It used to raise out of the worker, which cost a worker per task."""
    path = "/proc/no-such-dir/u"
    ok, value, tb, out, err = worker_module.run_task("tok", square, (2,), {}, path, path, False)
    assert (ok, out, err) == (False, "", "") and isinstance(value, OSError) and "Traceback" in tb
    assert worker_module.TOKEN_ENV not in os.environ


def test_worker_run_task_survives_a_task_that_removes_its_directory(tmp_path):
    """A task that succeeded used to be reported as a dead worker."""
    gone = tmp_path / "gone"
    gone.mkdir()

    def leaves_nothing():
        os.chdir(str(gone))
        gone.rmdir()
        return 7

    cwd = os.getcwd()
    ok, value, *_ = worker_module.run_task("tok", leaves_nothing, (), {}, None, None, False)
    assert (ok, value, os.getcwd()) == (True, 7, cwd)


@pytest.mark.skipif(sys.platform != "linux", reason="reads /proc and uses GNU grep's -z")
def test_reap_script_kills_the_processes_that_carry_the_token():
    token = f"{worker_module.TOKEN_ENV}=reap-test.{os.getpid()}"
    name, value = token.split("=", 1)
    marked = subprocess.Popen(["/bin/sleep", "30"], env={**os.environ, name: value})
    other = subprocess.Popen(["/bin/sleep", "30"])
    try:
        subprocess.run(
            ["/bin/sh", "-c", worker_module.REAP_SCRIPT, "reap", token, "KILL"], check=True
        )
        assert marked.wait(timeout=5) == -signal.SIGKILL
        assert other.poll() is None
    finally:
        marked.kill()
        other.kill()
        marked.wait()
        other.wait()


def test_worker_run_task_restores_cwd_and_sets_the_token(tmp_path):
    def wander():
        os.chdir(str(tmp_path))
        return os.environ[worker_module.TOKEN_ENV]

    cwd = os.getcwd()
    ok, value, *_ = worker_module.run_task("tok-1", wander, (), {}, None, None, False)
    assert (ok, value, os.getcwd()) == (True, "tok-1", cwd)


# ============================================================================
# Real Dragon runtime (``dragon python -m pytest``)
# ============================================================================


needs_runtime = pytest.mark.skipif(
    "DRAGON_MY_PUID" not in os.environ,
    reason="needs a running Dragon runtime (dragon python -m pytest)",
)


@needs_runtime
async def test_real_runtime_mixed_tasks(tmp_path):
    """Function, async function, placed function, executable and a group on a real runtime."""
    from dragon.infrastructure.policy import Policy
    from dragon.native.machine import current

    # Dragon's workers cannot import this test module: send its functions by value.
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    be = await DragonNativeExecutionBackend(workers_per_node=4)
    be._work_dir = str(tmp_path)
    rec = Recorder(be)
    host = current().hostname
    placed = Policy(placement=Policy.Placement.HOST_NAME, host_name=host)
    tasks = [
        fn_task(square, 7),
        fn_task(asquare, 8),
        fn_task(square, 3, task_backend_specific_kwargs={"process_template": {"policy": placed}}),
        exe_task("/bin/echo", "hello"),
        exe_task(
            "/bin/sh",
            "-c",
            "echo $$",
            task_backend_specific_kwargs={"process_templates": [(2, {})]},
        ),
        fn_task(boom),
    ]
    try:
        await be.submit_tasks(tasks)
        states = [(await rec.wait(t["uid"], 120))[0] for t in tasks]
        assert states == ["DONE"] * 5 + ["FAILED"], [t.get("exception") for t in tasks]
        assert [t["return_value"] for t in tasks[:3]] == [49, 64, 9]
        assert tasks[3]["stdout"] == "hello\n"
        assert len(tasks[4]["stdout"].split()) == 2
    finally:
        await be.shutdown()
        cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
