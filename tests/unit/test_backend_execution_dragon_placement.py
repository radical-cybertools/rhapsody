"""DragonExecutionBackend placement, tested without a Dragon install.

AsyncFlow's ``ResourcePolicy`` in node mode writes ``task["placement"] = {"node", "host", "gpus"}``
into the task description. ``build_task`` turns it into a Dragon host-name ``Policy`` on the task's
process template(s); a native function task becomes one placed process. These tests swap Batch,
ProcessTemplate and Policy for fakes, so they also run where Dragon is absent (the main Dragon test
module skips there).
"""

import enum
import logging
import os
import pickle
from dataclasses import dataclass
from dataclasses import field
from unittest.mock import patch

import pytest

from rhapsody.backends.execution import dragon as dragon_module
from rhapsody.backends.execution.dragon import DragonExecutionBackend


@dataclass
class FakePolicy:
    """Stands in for dragon.infrastructure.policy.Policy."""

    class Placement(enum.IntEnum):
        HOST_NAME = -3
        DEFAULT = -1

    placement: Placement = Placement.DEFAULT
    host_name: str = ""
    gpu_affinity: list = field(default_factory=list)


class FakeProcessTemplate:
    """Stands in for dragon.native.process.ProcessTemplate; keeps what it was given."""

    def __init__(self, target, args=None, kwargs=None, policy=None, **extra):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.policy = policy
        self.extra = extra


class FakeBatchTask:
    def __init__(self, tuid):
        self.uid = tuid


class FakeOptionsProxy:
    """Records which Batch submission path build_task took."""

    def __init__(self, batch):
        self._batch = batch

    def _submit(self, kind, *payload):
        self._batch.calls.append((kind, payload))
        return FakeBatchTask(f"tuid-{len(self._batch.calls)}")

    def function(self, target, *args, **kwargs):
        return self._submit("function", target, args, kwargs)

    def process(self, process_template):
        return self._submit("process", process_template)

    def job(self, process_templates):
        return self._submit("job", process_templates)


class FakeBatch:
    num_workers = 1
    num_managers = 1

    def __init__(self):
        self.calls = []

    def options(self, **kwargs):
        return FakeOptionsProxy(self)


@pytest.fixture
def backend(tmp_path):
    """DragonExecutionBackend on a FakeBatch, with Dragon's Policy and ProcessTemplate faked."""
    with (
        patch.object(dragon_module, "Batch", return_value=FakeBatch()),
        patch.object(dragon_module, "ProcessTemplate", FakeProcessTemplate),
        patch.object(dragon_module, "Policy", FakePolicy),
    ):
        backend = DragonExecutionBackend()
        backend._work_dir = str(tmp_path)
        # Dragon's node list, which build_task checks placements against.
        backend._node_gpus = {"gpua042": {0, 1, 2, 3}, "cn001": set()}
        # The GPU reachability probe has its own tests below.
        backend._gpus_probed = True
        yield backend


def _task(uid="task.000001", placement=None, backend_kwargs=None, **fields):
    """A task description as AsyncFlow hands it to the backend; a function task by default."""
    task = {
        "uid": uid,
        "name": uid,
        "task_backend_specific_kwargs": backend_kwargs or {},
        **fields,
    }
    if "executable" not in fields:
        task.setdefault("function", _double)
        task.setdefault("args", [21])
        task.setdefault("kwargs", {})
    if placement is not None:
        task["placement"] = placement
    return task


def _double(x):
    return 2 * x


def _fail():
    raise ValueError("boom")


PLACEMENT = {"node": 1, "host": "gpua042", "gpus": [2, 3]}


def _assert_placed(policy, host="gpua042", gpus=(2, 3)):
    assert policy.placement == FakePolicy.Placement.HOST_NAME
    assert policy.host_name == host
    assert policy.gpu_affinity == list(gpus)


# ============================================================================
# No placement: today's behaviour
# ============================================================================


async def test_function_without_placement_stays_native(backend):
    await backend.build_task(_task())

    [(kind, (target, args, kwargs))] = backend.batch.calls
    assert kind == "function"
    assert target is _double
    assert args == (21,)
    assert backend._task_registry["task.000001"]["result_path"] is None
    assert backend._task_registry["task.000001"]["is_native_function"] is True


async def test_placement_without_host_stays_native(backend):
    await backend.build_task(_task(placement={"node": 0, "host": None, "gpus": [0]}))

    [(kind, _)] = backend.batch.calls
    assert kind == "function"


async def test_executable_without_placement_has_no_policy(backend):
    await backend.build_task(_task(executable="/bin/hostname", arguments=["-s"]))

    [(kind, (pt,))] = backend.batch.calls
    assert kind == "process"
    assert pt.target == "/bin/hostname"
    assert pt.args == ("-s",)
    assert pt.policy is None


async def test_process_templates_without_placement_are_untouched(backend):
    user_policy = object()
    task = _task(backend_kwargs={"process_templates": [(2, {"cwd": "/a", "policy": user_policy})]})
    await backend.build_task(task)

    [(kind, ([(nranks, pt)],))] = backend.batch.calls
    assert kind == "job"
    assert nranks == 2
    assert pt.policy is user_policy
    assert pt.extra == {"cwd": "/a"}


# ============================================================================
# Placement: function, process and job tasks
# ============================================================================


async def test_placed_function_runs_as_one_placed_process(backend, tmp_path):
    await backend.build_task(_task(placement=PLACEMENT, kwargs={"unused": None}))

    [(kind, (pt,))] = backend.batch.calls
    assert kind == "process"
    _assert_placed(pt.policy)
    assert pt.target is dragon_module._run_placed_function
    result_path, target, args, kwargs = pt.args
    assert result_path == os.path.join(str(tmp_path), "task.000001.result")
    assert target is _double
    assert args == (21,)
    assert kwargs == {"unused": None}

    reg = backend._task_registry["task.000001"]
    assert reg["result_path"] == result_path
    assert reg["is_native_function"] is True


async def test_placed_function_without_gpus_pins_host_only(backend):
    await backend.build_task(_task(placement={"node": 0, "host": "cn001", "gpus": []}))

    [(kind, (pt,))] = backend.batch.calls
    assert kind == "process"
    _assert_placed(pt.policy, host="cn001", gpus=())


async def test_placed_executable_gets_policy(backend):
    task = _task(executable="/bin/hostname", arguments=[], placement=PLACEMENT)
    await backend.build_task(task)

    [(kind, (pt,))] = backend.batch.calls
    assert kind == "process"
    assert pt.target == "/bin/hostname"
    _assert_placed(pt.policy)
    assert backend._task_registry["task.000001"]["result_path"] is None


async def test_placed_process_template_keeps_user_fields(backend):
    task = _task(placement=PLACEMENT, backend_kwargs={"process_template": {"cwd": "/run"}})
    await backend.build_task(task)

    [(kind, (pt,))] = backend.batch.calls
    assert kind == "process"
    assert pt.target is _double
    assert pt.extra == {"cwd": "/run"}
    _assert_placed(pt.policy)


async def test_placed_job_puts_policy_on_every_template(backend):
    task = _task(
        executable="/bin/hostname",
        arguments=[],
        placement=PLACEMENT,
        backend_kwargs={"process_templates": [(2, {"cwd": "/a"}), (1, {})]},
    )
    await backend.build_task(task)

    [(kind, (templates,))] = backend.batch.calls
    assert kind == "job"
    assert [n for n, _ in templates] == [2, 1]
    for _, pt in templates:
        _assert_placed(pt.policy)


async def test_user_policy_wins_with_warning(backend, caplog):
    user_policy = FakePolicy(gpu_affinity=[0])
    task = _task(
        placement=PLACEMENT,
        backend_kwargs={"process_templates": [(1, {"policy": user_policy}), (1, {})]},
    )
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(task)

    [(kind, (templates,))] = backend.batch.calls
    assert kind == "job"
    (_, mine), (_, placed) = templates
    assert mine.policy is user_policy
    _assert_placed(placed.policy)
    assert "overrides placement" in caplog.text


# ============================================================================
# Placement checks: Dragon aborts on an unknown host and drops unknown GPUs
# ============================================================================


async def test_unknown_host_fails_the_task_only(backend):
    with pytest.raises(ValueError, match="not a Dragon node"):
        await backend.build_task(_task(placement={"node": 0, "host": "three", "gpus": []}))

    assert backend.batch.calls == []
    assert backend._task_registry == {}


async def test_unknown_gpu_fails_the_task_only(backend):
    with pytest.raises(ValueError, match=r"GPUs \[4\]"):
        await backend.build_task(_task(placement={"node": 1, "host": "gpua042", "gpus": [3, 4]}))

    assert backend.batch.calls == []


async def test_dragon_nodes_are_looked_up_once(backend):
    class FakeNode:
        def __init__(self, h_uid):
            self.hostname, self.gpus = {1: ("gpua042", [0, 1]), 2: ("cn001", None)}[h_uid]

    class FakeSystem:
        calls = 0

        def __init__(self):
            FakeSystem.calls += 1
            self.nodes = [1, 2]

    backend._node_gpus = None
    with (
        patch.object(dragon_module, "System", FakeSystem),
        patch.object(dragon_module, "Node", FakeNode),
    ):
        await backend.build_task(_task(uid="t.1", placement={"host": "gpua042", "gpus": [1]}))
        await backend.build_task(_task(uid="t.2", placement={"host": "cn001", "gpus": []}))

    assert FakeSystem.calls == 1
    assert backend._node_gpus == {"gpua042": {0, 1}, "cn001": set()}
    assert [kind for kind, _ in backend.batch.calls] == ["process", "process"]


# ============================================================================
# GPU reachability: Dragon lists GPUs a Slurm job without --gres cannot use
# ============================================================================


class FakeCurrentNode:
    hostname = "gpua042"


@pytest.fixture
def probe(backend):
    """Arm the one-time GPU probe.

    Set ``probe.reachable`` to what CUDA reports, ``probe.loaded`` to whether this Python can load
    libcuda, and ``probe.smi`` to what ``nvidia-smi -L`` counts.
    """

    class Probe:
        reachable = None
        loaded = True
        smi = None
        calls = 0
        smi_calls = 0

        @classmethod
        def count(cls):
            cls.calls += 1
            return cls.reachable, cls.loaded

        @classmethod
        def smi_count(cls):
            cls.smi_calls += 1
            return cls.smi

    backend._gpus_probed = False
    with (
        patch.object(dragon_module, "current_node", FakeCurrentNode),
        patch.object(dragon_module, "_cuda_device_count", Probe.count),
        patch.object(dragon_module, "_nvidia_smi_device_count", Probe.smi_count),
    ):
        yield Probe


async def test_unreachable_gpus_warn_once(backend, probe, caplog):
    probe.reachable = 0
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(_task(uid="t.1", placement=PLACEMENT))
        await backend.build_task(_task(uid="t.2", placement=PLACEMENT))

    assert probe.calls == 1
    assert probe.smi_calls == 0
    assert caplog.text.count("CUDA reaches 0 of them") == 1
    # A warning, not an error: both tasks are still submitted with their placement.
    assert [kind for kind, _ in backend.batch.calls] == ["process", "process"]


async def test_reachable_gpus_do_not_warn(backend, probe, caplog):
    probe.reachable = 4
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(_task(placement=PLACEMENT))

    assert probe.calls == 1
    assert "reaches" not in caplog.text


async def test_unknown_reachability_does_not_warn(backend, probe, caplog):
    probe.reachable = None  # a CUDA error other than "no device"
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(_task(placement=PLACEMENT))

    assert probe.smi_calls == 0
    assert caplog.text == ""


async def test_without_libcuda_nvidia_smi_counts(backend, probe, caplog):
    # rust job 24840: EESSI's Python cannot load libcuda on julia, and the job has no --gres.
    probe.loaded = False
    probe.smi = 0
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(_task(placement=PLACEMENT))

    assert probe.smi_calls == 1
    assert "cannot load libcuda.so.1" in caplog.text
    assert "nvidia-smi reaches 0 of them" in caplog.text


async def test_without_libcuda_or_nvidia_smi_only_names_libcuda(backend, probe, caplog):
    probe.loaded = False
    probe.smi = None
    with caplog.at_level(logging.WARNING, logger=dragon_module.__name__):
        await backend.build_task(_task(placement=PLACEMENT))

    assert "cannot load libcuda.so.1" in caplog.text
    assert "reaches" not in caplog.text


async def test_no_probe_without_gpus_in_the_placement(backend, probe):
    await backend.build_task(_task(placement={"node": 0, "host": "cn001", "gpus": []}))

    assert probe.calls == 0
    assert backend._gpus_probed is False


async def test_no_probe_when_this_node_has_no_listed_gpus(backend, probe):
    # This process runs on cn001, which has no GPUs to compare against.
    with patch.object(FakeCurrentNode, "hostname", "cn001"):
        await backend.build_task(_task(placement=PLACEMENT))

    assert probe.calls == 0


class FakeCompleted:
    def __init__(self, returncode, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (FakeCompleted(0, "2\n"), (2, True)),
        (FakeCompleted(0, "0\n"), (0, True)),
        (FakeCompleted(2), (None, False)),  # no libcuda
        (FakeCompleted(3), (None, True)),  # CUDA error other than "no device"
        (FakeCompleted(0, "garbage"), (None, True)),
    ],
)
def test_cuda_device_count_parses_the_probe(outcome, expected):
    with patch.object(dragon_module.subprocess, "run", return_value=outcome):
        assert dragon_module._cuda_device_count() == expected


def test_cuda_device_count_survives_a_hung_probe():
    hung = dragon_module.subprocess.TimeoutExpired(cmd="probe", timeout=60)
    with patch.object(dragon_module.subprocess, "run", side_effect=hung):
        assert dragon_module._cuda_device_count() == (None, True)


def test_cuda_device_count_runs_for_real():
    count, loaded = dragon_module._cuda_device_count()
    assert count is None or count >= 0
    assert loaded or count is None


SMI_TWO_GPUS = (
    "GPU 0: NVIDIA A100-SXM4-40GB (UUID: GPU-ccdb6af5-102b-3fb4-4e06-8b7aaeba0578)\n"
    "GPU 1: NVIDIA A100-SXM4-40GB (UUID: GPU-43539da6-e86a-d93e-8db2-f8814ef47c41)\n"
)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (FakeCompleted(0, SMI_TWO_GPUS), 2),
        (FakeCompleted(0, "No devices found.\n"), 0),  # rust, job without --gres
        (FakeCompleted(6, "No devices were found\n"), 0),  # versions that exit non-zero
        (FakeCompleted(9, stderr="NVIDIA-SMI has failed\n"), None),
    ],
)
def test_nvidia_smi_device_count_counts_gpu_lines(outcome, expected):
    with (
        patch.object(dragon_module.shutil, "which", return_value="/usr/bin/nvidia-smi"),
        patch.object(dragon_module.subprocess, "run", return_value=outcome),
    ):
        assert dragon_module._nvidia_smi_device_count() == expected


def test_nvidia_smi_device_count_without_nvidia_smi():
    with patch.object(dragon_module.shutil, "which", return_value=None):
        assert dragon_module._nvidia_smi_device_count() is None


# ============================================================================
# Placed function outcome: written by the process, read by the monitor
# ============================================================================


def test_placed_function_value_round_trips(tmp_path):
    path = str(tmp_path / "t.result")
    dragon_module._run_placed_function(path, _double, (21,), {})

    assert dragon_module._read_placed_result(path, 0, False, None) == (42, False, None)
    assert not os.path.exists(path)


def test_placed_function_exception_round_trips(tmp_path):
    path = str(tmp_path / "t.result")
    dragon_module._run_placed_function(path, _fail, (), {})

    result, raised, tb = dragon_module._read_placed_result(path, 0, False, None)
    assert raised is True
    assert isinstance(result, ValueError)
    assert "boom" in tb


def test_placed_function_unpicklable_value_fails(tmp_path):
    path = str(tmp_path / "t.result")
    dragon_module._run_placed_function(path, lambda: (lambda: None), (), {})

    result, raised, _ = dragon_module._read_placed_result(path, 0, False, None)
    assert raised is True
    assert "cannot pickle" in str(result)


def test_missing_result_after_nonzero_exit_fails_with_exit_code(tmp_path):
    result, raised, _ = dragon_module._read_placed_result(str(tmp_path / "x"), 137, False, None)
    assert raised is True
    assert isinstance(result, SystemExit)
    assert result.code == 137


def test_missing_result_after_clean_exit_names_shared_fs(tmp_path):
    result, raised, _ = dragon_module._read_placed_result(str(tmp_path / "x"), 0, False, None)
    assert raised is True
    assert "shared" in str(result)


def test_dragon_side_failure_is_kept_and_file_removed(tmp_path):
    path = tmp_path / "t.result"
    path.write_bytes(pickle.dumps((1, False, None)))
    timeout = TimeoutError("walltime")

    assert dragon_module._read_placed_result(str(path), timeout, True, "tb") == (
        timeout,
        True,
        "tb",
    )
    assert not path.exists()


class FakeDoneTask(FakeBatchTask):
    """A placed process that exited 0; Batch reports the exit code."""

    stdout_path = stderr_path = traceback = None

    def get(self, block=True):
        return 0

    def get_stdout(self, block=False):
        return ""

    def get_stderr(self, block=False):
        return ""


def test_monitor_delivers_placed_function_value(backend, tmp_path):
    uid, tuid = "task.000001", "tuid-1"
    path = str(tmp_path / f"{uid}.result")
    dragon_module._run_placed_function(path, _double, (21,), {})
    backend._task_registry[uid] = {
        "uid": uid,
        "description": {"uid": uid},
        "is_native_function": True,
        "result_path": path,
    }
    backend._monitored_batches[tuid] = (FakeDoneTask(tuid), uid)

    delivered = []

    class Loop:
        def call_soon_threadsafe(self, fn, completions):
            delivered.extend(completions)

    polls = iter([tuid])

    def poll(timeout=None):
        tuid = next(polls, None)
        if tuid is None:
            backend._shutdown_event.set()
        return tuid

    backend._loop = Loop()
    backend.batch.poll = poll
    backend._monitor_loop()

    [(uid_c, result, _tb, raised, _out, _err)] = delivered
    assert (uid_c, result, raised) == (uid, 42, False)
