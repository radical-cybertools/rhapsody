"""Unit tests for Dask parallel execution backend.

This module tests the Dask parallel execution backend defined in
rhapsody.backends.execution.dask_parallel.
"""

import asyncio

import pytest

from rhapsody import ComputeTask


async def _pickling_regression_target(n):
    """Module-level async target used by test_wraps_closure_pickling_regression."""
    return n


def _create_task_that_closes_coro(coro, **kwargs):
    """Stand-in for `asyncio.create_task` in tests that never let the scheduled completion coroutine
    run — closes it immediately instead of leaking it, which otherwise trips a "coroutine was never
    awaited" RuntimeWarning at GC time."""
    coro.close()
    return None


def test_dask_backend_import():
    """Test that DaskExecutionBackend can be imported."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        assert DaskExecutionBackend is not None
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_class_exists():
    """Test that DaskExecutionBackend class exists and inherits from base."""
    try:
        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.base import BaseBackend

        # Check inheritance
        assert issubclass(DaskExecutionBackend, BaseBackend)
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_init():
    """Test DaskExecutionBackend initialization."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        # Test basic initialization
        backend = DaskExecutionBackend()
        assert backend is not None
        assert not backend._initialized
        assert backend._client is None
        assert backend.tasks == {}
        assert backend._runtime == {}

        # Test initialization with resources
        resources = {"n_workers": 2, "threads_per_worker": 1}
        backend_with_resources = DaskExecutionBackend(resources)
        assert backend_with_resources._resources == resources

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_import_error():
    """Test that DaskExecutionBackend raises ImportError when Dask is not available."""
    try:
        # This will only run if dask is actually available
        import dask.distributed

        pytest.skip("Dask is available, cannot test ImportError scenario")
    except ImportError:
        # Dask is not available, so we should get ImportError
        from rhapsody.backends import DaskExecutionBackend

        with pytest.raises(ImportError, match="Dask is required for DaskExecutionBackend"):
            DaskExecutionBackend()


@pytest.mark.asyncio
async def test_dask_backend_async_init():
    """Test DaskExecutionBackend async initialization."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Test that it's awaitable
        assert hasattr(backend, "__await__")

        # Test async initialization (this might fail due to no Dask cluster)
        try:
            initialized_backend = await backend
            assert initialized_backend._initialized
            assert initialized_backend is backend  # Should return self

            # Test state
            state = await backend.state()
            assert state == "INITIALIZED"

            # Cleanup
            await backend.shutdown()

        except Exception as e:
            # Expected if no Dask cluster is available
            print(f"Async init failed (expected): {e}")

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_context_manager():
    """Test DaskExecutionBackend as async context manager."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        try:
            async with DaskExecutionBackend() as backend:
                assert backend._initialized
                # Test basic functionality
                assert hasattr(backend, "submit_tasks")
                assert hasattr(backend, "cancel_task")

        except Exception as e:
            # Expected if no Dask cluster is available
            print(f"Context manager test failed (expected): {e}")

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_callback_registration():
    """Test callback registration functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        def test_callback(task, state):
            pass

        # Test callback registration
        backend.register_callback(test_callback)
        assert backend._callback_func is test_callback

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_state_mapper():
    """Test state mapper functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # This should work even without initialization
        state_mapper = backend.get_task_states_map()
        assert state_mapper is not None

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_task_validation():
    """Test task validation and error handling."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Mock callback to capture calls
        callback_calls = []

        def mock_callback(task, state):
            callback_calls.append((task, state))

        backend.register_callback(mock_callback)

        # Test without initialization (should raise RuntimeError)
        with pytest.raises(RuntimeError, match="DaskExecutionBackend must be awaited"):
            await backend.submit_tasks([])

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_task_submission_routing():
    """Test that tasks are routed to the correct submission methods."""
    try:
        from unittest.mock import AsyncMock
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True
        backend._client = MagicMock()
        backend._client.status = "running"

        # Executable tasks route to _submit_executable (not FAILED)
        with patch.object(backend, "_submit_executable", new_callable=AsyncMock) as mock_exec:
            executable_task = ComputeTask(executable="/bin/echo", arguments=["hello"])
            await backend.submit_tasks([executable_task])
            mock_exec.assert_called_once()

        # Both sync and async function tasks route to _submit_function — Dask itself
        # (not a RHAPSODY-side wrapper) distinguishes sync/async execution once the
        # callable reaches a worker, so there is only one dispatch path here.
        with patch.object(backend, "_submit_function", new_callable=AsyncMock) as mock_fn:

            def sync_fn():
                return "sync"

            sync_task = ComputeTask(function=sync_fn, args=[], kwargs={})
            await backend.submit_tasks([sync_task])
            mock_fn.assert_called_once()

        with patch.object(backend, "_submit_function", new_callable=AsyncMock) as mock_fn:

            async def async_fn():
                return "async"

            async_task = ComputeTask(function=async_fn, args=[], kwargs={})
            await backend.submit_tasks([async_task])
            mock_fn.assert_called_once()

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_cancel_functionality():
    """Test task cancellation functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()
        backend._initialized = True  # Bypass initialization

        # Test canceling non-existent task
        result = await backend.cancel_task("nonexistent")
        assert result is False

        # Test cancel_all_tasks with no tasks
        cancelled_count = await backend.cancel_all_tasks()
        assert cancelled_count == 0

    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_dask_backend_class_methods():
    """Test DaskExecutionBackend class methods."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        # Test that create class method exists
        assert hasattr(DaskExecutionBackend, "create")
        assert callable(DaskExecutionBackend.create)

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_backend_shutdown():
    """Test DaskExecutionBackend shutdown functionality."""
    try:
        from rhapsody.backends import DaskExecutionBackend

        backend = DaskExecutionBackend()

        # Test shutdown without initialization
        await backend.shutdown()
        assert backend._client is None
        assert not backend._initialized

        # Test shutdown after manual initialization flag setting
        backend = DaskExecutionBackend()
        backend._initialized = True
        backend.tasks = {"test": "task"}
        backend._runtime = {"test": "runtime"}

        await backend.shutdown()
        assert backend._client is None
        assert not backend._initialized
        assert len(backend.tasks) == 0
        assert len(backend._runtime) == 0

    except ImportError:
        pytest.skip("Dask dependencies not available")


# ---------------------------------------------------------------------------
# capture_stdio tests
# ---------------------------------------------------------------------------


def test_run_executable_capture_stdio_writes_files(tmp_path):
    """_run_executable with capture_stdio=True writes files and returns their paths."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout_val, stderr_val, returncode = _run_executable(
            "/bin/bash",
            ["-c", "echo hello; echo err >&2"],
            capture_stdio=True,
            output_dir=str(tmp_path),
            uid="task.000001",
        )

        assert returncode == 0
        assert stdout_val.endswith(".stdout")
        assert stderr_val.endswith(".stderr")
        assert open(stdout_val).read() == "hello\n"
        assert open(stderr_val).read() == "err\n"
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_run_executable_capture_stdio_false_returns_strings():
    """_run_executable without capture_stdio returns decoded strings (default)."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout, stderr, returncode = _run_executable("/bin/echo", ["world"])
        assert returncode == 0
        assert stdout == "world\n"
        assert stderr == ""
    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_submit_executable_passes_capture_stdio(tmp_path):
    """_submit_executable forwards capture_stdio and output_dir to _run_executable."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime

        backend = DaskExecutionBackend()
        backend._initialized = True
        backend._work_dir = str(tmp_path)

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(executable="/bin/echo", arguments=["hi"], capture_stdio=True)
        backend.tasks[task["uid"]] = task
        backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="executable")

        with patch("asyncio.create_task", side_effect=_create_task_that_closes_coro):
            await backend._submit_executable(task)

        assert captured.get("capture_stdio") is True
        assert captured.get("output_dir") == str(tmp_path)
        assert captured.get("uid") == task["uid"]

    except ImportError:
        pytest.skip("Dask dependencies not available")


# ---------------------------------------------------------------------------
# _run_executable tests
# ---------------------------------------------------------------------------


def test_run_executable_is_picklable():
    """_run_executable must be picklable so Dask can ship it to workers."""
    import pickle

    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        pickled = pickle.dumps(_run_executable)
        assert len(pickled) > 0
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_run_executable_captures_stdout():
    """_run_executable returns (stdout, stderr, returncode) correctly."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout, stderr, returncode = _run_executable("/bin/echo", ["hello"])
        assert returncode == 0
        assert "hello" in stdout
        assert stderr == ""
    except ImportError:
        pytest.skip("Dask dependencies not available")


def test_run_executable_captures_stderr_and_nonzero_exit():
    """_run_executable captures stderr and non-zero exit codes."""
    try:
        from rhapsody.backends.execution.dask_parallel import _run_executable

        stdout, stderr, returncode = _run_executable("/bin/bash", ["-c", "echo err >&2; exit 1"])
        assert returncode == 1
        assert "err" in stderr
    except ImportError:
        pytest.skip("Dask dependencies not available")


# ---------------------------------------------------------------------------
# cwd tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_submit_executable_cwd_from_bksp():
    """_submit_executable forwards cwd from task_backend_specific_kwargs to _run_executable."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime

        backend = DaskExecutionBackend()
        backend._initialized = True

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            future = MagicMock()
            future.__await__ = lambda self: iter([])
            return future

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(
            executable="/bin/pwd",
            task_backend_specific_kwargs={"cwd": "/tmp"},
        )
        backend.tasks[task["uid"]] = task
        backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="executable")

        with patch("asyncio.create_task", side_effect=_create_task_that_closes_coro):
            await backend._submit_executable(task)

        assert captured.get("cwd") == "/tmp"

    except ImportError:
        pytest.skip("Dask dependencies not available")


@pytest.mark.asyncio
async def test_dask_submit_executable_no_cwd():
    """When no cwd is set, cwd is None (no crash)."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime

        backend = DaskExecutionBackend()
        backend._initialized = True

        captured = {}

        def fake_submit(fn, *args, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        backend._client = MagicMock()
        backend._client.submit = fake_submit
        backend._client.scheduler_info.return_value = {"workers": {}}

        task = ComputeTask(executable="/bin/pwd")
        backend.tasks[task["uid"]] = task
        backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="executable")

        with patch("asyncio.create_task", side_effect=_create_task_that_closes_coro):
            await backend._submit_executable(task)

        assert captured.get("cwd") is None

    except ImportError:
        pytest.skip("Dask dependencies not available")


# ---------------------------------------------------------------------------
# result_contract tests — stdout/stderr must be str after any DONE/FAILED
# ---------------------------------------------------------------------------


@pytest.mark.result_contract
@pytest.mark.asyncio
async def test_dask_function_done_stdout_is_string():
    """Function DONE callback must include stdout as a str."""
    try:
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime
    except ImportError:
        pytest.skip("Dask not available")

    backend = DaskExecutionBackend()
    backend._initialized = True
    captured = []
    backend.register_callback(lambda t, s: captured.append((dict(t), s)))

    task = ComputeTask(function=lambda: 99, args=[])
    backend.tasks[task["uid"]] = {}
    backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="function")

    fut = asyncio.get_event_loop().create_future()
    with patch.object(backend, "_client") as mc:
        mc.submit.return_value = fut
        asyncio.create_task(backend._submit_function(task))
        await asyncio.sleep(0)
        fut.set_result(99)
        await asyncio.sleep(0)

    done = [(t, s) for t, s in captured if s == "DONE"]
    assert done, "DONE callback never fired"
    assert done[0][0].get("stdout") == ""
    assert done[0][0].get("stderr") == ""


@pytest.mark.result_contract
@pytest.mark.asyncio
async def test_dask_function_failed_stdout_is_string():
    """Function FAILED callback must include stdout as a str."""
    try:
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime
    except ImportError:
        pytest.skip("Dask not available")

    backend = DaskExecutionBackend()
    backend._initialized = True
    captured = []
    backend.register_callback(lambda t, s: captured.append((dict(t), s)))

    task = ComputeTask(function=lambda: 1 / 0, args=[])
    backend.tasks[task["uid"]] = {}
    backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="function")

    fut = asyncio.get_event_loop().create_future()
    with patch.object(backend, "_client") as mc:
        mc.submit.return_value = fut
        asyncio.create_task(backend._submit_function(task))
        await asyncio.sleep(0)
        fut.set_exception(ZeroDivisionError("div by zero"))
        await asyncio.sleep(0)

    failed = [(t, s) for t, s in captured if s == "FAILED"]
    assert failed, "FAILED callback never fired"
    assert isinstance(failed[0][0].get("stdout"), str)


# ---------------------------------------------------------------------------
# Pickling regression — proves the old @wraps-closure pattern was the bug,
# and that submitting the real callable directly is not.
# ---------------------------------------------------------------------------


def test_wraps_closure_pickling_regression():
    """Regression test for the original `PicklingError`.

    The deleted `_submit_async_function` wrapped every async task callable in a
    local closure decorated with `@wraps(task["function"])`. `@wraps` copies the
    original function's `__module__`/`__qualname__` onto the closure, so pickling
    it by reference resolves to a *different* object living at that name and
    raises `PicklingError: ... it's not the same object as ...` — this is the
    exact shape of the original bug (`Can't pickle <function infer_batch_task
    ...>: it's not the same object as __main__.infer_batch_task`).

    Proves (a) the old wrapping pattern really does break pickling, and (b) the
    new pattern — submitting the real callable directly, optionally bound via
    `functools.partial` for kwargs — does not.
    """
    import pickle
    from functools import partial
    from functools import wraps

    # Old (deleted) pattern: a closure decorated with @wraps(original).
    @wraps(_pickling_regression_target)
    async def async_wrapper():
        return await _pickling_regression_target(1)

    with pytest.raises(pickle.PicklingError, match="not the same object as"):
        pickle.dumps(async_wrapper)

    # New pattern: submit the real module-level function directly, or via partial
    # to pre-bind kwargs — never through a renamed/re-wrapped closure.
    restored_fn = pickle.loads(pickle.dumps(_pickling_regression_target))
    assert restored_fn is _pickling_regression_target

    bound = partial(_pickling_regression_target, 1)
    restored_partial = pickle.loads(pickle.dumps(bound))
    assert restored_partial.func is _pickling_regression_target
    assert restored_partial.args == (1,)


# ---------------------------------------------------------------------------
# Client/cluster ownership and validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_external_sync_client_rejected():
    """A caller-supplied Client that isn't asynchronous=True must be rejected at init."""
    try:
        from unittest.mock import MagicMock

        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    sync_client = MagicMock()
    sync_client.asynchronous = False

    with pytest.raises(ValueError, match="asynchronous=True"):
        await DaskExecutionBackend(client=sync_client)


# ---------------------------------------------------------------------------
# Callback handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_async_callback_registered_and_invoked():
    """_invoke_callback awaits an async callback rather than firing-and-forgetting it."""
    try:
        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()
    backend._initialized = True

    calls = []

    async def async_callback(task, state):
        await asyncio.sleep(0)
        calls.append((task["uid"], state))

    backend.register_callback(async_callback)

    task = ComputeTask(function=lambda: 1, args=[])
    await backend._invoke_callback(task, "RUNNING")

    assert calls == [(task["uid"], "RUNNING")]


@pytest.mark.asyncio
async def test_dask_callback_exception_does_not_break_task_completion():
    """A raising callback must not crash `_on_done` or corrupt task state."""
    try:
        from rhapsody.backends import DaskExecutionBackend
        from rhapsody.backends.execution.dask_parallel import _TaskRuntime
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()
    backend._initialized = True

    def bad_callback(task, state):
        raise RuntimeError("callback boom")

    backend.register_callback(bad_callback)

    task = ComputeTask(function=lambda: 42, args=[])
    backend.tasks[task["uid"]] = task
    backend._runtime[task["uid"]] = _TaskRuntime(uid=task["uid"], kind="function")

    fut = asyncio.get_event_loop().create_future()
    fut.set_result(42)

    # Must not raise despite the callback raising on every invocation.
    await backend._on_done(task, fut, "function")

    assert task["return_value"] == 42
    assert task["uid"] not in backend.tasks


# ---------------------------------------------------------------------------
# Data dependency hooks — documented no-ops
# ---------------------------------------------------------------------------


def test_dask_link_data_deps_are_safe_noops():
    """link_explicit_data_deps / link_implicit_data_deps are documented no-ops."""
    try:
        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()

    src = ComputeTask(function=lambda: 1, args=[])
    dst = ComputeTask(function=lambda: 2, args=[])
    src_before, dst_before = dict(src), dict(dst)

    assert (
        backend.link_explicit_data_deps(
            src_task=src, dst_task=dst, file_name="x", file_path="/tmp/x"
        )
        is None
    )
    assert backend.link_implicit_data_deps(src, dst) is None

    assert dict(src) == src_before
    assert dict(dst) == dst_before
    assert backend.tasks == {}


# ---------------------------------------------------------------------------
# Argument handling — no more silent mutation/filtering of the caller's task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_args_kwargs_not_mutated_on_caller_task():
    """submit_tasks must not rewrite the caller's task['args']/task['kwargs']."""
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()
    backend._initialized = True
    backend._client = MagicMock()
    backend._client.status = "running"
    backend._client.submit = MagicMock(return_value=MagicMock())

    task = ComputeTask(function=lambda a, x=None: (a, x), args=(1, 2), kwargs={"x": 1})
    args_before = task["args"]
    kwargs_before = task["kwargs"]

    with patch("asyncio.create_task", side_effect=_create_task_that_closes_coro):
        await backend.submit_tasks([task])

    assert task["args"] == args_before
    assert task["kwargs"] == kwargs_before


# ---------------------------------------------------------------------------
# Resource pre-check — must use a live scheduler snapshot, not the stale cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_resources_satisfiable_uses_scheduler_identity():
    """_check_resources_satisfiable must use scheduler.identity(), not the always- empty
    scheduler_info() cache on an asynchronous client."""
    try:
        from unittest.mock import AsyncMock
        from unittest.mock import MagicMock

        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()
    backend._initialized = True
    backend._client = MagicMock()
    backend._client.scheduler = MagicMock()
    backend._client.scheduler.identity = AsyncMock(
        return_value={"workers": {"w1": {"resources": {"GPU": 2}}}}
    )

    assert await backend._check_resources_satisfiable({"GPU": 1}) is True
    assert await backend._check_resources_satisfiable({"GPU": 4}) is False
    backend._client.scheduler.identity.assert_called_with(n_workers=-1)


# ---------------------------------------------------------------------------
# Dask key collision — proves distinct tasks never silently share a Future
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dask_duplicate_function_args_get_distinct_keys():
    """Two tasks with the same function/args must get distinct Dask submission keys.

    Regression test: `client.submit()` defaults to `pure=True` with no explicit `key`,
    deriving the Dask key from `tokenize(func, kwargs, *args)`. Two RHAPSODY tasks
    calling the same function with the same args used to tokenize to the identical
    key, so the second `submit()` call would silently return the first task's
    `Future` (`distributed.client.Client.submit`: `if key in self.futures: return
    Future(key, self)`) instead of doing independent work. Passing `key=task["uid"]`
    explicitly bypasses the tokenize path entirely.
    """
    try:
        from unittest.mock import MagicMock
        from unittest.mock import patch

        from rhapsody.backends import DaskExecutionBackend
    except ImportError:
        pytest.skip("Dask dependencies not available")

    backend = DaskExecutionBackend()
    backend._initialized = True
    backend._client = MagicMock()
    backend._client.status = "running"

    submitted_keys = []

    def fake_submit(fn, *args, **kwargs):
        submitted_keys.append(kwargs.get("key"))
        return MagicMock()

    backend._client.submit = fake_submit

    def shared_fn(n):
        return n

    tasks = [
        ComputeTask(function=shared_fn, args=(1,)),
        ComputeTask(function=shared_fn, args=(1,)),
    ]

    with patch("asyncio.create_task", side_effect=_create_task_that_closes_coro):
        await backend.submit_tasks(tasks)

    assert len(submitted_keys) == 2
    assert submitted_keys[0] != submitted_keys[1]
    assert submitted_keys[0] == tasks[0]["uid"]
    assert submitted_keys[1] == tasks[1]["uid"]
