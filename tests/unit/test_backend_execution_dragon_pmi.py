"""DragonExecutionBackend: the PMI that MPI jobs (``process_templates``) are launched with.

Batch launches a job with ``PMIBackend.CRAY`` unless ``Batch.options(pmi=...)`` says otherwise,
and only job() tasks use a PMI. These tests check what the backend passes to ``Batch.options()``
and when it warns, with Batch mocked out. No Dragon runtime is needed, but Dragon must be
installed for ``PMIBackend`` and ``ProcessTemplate``.
"""

import uuid
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from rhapsody.backends.execution.dragon import CRAY_MPICH_ENV
from rhapsody.backends.execution.dragon import DragonExecutionBackend

PMIBackend = pytest.importorskip(
    "dragon.infrastructure.facts", reason="Dragon is required for Dragon backend tests"
).PMIBackend


def _make_backend(**kwargs):
    """DragonExecutionBackend on a mocked Batch, with a mock logger to count warnings."""
    mock_batch = MagicMock()
    mock_batch.num_workers = 16
    mock_batch.num_managers = 2
    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        backend = DragonExecutionBackend(**kwargs)
    backend.logger = MagicMock()
    return backend


def _job_task(templates=((4, {}),), **backend_specific):
    """An executable task run as one Batch job of ``templates``' ranks."""
    return {
        "uid": f"task.pmi-{uuid.uuid4().hex[:8]}",
        "executable": "/bin/true",
        "arguments": [],
        "function": None,
        "args": [],
        "kwargs": {},
        "name": "mpi",
        "task_backend_specific_kwargs": {"process_templates": list(templates), **backend_specific},
    }


def _work():
    return 0


def _function_job_task(templates=((1, {}),), **backend_specific):
    """A Python function task run as one Batch job of ``templates``' ranks."""
    return {
        **_job_task(templates, **backend_specific),
        "executable": None,
        "function": _work,
        "name": "fn",
    }


@pytest.fixture(autouse=True)
def _no_cray_mpich(monkeypatch):
    """Outside Cray MPICH, as on CI; the tests that need it set it."""
    for name in CRAY_MPICH_ENV:
        monkeypatch.delenv(name, raising=False)


def _options_kwargs(backend):
    return backend.batch.options.call_args.kwargs


def _warnings(backend, text):
    return [c for c in backend.logger.warning.call_args_list if text in c.args[0]]


# ============================================================================
# Which PMI reaches Batch.options()
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pmi", "expected"),
    [
        ("pmix", PMIBackend.PMIX),
        ("PMIx", PMIBackend.PMIX),
        (PMIBackend.PMIX, PMIBackend.PMIX),
        ("cray", PMIBackend.CRAY),
        ("none", None),
    ],
)
async def test_job_gets_task_pmi(pmi, expected):
    backend = _make_backend()

    await backend.build_task(_job_task(pmi=pmi, timeout=60))

    assert _options_kwargs(backend)["pmi"] is expected
    backend.batch.options.return_value.job.assert_called_once()


@pytest.mark.asyncio
async def test_job_falls_back_to_backend_pmi():
    backend = _make_backend(pmi="pmix")

    await backend.build_task(_job_task(timeout=60))

    assert _options_kwargs(backend)["pmi"] is PMIBackend.PMIX


@pytest.mark.asyncio
async def test_create_passes_backend_pmi():
    mock_batch = MagicMock()
    with patch("rhapsody.backends.execution.dragon.Batch", return_value=mock_batch):
        backend = await DragonExecutionBackend.create(pmi="pmix")

    await backend.build_task(_job_task(timeout=60))

    assert _options_kwargs(backend)["pmi"] is PMIBackend.PMIX


@pytest.mark.asyncio
async def test_task_pmi_takes_precedence_over_backend_pmi():
    backend = _make_backend(pmi="cray")

    await backend.build_task(_job_task(pmi="pmix", timeout=60))

    assert _options_kwargs(backend)["pmi"] is PMIBackend.PMIX


@pytest.mark.asyncio
async def test_job_without_pmi_keeps_batch_default():
    backend = _make_backend()

    await backend.build_task(_job_task(timeout=60))

    assert "pmi" not in _options_kwargs(backend)
    backend.batch.options.return_value.job.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task", "submit"),
    [
        (
            {"function": lambda: None, "task_backend_specific_kwargs": {"pmi": "pmix"}},
            "function",
        ),
        (
            {
                "function": lambda: None,
                "task_backend_specific_kwargs": {"process_template": {}, "pmi": "pmix"},
            },
            "process",
        ),
        (
            {"executable": "/bin/true", "task_backend_specific_kwargs": {"pmi": "pmix"}},
            "process",
        ),
    ],
    ids=["native-function", "process-template", "executable"],
)
async def test_function_and_process_tasks_get_no_pmi(task, submit):
    backend = _make_backend(pmi="pmix")
    task = {"uid": f"task.pmi-{uuid.uuid4().hex[:8]}", "name": "single", **task}

    await backend.build_task(task)

    assert "pmi" not in _options_kwargs(backend)
    getattr(backend.batch.options.return_value, submit).assert_called_once()
    backend.logger.warning.assert_not_called()


def test_unknown_backend_pmi_raises_before_batch_starts():
    with (
        patch("rhapsody.backends.execution.dragon.Batch") as mock_batch_cls,
        pytest.raises(ValueError, match="Unknown pmi 'pmi2'"),
    ):
        DragonExecutionBackend(pmi="pmi2")

    mock_batch_cls.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_task_pmi_raises():
    backend = _make_backend(pmi="pmix")

    with pytest.raises(ValueError, match="Unknown pmi 'mpich'"):
        await backend.build_task(_job_task(pmi="mpich", timeout=60))

    backend.batch.options.assert_not_called()


# ============================================================================
# Warnings for multi-rank jobs; none is refused
# ============================================================================


@pytest.mark.asyncio
async def test_multi_rank_job_without_pmi_warns_once():
    backend = _make_backend()

    await backend.build_task(_job_task(timeout=60))
    await backend.build_task(_job_task(timeout=60))

    assert len(_warnings(backend, "no PMI")) == 1
    assert backend.batch.options.return_value.job.call_count == 2


@pytest.mark.asyncio
async def test_multi_rank_job_without_timeout_warns_once():
    backend = _make_backend(pmi="pmix")

    await backend.build_task(_job_task())
    await backend.build_task(_job_task())

    assert len(_warnings(backend, "no timeout")) == 1
    assert backend.batch.options.return_value.job.call_count == 2


@pytest.mark.asyncio
async def test_one_rank_function_job_without_pmi_warns():
    """Batch's default PMI reaches a one-rank job too, and a Python function can crash under it."""
    backend = _make_backend()

    await backend.build_task(_function_job_task())

    assert len(_warnings(backend, "has 1 rank and no PMI")) == 1
    assert _warnings(backend, "no timeout") == []
    assert "pmi" not in _options_kwargs(backend)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", CRAY_MPICH_ENV)
async def test_cray_mpich_environment_keeps_the_default_without_a_pmi_warning(monkeypatch, name):
    monkeypatch.setenv(name, "8.1.30")
    backend = _make_backend()

    await backend.build_task(_job_task())
    await backend.build_task(_function_job_task())

    assert _warnings(backend, "no PMI") == []
    assert len(_warnings(backend, "no timeout")) == 1
    assert "pmi" not in _options_kwargs(backend)
    debug = [c.args[0] for c in backend.logger.debug.call_args_list]
    assert sum("Cray MPICH environment detected" in m for m in debug) == 1


@pytest.mark.asyncio
async def test_ranks_are_counted_across_templates():
    backend = _make_backend()

    await backend.build_task(_job_task(templates=[(1, {}), (1, {})]))

    assert len(_warnings(backend, "has 2 ranks and no PMI")) == 1
    assert len(_warnings(backend, "has 2 ranks and no timeout")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend_pmi", "task"),
    [
        (None, _job_task(templates=[(1, {})])),
        ("pmix", _job_task(timeout=60)),
        (None, _job_task(pmi="cray", timeout=60)),
    ],
    ids=["single-rank", "backend-pmi", "task-pmi"],
)
async def test_job_with_pmi_and_timeout_or_one_rank_does_not_warn(backend_pmi, task):
    backend = _make_backend(pmi=backend_pmi)

    await backend.build_task(task)

    backend.logger.warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend_pmi", "task"),
    [
        (None, _job_task(pmi="none")),
        ("none", _job_task()),
        ("pmix", _job_task(pmi="none")),
    ],
    ids=["task-none", "backend-none", "task-none-over-backend-pmix"],
)
async def test_job_with_pmi_none_needs_no_timeout(backend_pmi, task):
    """A job with pmi="none" is non-MPI: no rank waits in a collective for a dead one."""
    backend = _make_backend(pmi=backend_pmi)

    await backend.build_task(task)

    assert _options_kwargs(backend)["pmi"] is None
    backend.logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_task_pmi_none_does_not_use_up_the_timeout_warning():
    backend = _make_backend(pmi="pmix")

    await backend.build_task(_job_task(pmi="none"))
    await backend.build_task(_job_task())

    assert len(_warnings(backend, "no timeout")) == 1
