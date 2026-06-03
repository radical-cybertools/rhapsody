"""Contract: ``Batch.job()`` — multi-rank / MPI launch mode.

Rhapsody V3 builds jobs as a list of ``(nranks, ProcessTemplate)`` tuples.
Wire-only checks always run; actual launches are guarded by ``requires_mpi``
(opt-in via ``DRAGON_CI_HAS_PMI=1``) because a failing PMIx init aborts
the whole Dragon runtime, not just the test.
"""

from __future__ import annotations

import inspect

import pytest
from dragon.infrastructure.facts import PMIBackend
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch
from dragon.workflows.batch.batch import Job


@pytest.mark.parametrize("kwarg", ["process_templates", "name", "timeout", "pmi"])
def test_batch_job_kwarg(kwarg):
    assert kwarg in inspect.signature(Batch.job).parameters, (
        f"Batch.job({kwarg}=) removed — Rhapsody depends on it"
    )


def test_pmi_backend_has_pmix():
    """Rhapsody passes ``PMIBackend.PMIX`` as the cross-vendor portable backend."""
    assert "PMIX" in {m.name for m in PMIBackend}


@pytest.mark.requires_mpi
@pytest.mark.parametrize("nranks", [1, 2])
def test_batch_job_launch(batch: Batch, nranks):
    """A real PMIx launch must produce a ``Job`` handle and complete cleanly.

    Skips on hosts where PMIx is unavailable (documented failure mode is
    ``RuntimeError: Unable to initialize PMIx server`` from
    ``DragonPMIxJob.__init__``).
    """
    try:
        job = batch.job(
            [(nranks, ProcessTemplate("/bin/true", args=()))],
            name=f"dragon-ci-{nranks}rank", pmi=PMIBackend.PMIX,
        )
        job.get(timeout=120.0)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PMIX launch unavailable on this host: {exc!r}")

    assert isinstance(job, Job)
    result, tb, raised, _stdout, _stderr = batch.results_ddict[job.uid]
    assert raised is False, f"{nranks}-rank job failed: result={result!r} tb={tb!r}"
