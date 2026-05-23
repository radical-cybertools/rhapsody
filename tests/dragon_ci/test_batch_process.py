"""Contract: ``Batch.process()`` mode and ``ProcessTemplate`` plumbing.

Rhapsody V3 launches subprocess tasks via::

    batch.process(ProcessTemplate(target, args=(...), cwd=..., policy=...))

Key contract pinned by these tests: **a bare ``ProcessTemplate`` (no
``stdout=Popen.PIPE``) does NOT capture child stdout/stderr into the
``results_ddict`` 5-tuple — those slots come back as empty strings.**
Rhapsody works around this by wrapping the command in a shell script that
redirects to files (see the V3 ``capture_stdio`` code path).
"""

from __future__ import annotations

import pytest

from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch


def test_process_returns_five_tuple(batch: Batch):
    """A trivial ``/bin/echo`` task completes and yields the 5-tuple shape."""
    task = batch.process(ProcessTemplate("/bin/echo", args=("hello-process",)))
    entry = batch.results_ddict[task.uid]
    assert isinstance(entry, tuple) and len(entry) == 5, (
        f"results_ddict shape changed for batch.process: {entry!r}"
    )
    _result, _tb, raised, _stdout, _stderr = entry
    assert raised is False


def test_process_default_stdio_is_empty(batch: Batch):
    """Without ``stdout=Popen.PIPE``, the stdout/stderr slots are empty strings.

    If Dragon ever starts capturing by default, Rhapsody's ``capture_stdio``
    workaround can be retired — this test will fail then.
    """
    task = batch.process(ProcessTemplate("/bin/echo", args=("hello-process",)))
    _r, _tb, _raised, stdout, stderr = batch.results_ddict[task.uid]
    assert stdout == "" and stderr == "", (
        f"Dragon may now capture stdio by default: stdout={stdout!r} stderr={stderr!r}"
    )


def test_process_template_args_none_raises_typeerror(batch: Batch):
    """``ProcessTemplate(...).args`` defaults to None, which makes
    ``Batch.process`` raise ``TypeError: 'NoneType' object is not iterable``.
    Pass ``args=()`` to avoid it."""
    with pytest.raises(TypeError, match="NoneType"):
        batch.process(ProcessTemplate("/bin/true"))


def test_process_non_zero_exit_is_visible(batch: Batch):
    """``/bin/false`` (exit 1) must surface failure through the 5-tuple."""
    task = batch.process(ProcessTemplate("/bin/false", args=()))
    result, tb, raised, _stdout, _stderr = batch.results_ddict[task.uid]
    # Dragon may store the failure as ``raised=True`` or as a non-zero result.
    failure_visible = raised or bool(tb) or result not in (None, 0, True)
    assert failure_visible, (
        "Non-zero exit produced a clean-looking tuple — cannot distinguish failure"
    )


def test_process_capture_stdio_via_shell_redirect(batch: Batch, tmp_path):
    """Pins Rhapsody's ``capture_stdio`` workaround.

    A wrapper bash script redirects its own stdout/stderr to files, then
    runs the real command. Dragon launches the bash; the files end up with
    the captured output regardless of Dragon's PIPE behaviour.
    """
    stdout_path = tmp_path / "task.stdout"
    script_path = tmp_path / "task.sh"
    script_path.write_text(
        f"#!/usr/bin/bash\n"
        f"/bin/echo 'stdout-from-wrapped' 1>{stdout_path}\n"
    )
    script_path.chmod(0o755)

    task = batch.process(ProcessTemplate("/bin/bash", args=(str(script_path),)))
    _ = batch.results_ddict[task.uid]  # wait for completion

    assert stdout_path.exists(), "wrapper script did not produce stdout file"
    assert "stdout-from-wrapped" in stdout_path.read_text()
