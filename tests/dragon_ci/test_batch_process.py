"""Contract: ``Batch.process()`` mode and ``ProcessTemplate`` plumbing.

Rhapsody V3 launches subprocess tasks via::

    batch.process(ProcessTemplate(target, args=(...), cwd=..., policy=...))

Key contract pinned by these tests: ``task.get()`` returns the exit code, and **a
bare ``ProcessTemplate`` (no ``stdout=Popen.PIPE``) does NOT capture child
stdout/stderr — ``task.get_stdout()``/``get_stderr()`` come back empty.**
Rhapsody works around this by wrapping the command in a shell script that
redirects to files (see the V3 ``capture_stdio`` code path).
"""

from __future__ import annotations

import shlex

import pytest
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch


def test_process_get_returns_zero_exit_code(batch: Batch):
    """Rhapsody maps ``get()`` of a process task to its exit code."""
    task = batch.process(ProcessTemplate("/bin/true", args=()))
    assert task.get(timeout=60.0) == 0


def test_process_default_stdio_is_empty(batch: Batch):
    """Without ``stdout=Popen.PIPE``, ``get_stdout()``/``get_stderr()`` are empty.

    If Dragon ever starts capturing by default, Rhapsody's ``capture_stdio``
    workaround can be retired — this test will fail then.
    """
    task = batch.process(ProcessTemplate("/bin/echo", args=("hello-process",)))
    assert task.get(timeout=60.0) == 0
    stdout = task.get_stdout(block=False) or ""
    stderr = task.get_stderr(block=False) or ""
    assert stdout == "" and stderr == "", (
        f"Dragon may now capture stdio by default: stdout={stdout!r} stderr={stderr!r}"
    )


def test_process_template_args_none_raises_typeerror(batch: Batch):
    """``ProcessTemplate(...).args`` defaults to None, which makes ``Batch.process`` raise
    ``TypeError: 'NoneType' object is not iterable``.

    Pass ``args=()`` to avoid it.
    """
    with pytest.raises(TypeError, match="NoneType"):
        batch.process(ProcessTemplate("/bin/true"))


def test_process_non_zero_exit_is_visible(batch: Batch):
    """``/bin/false`` (exit 1): ``get()`` returns the exit code, it does not raise."""
    task = batch.process(ProcessTemplate("/bin/false", args=()))
    assert task.get(timeout=60.0) == 1


def test_process_capture_stdio_via_shell_redirect(batch: Batch, tmp_path):
    """Pins Rhapsody's ``capture_stdio`` workaround.

    A wrapper bash script redirects its own stdout/stderr to files, then runs the real command.
    Dragon launches the bash; the files end up with the captured output regardless of Dragon's PIPE
    behaviour.
    """
    stdout_path = tmp_path / "task.stdout"
    script_path = tmp_path / "task.sh"
    script_path.write_text(
        f"#!/usr/bin/bash\n/bin/echo 'stdout-from-wrapped' 1>{shlex.quote(str(stdout_path))}\n"
    )
    script_path.chmod(0o755)

    task = batch.process(ProcessTemplate("/bin/bash", args=(str(script_path),)))
    task.get(timeout=60.0)

    assert stdout_path.exists(), "wrapper script did not produce stdout file"
    assert "stdout-from-wrapped" in stdout_path.read_text()
