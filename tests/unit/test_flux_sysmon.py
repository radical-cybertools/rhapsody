"""Unit tests for flux sysmon capture and sdexec startup (no flux needed)."""

import errno
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from rhapsody.backends.execution.flux import flux_sysmon
from rhapsody.backends.execution.flux.flux_service import systemd_safe_env
from rhapsody.backends.execution.flux.flux_service import user_systemd_available
from rhapsody.backends.multiproc.wire import inline_envelope


@pytest.fixture
def mock_flux_module(monkeypatch):
    flux_mock = MagicMock()
    flux_job_mock = MagicMock()
    flux_job_mock.JournalConsumer = MagicMock()
    monkeypatch.setitem(sys.modules, "flux", flux_mock)
    monkeypatch.setitem(sys.modules, "flux.job", flux_job_mock)

    from rhapsody.backends.execution.flux.flux_module import FluxModule

    monkeypatch.setattr(FluxModule, "_flux_core", None)
    monkeypatch.setattr(FluxModule, "_flux_job", None)
    monkeypatch.setattr(FluxModule, "_flux_exc", None)
    monkeypatch.setattr(FluxModule, "_flux_exe", None)
    with patch("shutil.which", return_value="/fake/flux"):
        yield flux_mock, flux_job_mock


# ---------------------------------------------------------------------------
# flux_sysmon
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("123", 123),
        ("2.5625M", int(2.5625 * 1024**2)),
        ("205.60156M", int(205.60156 * 1024**2)),
        ("2.0390701G", int(2.0390701 * 1024**3)),
        ("1K", 1024),
    ],
)
def test_parse_size(text, expected):
    assert flux_sysmon.parse_size(text) == expected


def test_summarize_sums_ranks():
    entries = [
        (0, "memory.peak=1M"),
        (0, "loadavg-overall=0.5"),
        (1, "memory.peak=3M"),
        (1, "loadavg-overall=1.5"),
    ]
    usage = flux_sysmon.summarize(entries, "job")
    assert usage["memory_peak_bytes"] == 4 * 1024**2
    assert usage["cpu_loadavg"] == 2.0
    assert usage["scope"] == "job"
    assert usage["per_rank"][1] == {"memory_peak_bytes": 3 * 1024**2, "cpu_loadavg": 1.5}


def test_summarize_partial_and_garbage():
    usage = flux_sysmon.summarize([(0, "memory.peak=x"), (0, "loadavg-overall=0.25")], "shell")
    assert usage["memory_peak_bytes"] is None
    assert usage["cpu_loadavg"] == 0.25
    assert flux_sysmon.summarize([(0, "something else")], "job") is None
    assert flux_sysmon.summarize([], "job") is None


def _event(name, **ctx):
    return SimpleNamespace(name=name, context=ctx)


class _Watcher:
    def __init__(self, events, hang=False):
        self._events = list(events)
        self._hang = hang
        self.canceled = False

    def wait_for(self, timeout):
        if self._hang and not self._events:
            raise OSError(errno.ETIMEDOUT, "timeout")
        return self

    def get_event(self):
        return self._events.pop(0) if self._events else None

    def cancel(self):
        self.canceled = True


def test_read_entries_filters_sysmon_logs():
    watcher = _Watcher(
        [
            _event("header"),
            _event("data", stream="stdout", rank="0"),
            _event("log", rank=1, component="sysmon", message="memory.peak=1M"),
            _event("log", rank=0, component="shell", message="unrelated"),
            _event("log", rank=0, component="sysmon", message="loadavg-overall=1.0"),
        ]
    )
    flux_job = SimpleNamespace(event_watch_async=lambda h, j, log: watcher)
    entries = flux_sysmon.read_entries(flux_job, None, 1, timeout=2.0)
    assert entries == [(1, "memory.peak=1M"), (0, "loadavg-overall=1.0")]


def test_read_entries_returns_partial_on_timeout():
    watcher = _Watcher(
        [_event("log", rank=0, component="sysmon", message="memory.peak=1M")], hang=True
    )
    flux_job = SimpleNamespace(event_watch_async=lambda h, j, log: watcher)
    assert flux_sysmon.read_entries(flux_job, None, 1, timeout=0.5) == [(0, "memory.peak=1M")]
    assert watcher.canceled


# ---------------------------------------------------------------------------
# sdexec preconditions
# ---------------------------------------------------------------------------


def test_systemd_safe_env():
    env = {
        "PATH": "/bin",
        "TABS": "a\tb\nc",
        "BASH_FUNC_module%%": "() { :; }",
        "PS1": "\x1b[1;32m$ \x1b[0m",
        "1BAD": "x",
        "UNDECODABLE": "a\udcff",
    }
    assert systemd_safe_env(env) == {"PATH": "/bin", "TABS": "a\tb\nc"}


def test_user_systemd_available(tmp_path, monkeypatch):
    bus = tmp_path / "bus"
    monkeypatch.delenv("DBUS_SESSION_BUS_ADDRESS", raising=False)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    assert not user_systemd_available()
    bus.touch()
    assert user_systemd_available()
    monkeypatch.setenv("DBUS_SESSION_BUS_ADDRESS", f"unix:path={tmp_path}/other")
    assert not user_systemd_available()
    monkeypatch.setenv("DBUS_SESSION_BUS_ADDRESS", f"unix:path={bus}")
    assert user_systemd_available()


# ---------------------------------------------------------------------------
# FluxService sdexec start / fallback
# ---------------------------------------------------------------------------


def test_service_rejects_bad_sdexec(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    with pytest.raises(ValueError, match="sdexec"):
        FluxService(sdexec="yes")


def test_service_sdexec_command_and_env(mock_flux_module, monkeypatch):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    monkeypatch.setenv("BASH_FUNC_x%%", "() { :; }")
    fs = FluxService(launcher="srun -N2", sdexec=True)
    with patch("subprocess.Popen") as popen:
        popen.return_value.wait.return_value = 0
        fs._launch(True, None)
    cmd = popen.call_args.args[0]
    assert cmd.startswith("srun -N2 /fake/flux start --conf=systemd.enable=true")
    assert "--conf=exec.service=sdexec" in cmd
    assert "flux run -N \\$(flux getattr size) true" in cmd
    assert "BASH_FUNC_x%%" not in popen.call_args.kwargs["env"]
    assert fs.sdexec


def test_service_rexec_command_unchanged(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService()
    with patch("subprocess.Popen") as popen:
        popen.return_value.wait.return_value = 0
        fs._launch(False, None)
    cmd = popen.call_args.args[0]
    assert "--conf" not in cmd and "flux run" not in cmd
    assert popen.call_args.kwargs["env"] is None
    assert not fs.sdexec


def test_service_smoke_failure_marker(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService()
    fs._on_line("RHAPSODY_FLUX_SDEXEC_FAILED")
    assert fs._sdexec_failed and fs._settled.is_set() and not fs.ready()


def test_service_auto_falls_back_to_rexec(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService(sdexec="auto")
    calls = []

    def fake_launch(sdexec, timeout):
        calls.append(sdexec)
        fs._sdexec_failed = sdexec  # the sdexec smoke job fails
        return not sdexec

    with (
        patch.object(fs, "_sdexec_possible", return_value=True),
        patch.object(fs, "_launch", side_effect=fake_launch),
        patch.object(fs, "stop"),
    ):
        assert fs.start(timeout=1.0)
    assert calls == [True, False]


def test_service_auto_skips_sdexec_without_preconditions(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService(sdexec="auto")
    with (
        patch.object(fs, "_sdexec_possible", return_value=False),
        patch.object(fs, "_launch", return_value=True) as launch,
    ):
        assert fs.start(timeout=1.0)
    launch.assert_called_once_with(False, 1.0)


def test_service_required_sdexec_raises(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService(sdexec=True)

    def fake_launch(sdexec, timeout):
        fs._sdexec_failed = True
        return False

    with (
        patch.object(fs, "_launch", side_effect=fake_launch),
        patch.object(fs, "stop"),
        pytest.raises(RuntimeError, match="sdexec"),
    ):
        fs.start(timeout=1.0)


def test_sdexec_possible_requires_conf_option(mock_flux_module, monkeypatch, tmp_path):
    from rhapsody.backends.execution.flux.flux_service import FluxService

    (tmp_path / "bus").touch()
    monkeypatch.delenv("DBUS_SESSION_BUS_ADDRESS", raising=False)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    fs = FluxService(sdexec="auto")
    with patch(
        "subprocess.run", return_value=SimpleNamespace(stdout="  -c, --config-path", stderr="")
    ):
        assert not fs._sdexec_possible()
    with patch("subprocess.run", return_value=SimpleNamespace(stdout="  --conf=VALUE", stderr="")):
        assert fs._sdexec_possible()


# ---------------------------------------------------------------------------
# Backend wiring
# ---------------------------------------------------------------------------


def test_spec_sets_sysmon_option(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_module import spec_from_dict

    spec = spec_from_dict({"executable": "true", "sysmon": True})
    spec.setattr_shell_option.assert_any_call("sysmon", {})
    spec.setattr_shell_option.reset_mock()  # the JobspecV1 mock returns one shared spec
    spec = spec_from_dict({"executable": "true"})
    assert ("sysmon", {}) not in [c.args for c in spec.setattr_shell_option.call_args_list]


def test_backend_sdexec_default_follows_sysmon(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    assert FluxExecutionBackend()._sdexec is False
    assert FluxExecutionBackend(sysmon=True)._sdexec == "auto"
    assert FluxExecutionBackend(sysmon=True, sdexec=False)._sdexec is False


@pytest.mark.parametrize(("service", "scope"), [("sdexec", "job"), ("rexec", "shell")])
def test_backend_sysmon_scope(mock_flux_module, service, scope):
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    flux_mock, _ = mock_flux_module
    b = FluxExecutionBackend(uri="local:///x", sysmon=True)
    flux_mock.Flux.return_value.rpc.return_value.get.return_value = {"exec": {"service": service}}
    b._init_sysmon()
    try:
        assert b._sysmon_scope == scope
    finally:
        b._sysmon_pool.shutdown()


def test_backend_finish_attaches_usage_before_callback(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    flux_mock, _ = mock_flux_module
    b = FluxExecutionBackend(uri="local:///x", sysmon=True)
    flux_mock.Flux.return_value.rpc.return_value.get.return_value = {"exec": {"service": "sdexec"}}
    b._init_sysmon()
    seen = []
    b.register_callback(lambda t, s: seen.append((s, dict(t))))
    b.tasks["t1"] = {"uid": "t1"}
    event = SimpleNamespace(name="finish", context={"status": 0}, jobid=42)
    with patch.object(flux_sysmon, "read_entries", return_value=[(0, "memory.peak=1M")]) as read:
        b._flux_event_cb("t1", event)
        b._sysmon_pool.shutdown(wait=True)
    assert read.call_args.args[2] == 42
    assert seen[0][0] == "DONE"
    assert seen[0][1]["resource_usage"]["memory_peak_bytes"] == 1024**2
    assert seen[0][1]["resource_usage"]["scope"] == "job"


def test_backend_without_sysmon_calls_back_directly(mock_flux_module):
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    b = FluxExecutionBackend(uri="local:///x")
    seen = []
    b.register_callback(lambda t, s: seen.append(s))
    b.tasks["t1"] = {"uid": "t1"}
    b._flux_event_cb("t1", SimpleNamespace(name="finish", context={"status": 256}, jobid=1))
    assert seen == ["FAILED"]
    assert "resource_usage" not in b.tasks["t1"]


# ---------------------------------------------------------------------------
# Proxy envelope
# ---------------------------------------------------------------------------


def test_envelope_carries_resource_usage_only_when_set():
    assert "resource_usage" not in inline_envelope({"state": "DONE"})["payload"]
    usage = {"memory_peak_bytes": 1, "scope": "job"}
    payload = inline_envelope({"state": "DONE", "resource_usage": usage})["payload"]
    assert payload["resource_usage"] == usage


def test_host_passes_backend_kwargs(monkeypatch):
    """CONFIG's ``kwargs`` reach the backend constructor in the child."""
    import rhapsody.backends.multiproc.spawn as spawn

    sent = []
    monkeypatch.setattr(spawn, "send_msg", lambda conn, msg: sent.append(msg))
    monkeypatch.setattr(spawn, "recv_msg", lambda conn: {"op": "READY", "terminal_states": []})
    monkeypatch.setattr(spawn.subprocess, "Popen", MagicMock())
    listener = MagicMock()
    monkeypatch.setattr(spawn, "Listener", MagicMock(return_value=listener))
    spawn.spawn_backend_host("flux", backend_kwargs={"sysmon": True})
    assert sent[0]["kwargs"] == {"sysmon": True}


def test_service_auto_timeout_does_not_fall_back(mock_flux_module):
    """A startup timeout is not an sdexec failure: no rexec relaunch."""
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService(sdexec="auto")
    with (
        patch.object(fs, "_sdexec_possible", return_value=True),
        patch.object(fs, "_launch", return_value=False) as launch,
        patch.object(fs, "stop"),
    ):
        assert not fs.start(timeout=1.0)
    launch.assert_called_once_with(True, 1.0)


def test_service_ignores_output_of_replaced_process(mock_flux_module):
    """A failure marker read from an old process must not affect the relaunch."""
    from rhapsody.backends.execution.flux.flux_service import FluxService

    fs = FluxService()
    fs._proc = object()
    fs._reader([b"RHAPSODY_FLUX_SDEXEC_FAILED\n"], object())
    assert not fs._sdexec_failed
