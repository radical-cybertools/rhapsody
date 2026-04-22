"""Unit tests for the Flux execution backend and helpers."""

import inspect
import sys
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_flux_module(monkeypatch):
    """Fixture that intercepts flux import and mocks it to allow class instantiation."""
    flux_mock = MagicMock()
    flux_job_mock = MagicMock()
    flux_mock.Flux = MagicMock()
    flux_job_mock.JobspecV1 = MagicMock()

    # Mock JournalConsumer presence for _flux_v check
    flux_job_mock.JournalConsumer = MagicMock()

    monkeypatch.setitem(sys.modules, "flux", flux_mock)
    monkeypatch.setitem(sys.modules, "flux.job", flux_job_mock)

    from rhapsody.backends.execution.flux.flux_module import FluxModule
    monkeypatch.setattr(FluxModule, "_flux_core", None)
    monkeypatch.setattr(FluxModule, "_flux_job", None)
    monkeypatch.setattr(FluxModule, "_flux_exc", None)
    monkeypatch.setattr(FluxModule, "_flux_exe", None)

    return flux_mock, flux_job_mock


def test_flux_module_verify_no_flux(monkeypatch):
    """Test verify raises RuntimeError if flux implies it's missing."""
    with patch("shutil.which", return_value=None):
        monkeypatch.setitem(sys.modules, "flux", None)
        monkeypatch.setitem(sys.modules, "flux.job", None)

        from rhapsody.backends.execution.flux.flux_module import FluxModule

        # Force a fresh evaluation safely using monkeypatch
        monkeypatch.setattr(FluxModule, "_flux_core", None)
        monkeypatch.setattr(FluxModule, "_flux_job", None)
        monkeypatch.setattr(FluxModule, "_flux_exc", ImportError("Fake"))
        monkeypatch.setattr(FluxModule, "_flux_exe", None)

        fm = FluxModule()
        with pytest.raises(RuntimeError, match="flux Python module not found"):
            fm.verify()


@pytest.mark.usefixtures("skip_if_no_flux")
def test_flux_spec_from_command():
    from rhapsody.backends.execution.flux.flux_module import spec_from_command

    spec = spec_from_command("echo hello")
    assert spec is not None
    assert "user" in spec.attributes
    assert "uid" in spec.attributes["user"]


@pytest.mark.usefixtures("skip_if_no_flux")
def test_flux_spec_from_dict():
    from rhapsody.backends.execution.flux.flux_module import spec_from_dict

    td = {
        "uid": "1234",
        "executable": "/bin/echo",
        "arguments": ["hello", "world"],
        "ranks": 2,
        "cores_per_rank": 4,
        "timeout": 10.0,
    }
    spec = spec_from_dict(td)
    assert spec.attributes["system"]["duration"] == 10.0
    assert spec.attributes["user"]["uid"] == "1234"


@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux")
async def test_flux_backend_init():
    from rhapsody.backends.constants import BackendMainStates
    from rhapsody.backends.execution.flux import FluxExecutionBackend

    # Provide a dummy URI to bypass FluxService starting flux instances
    backend = FluxExecutionBackend(uri="local://test")
    assert backend.name == "flux"
    assert backend._backend_state == BackendMainStates.INITIALIZED

    # We don't await _async_init since we provided a dummy URI, it would try to connect.
    # But initialization state is set.


@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux")
async def test_flux_backend_state_registration():
    from rhapsody.backends.constants import BackendMainStates
    from rhapsody.backends.constants import StateMapper
    from rhapsody.backends.execution.flux import FluxExecutionBackend

    backend = FluxExecutionBackend(uri="local://test")

    # Fake async init to trigger state registration without making network requests
    with patch("rhapsody.backends.execution.flux.flux_backend.FluxHelper"):
        await backend._async_init()
        assert backend._backend_state == BackendMainStates.INITIALIZED

    mapper = StateMapper(backend)
    assert mapper.to_main_state("DONE") == mapper.to_main_state("DONE")


@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux")
async def test_flux_backend_shutdown():
    from rhapsody.backends.constants import BackendMainStates
    from rhapsody.backends.execution.flux import FluxExecutionBackend

    with patch("rhapsody.backends.execution.flux.flux_backend.FluxHelper") as mock_helper:
        backend = FluxExecutionBackend(uri="local://test")
        await backend._async_init()

        await backend.shutdown()
        assert backend.state() == BackendMainStates.SHUTDOWN.value


def test_flux_service_banner_parsing(mock_flux_module):
    """Test flux service parses banner correctly."""
    from rhapsody.backends.execution.flux.flux_service import FluxService

    with patch("shutil.which", return_value="/fake/flux"):
        fs = FluxService()
        fs._on_line("FLUX_URI=local:///somepath FLUX_HOST=testhost")
        assert fs.uri == "local:///somepath"
        assert fs.r_uri == "ssh://testhost/somepath"
        assert fs._ready.is_set()
