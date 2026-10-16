"""Contract: ``dragon.telemetry.collector`` + ``AccVendor``.

Rhapsody's ``DragonTelemetryAdapter`` imports these helpers directly to
collect per-node GPU metrics inside a Dragon ProcessGroup worker.
"""

from __future__ import annotations

import inspect

import pytest
from dragon.infrastructure.gpu_desc import AccVendor
from dragon.infrastructure.gpu_desc import find_accelerators
from dragon.telemetry.collector import get_amd_metrics
from dragon.telemetry.collector import get_intel_metrics
from dragon.telemetry.collector import get_nvidia_metrics
from dragon.telemetry.collector import identify_gpu

# All tests here need dragon.telemetry.collector — auto-skip if unavailable.
pytestmark = pytest.mark.requires_telemetry_collector


@pytest.mark.parametrize(
    "fn",
    [identify_gpu, get_amd_metrics, get_intel_metrics, get_nvidia_metrics, find_accelerators],
)
def test_collector_symbol_callable(fn):
    assert callable(fn), f"{fn} no longer callable"


@pytest.mark.parametrize("name", ["NVIDIA", "AMD", "INTEL"])
def test_acc_vendor_member(name):
    assert name in {m.name for m in AccVendor}, f"AccVendor.{name} removed"


def test_identify_gpu_signature_and_return_shape():
    assert len(inspect.signature(identify_gpu).parameters) == 0, (
        "identify_gpu() now takes arguments"
    )
    vendor, _count = identify_gpu()
    if vendor is not None:
        assert vendor in list(AccVendor), f"unknown vendor: {vendor!r}"


def test_dragon_telemetry_module_importable():
    """The bare ``dragon.telemetry`` import is used as the adapter gate."""
    import dragon.telemetry  # noqa: F401


@pytest.mark.requires_gpu
def test_get_nvidia_metrics_returns_metric_value_dicts():
    """Telemetry adapter expects ``[{metric: ..., value: ...}, ...]``."""
    vendor, _count = identify_gpu()
    if vendor != AccVendor.NVIDIA:
        pytest.skip(f"NVIDIA GPU expected, got {vendor!r}")

    metrics = get_nvidia_metrics(0, telemetry_level=3)
    assert metrics, "get_nvidia_metrics returned empty for present GPU"
    for m in metrics:
        assert isinstance(m, dict)
        assert "metric" in m and "value" in m, f"metric dict shape changed: {m!r}"
