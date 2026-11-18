"""Importing rhapsody leaves the optional AI backends, and dragon.ai.inference, unimported.

Dragon's ``dragon/ai/inference/llm_engine.py`` sets ``DRAGON_PATCH_MP`` to ``""`` when it is
imported. As a side effect of ``import rhapsody``, that stopped every process a program later
started through Dragon's multiprocessing bridge from getting Dragon's start method
(radical-cybertools/rhapsody#159). The vLLM backend now loads on first use.

The import checks run in a fresh interpreter, since other tests may already have imported these
modules. The ``DRAGON_PATCH_MP`` check can only catch a regression where ``dragon.ai.inference``
is installed; elsewhere the module check covers it.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import rhapsody

_AI_MODULES = (
    "rhapsody.backends.ai",
    "rhapsody.backends.ai.config",
    "rhapsody.backends.ai.vllm",
    "dragon.ai.inference",
)

_PROBE = f"""
import json, os, sys
import rhapsody
import rhapsody.backends
print(json.dumps({{
    "imported": [m for m in {_AI_MODULES!r} if m in sys.modules],
    "dragon_patch_mp": os.environ.get("DRAGON_PATCH_MP"),
}}))
"""


@pytest.fixture(scope="module")
def fresh_import():
    """What a fresh ``import rhapsody`` imports, and what it leaves in DRAGON_PATCH_MP."""
    env = {k: v for k, v in os.environ.items() if k != "DRAGON_PATCH_MP"}
    src = str(Path(rhapsody.__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(p for p in (src, env.get("PYTHONPATH")) if p)
    out = subprocess.run(
        [sys.executable, "-c", _PROBE],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_import_rhapsody_does_not_import_ai_backends(fresh_import):
    assert fresh_import["imported"] == []


def test_import_rhapsody_leaves_dragon_patch_mp_unset(fresh_import):
    assert fresh_import["dragon_patch_mp"] is None


def test_vllm_backend_still_importable_from_backends():
    pytest.importorskip("aiohttp")
    from rhapsody.backends import DragonVllmInferenceBackend
    from rhapsody.backends.ai import vllm

    assert DragonVllmInferenceBackend is vllm.DragonVllmInferenceBackend


def test_unknown_backend_attribute_raises_attribute_error():
    import rhapsody.backends

    assert not hasattr(rhapsody.backends, "NoSuchBackend")
    with pytest.raises(AttributeError, match="NoSuchBackend"):
        rhapsody.backends.NoSuchBackend  # noqa: B018
