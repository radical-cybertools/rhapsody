"""Contract: ``dragon.data.ddict.ddict.DDict`` core surface.

Rhapsody V3 reads ``batch.results_ddict``; downstream users construct
DDicts directly via the kwargs and methods checked below.
"""

from __future__ import annotations

import inspect

import pytest
from dragon.data.ddict.ddict import DDict

_DDICT_KWARGS = (
    "managers_per_node", "n_nodes", "total_mem",
    "working_set_size", "wait_for_keys", "policy", "timeout",
)
_DDICT_METHODS = ("pput", "bput", "clear", "destroy", "detach", "keys", "items", "freeze")


@pytest.mark.parametrize("kwarg", _DDICT_KWARGS)
def test_ddict_ctor_kwarg(kwarg):
    assert kwarg in inspect.signature(DDict.__init__).parameters, (
        f"DDict.__init__ no longer accepts {kwarg!r}"
    )


@pytest.mark.parametrize("method", _DDICT_METHODS)
def test_ddict_method_present(method):
    assert callable(getattr(DDict, method, None)), f"DDict.{method}() removed"


@pytest.fixture(scope="module")
def _shared_ddict():
    """One DDict built once per module — tear down on exit."""
    d = DDict(n_nodes=1, managers_per_node=1, total_mem=4 * 1024 * 1024,
              wait_for_keys=False, working_set_size=1)
    try:
        yield d
    finally:
        d.destroy()


@pytest.fixture
def ddict(_shared_ddict):
    """Per-test view on the shared DDict, cleared each call."""
    _shared_ddict.clear()
    return _shared_ddict


def test_ddict_setitem_getitem_contains(ddict):
    ddict["a"] = 1
    assert "a" in ddict and ddict["a"] == 1


def test_ddict_pput(ddict):
    ddict.pput("k", {"nested": [1, 2, 3]})
    assert ddict["k"] == {"nested": [1, 2, 3]}


def test_ddict_delitem(ddict):
    ddict["k"] = "v"
    del ddict["k"]
    assert "k" not in ddict


def test_ddict_clear_removes_all(ddict):
    ddict["x"] = 1
    ddict["y"] = 2
    ddict.clear()
    assert "x" not in ddict and "y" not in ddict


def test_ddict_keyerror_on_missing(ddict):
    with pytest.raises(KeyError):
        _ = ddict["missing"]


def test_ddict_instance_has_is_frozen(ddict):
    """``is_frozen`` lives on the instance (not on the class) in Dragon 0.14."""
    assert hasattr(ddict, "is_frozen")


def test_ddict_round_trip_complex_payload(ddict):
    payload = {"list": [1, 2, 3], "tuple": (4, 5), "set": {6, 7},
               "nested": {"a": [(0, "z")]}}
    ddict["complex"] = payload
    assert ddict["complex"] == payload
