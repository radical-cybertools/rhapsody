"""Data-linking capability contract between RHAPSODY backends and AsyncFlow.

Backends declare whether they link data through two class attributes on
BaseBackend. AsyncFlow calls `link_implicit_data_deps` / `link_explicit_data_deps`
only on backends that declare the matching capability, and relies on
`link_explicit_data_deps` returning a descriptor with a "target" key when the
explicit capability is declared.
"""

import importlib

import pytest

from rhapsody.backends.base import BaseBackend

pytestmark = pytest.mark.result_contract

NON_LINKING_BACKENDS = [
    ("rhapsody.backends.execution.concurrent", "ConcurrentExecutionBackend"),
    ("rhapsody.backends.execution.dragon", "DragonExecutionBackend"),
    ("rhapsody.backends.execution.dask_parallel", "DaskExecutionBackend"),
    ("rhapsody.backends.execution.noop", "NoopExecutionBackend"),
    ("rhapsody.backends.execution.orbit", "OrbitExecutionBackend"),
]


def _import_backend_class(module_name, class_name):
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        pytest.skip(f"{module_name} not importable: {e}")
    return getattr(module, class_name)


def test_base_backend_declares_no_data_linking_by_default():
    assert BaseBackend.supports_implicit_data_linking is False
    assert BaseBackend.supports_explicit_data_linking is False


@pytest.mark.parametrize("module_name,class_name", NON_LINKING_BACKENDS)
def test_non_linking_backends_declare_no_data_linking(module_name, class_name):
    cls = _import_backend_class(module_name, class_name)
    assert cls.supports_implicit_data_linking is False
    assert cls.supports_explicit_data_linking is False
