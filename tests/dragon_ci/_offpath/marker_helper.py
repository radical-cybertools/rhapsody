"""Helper module deliberately placed in a subdirectory that the conftest
does NOT add to ``PYTHONPATH``. Tests that want to reproduce the runtime
``sys.path.insert`` regression import from here.
"""

def offpath_add(a, b):
    return a + b
