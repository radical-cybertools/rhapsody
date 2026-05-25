"""Documented Dragon bug — kept failing until Dragon fixes it.

A DDict built with ``wait_for_keys=True`` (the setting Batch's results-DDict
uses) **blocks indefinitely** when a never-written key is read, instead of
raising ``KeyError``. Rhapsody's V3 monitor loop has a
``try/except KeyError: continue`` polling pattern that depends on the
expected KeyError-on-missing contract — when that contract isn't honored,
the monitor loop deadlocks on the first never-arriving result.

This is the silent-hang failure mode that prompted the entire test suite.
We pin it via ``pytest.mark.timeout`` so pytest emits a clear failure
report with the exact blocking call in the trace. When Dragon fixes the
bug, ``KeyError`` will fire fast, the ``pytest.raises`` block will pass,
and the test goes green.

Lives in its own file because ``method="thread"`` leaks the test thread
(it's still wedged inside Dragon's C-level channel read) and the leaked
thread holds resources that break any subsequent test in the same pytest
invocation.
"""

import pytest
from dragon.data.ddict.ddict import DDict


@pytest.mark.timeout(5, method="thread")
def test_ddict_get_unknown_key_blocks_forever():
    # wait_for_keys=True requires working_set_size > 1 (Dragon enforces it).
    d = DDict(n_nodes=1, managers_per_node=1, total_mem=4 * 1024 * 1024,
              wait_for_keys=True, working_set_size=2)
    try:
        with pytest.raises(KeyError):
            _ = d["never-written"]
    finally:
        try:
            d.destroy()
        except Exception:
            pass
