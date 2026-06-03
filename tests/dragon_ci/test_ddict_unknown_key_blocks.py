"""Documented Dragon bug — **test disabled**.

A DDict built with ``wait_for_keys=True`` (the setting Batch's results-DDict
uses) **blocks indefinitely** when a never-written key is read, instead of
raising ``KeyError``. Rhapsody's V3 monitor loop has a
``try/except KeyError: continue`` polling pattern that depends on the
expected KeyError-on-missing contract — when that contract isn't honored,
the monitor loop deadlocks on the first never-arriving result.

This is the silent-hang failure mode that prompted the entire test suite.

The test below is **skipped** because letting it hit ``pytest.mark.timeout``
produces a hard failure that doesn't compose with ``pytest.mark.xfail``
(pytest-timeout raises a ``BaseException`` subclass that xfail does not
catch). To check whether Dragon has fixed the bug, remove the
``pytest.mark.skip`` decorator and run this file under
``dragon python -m pytest`` — the test will either return cleanly (Dragon
fixed it, re-enable permanently) or pytest-timeout will fire (still broken).
"""

import warnings

import pytest
from dragon.data.ddict.ddict import DDict

warnings.warn(
    "test_ddict_get_unknown_key_blocks_forever is DISABLED — Dragon bug "
    "(DDict[unknown_key] blocks instead of raising KeyError when "
    "wait_for_keys=True) is not actively checked. Re-enable to retest.",
    UserWarning,
    stacklevel=2,
)


@pytest.mark.skip(
    reason="DISABLED — Dragon bug: DDict[unknown_key] blocks instead of raising "
           "KeyError when wait_for_keys=True. Re-enable to check if fixed."
)
@pytest.mark.timeout(10, method="thread")
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
