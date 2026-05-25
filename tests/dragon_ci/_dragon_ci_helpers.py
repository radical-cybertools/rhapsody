"""Top-level helpers used by tests.

Must live in a regular importable module (not in a ``test_*.py`` file) so
Dragon's workers can re-import it via cloudpickle's by-reference path. See
the conftest module docstring for the full story.
"""

import asyncio
import os
import signal
import sys
import time

# --- Batch.function targets ----------------------------------------------


def add(a, b):
    return a + b


def kwfn(x, *, mult=1):
    return x * mult


def slow_double(x, delay):
    time.sleep(delay)
    return x * 2


def print_and_return(value, msg):
    print(msg)
    print("err msg", file=sys.stderr)
    return value


def raise_value_error(message):
    raise ValueError(message)


# --- async dispatch (test_async_in_batch.py) -----------------------------


async def async_double(x):
    await asyncio.sleep(0)
    return x * 2


def async_run_shim(*args, **kwargs):
    """Mirror Rhapsody's wrapper in dragon.py V3."""
    return asyncio.run(async_double(*args, **kwargs))


# --- ProcessGroup workers (test_process_group.py, test_queue_event.py) ---


def pg_worker(queue, shutdown_event):
    """Push one identity message, then loop until shutdown_event is set."""
    queue.put({"pid": os.getpid(), "host": os.uname().nodename}, timeout=10.0)
    while not shutdown_event.wait(timeout=0.05):
        pass


# --- worker failure-mode probes (test_worker_failure_modes.py) -----------


def fn_assert_false(_x):
    assert False, "intentional dragon_ci probe"


def fn_sys_exit_one(_x):
    sys.exit(1)


def fn_kill_self(_x):
    """Abnormal worker termination — no chance for Dragon's exit-detector."""
    os.kill(os.getpid(), signal.SIGKILL)
