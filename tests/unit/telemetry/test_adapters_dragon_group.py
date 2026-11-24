"""The Dragon adapter's sampler group, against fake Dragon modules: no Dragon runtime needed.

Dragon 0.15's ProcessGroup client, unless the group ignores errors on exit, watches the group's
exception queue in a thread whose poll returns at once: it spins a whole core for as long as the
group runs. The adapter's group of resource samplers runs for the whole session, so every process
with RHAPSODY's telemetry on under Dragon carried that spinning thread.
"""

from __future__ import annotations

import asyncio
import queue
import sys
import types

from rhapsody.telemetry.adapters.dragon import DragonTelemetryAdapter


class _Queue:
    def __init__(self, maxsize=0):
        self._q = queue.Queue()

    def get(self, timeout=None):
        return self._q.get(timeout=timeout)

    def put(self, item, timeout=None):
        self._q.put(item)

    def destroy(self):
        pass


class _Event:
    def set(self):
        pass

    def destroy(self):
        pass


def _fake_dragon(monkeypatch, groups):
    class ProcessGroup:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            groups.append(self)

        def add_process(self, nproc, template):
            pass

        def init(self):
            pass

        def start(self):
            pass

        def join(self, timeout=None):
            pass

        def close(self):
            pass

    class System:
        def hostname_policies(self):
            return [object()]

    modules = {
        "dragon": types.ModuleType("dragon"),
        "dragon.telemetry": types.ModuleType("dragon.telemetry"),
        "dragon.native": types.ModuleType("dragon.native"),
        "dragon.native.machine": types.ModuleType("dragon.native.machine"),
        "dragon.native.event": types.ModuleType("dragon.native.event"),
        "dragon.native.queue": types.ModuleType("dragon.native.queue"),
        "dragon.native.process": types.ModuleType("dragon.native.process"),
        "dragon.native.process_group": types.ModuleType("dragon.native.process_group"),
    }
    modules["dragon.native.machine"].System = System
    modules["dragon.native.event"].Event = _Event
    modules["dragon.native.queue"].Queue = _Queue
    modules["dragon.native.process"].ProcessTemplate = lambda **kwargs: kwargs
    modules["dragon.native.process_group"].ProcessGroup = ProcessGroup
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


async def test_the_samplers_group_starts_no_exception_monitor(monkeypatch):
    groups = []
    _fake_dragon(monkeypatch, groups)
    adapter = DragonTelemetryAdapter(session_id="s", backend_name="dragon_native", interval=60.0)
    adapter.start()
    try:
        for _ in range(200):
            if groups and adapter._queue is not None:
                break
            await asyncio.sleep(0.01)
        assert len(groups) == 1
    finally:
        adapter.stop()
    assert groups[0].kwargs.get("restart") is False
    assert groups[0].kwargs.get("ignore_error_on_exit") is True
