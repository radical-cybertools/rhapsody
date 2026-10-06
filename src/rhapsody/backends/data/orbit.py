"""ORBIT-hosted data backend: a DataBackend started remotely via an ORBIT endpoint.

`OrbitDataBackend` is the data-infrastructure counterpart of `OrbitExecutionBackend`.
It asks the endpoint behind an `OrbitExecutionBackend` to start a data backend
(`redis`, `dragon`, ...) inside that endpoint's rhapsody session, so the store
lives next to (and, for Dragon, in the same runtime as) the session's tasks.
The returned endpoints are the remote store's serialized descriptors.

Only the lifecycle crosses ORBIT; data never does.  Local data backends are
unaffected by this module.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
from typing import Any

from .base import DataBackend
from .base import DataBackendStateError
from .base import Endpoint


def _get_logger() -> logging.Logger:
    """Get logger for the orbit data backend module.

    This function provides lazy logger evaluation, ensuring the logger is created after the user has
    configured logging, not at module import time.
    """
    return logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class OrbitEndpoint(Endpoint):
    """Connection information for one node of a remotely started data backend.

    `kind` names the remote data backend type (e.g. `redis`, `dragon`);
    `descriptor` is that backend's own `Endpoint.serialize()` output, valid on
    the remote side (e.g. `host:port` for Redis, a DDict descriptor for Dragon).
    """

    kind: str
    descriptor: str

    def serialize(self) -> str:
        return self.descriptor


class OrbitDataBackend(DataBackend):
    """A data backend started on the remote endpoint of an `OrbitExecutionBackend`.

    The data backend joins the execution backend's remote rhapsody session: same
    endpoint, same session, and for Dragon the same runtime as the tasks that
    session runs.  The execution backend is initialized on `start()` if needed.

    Args:
        execution_backend: The `OrbitExecutionBackend` whose remote session hosts
            the data backend.
        kind: Remote data backend type, as known to the endpoint's rhapsody
            (`DataBackendRegistry`), e.g. `"redis"` or `"dragon"`.
        name: Name of this backend, locally (Session registration) and on the
            remote session.  Defaults to `<kind>_data`, which cannot collide with
            the remote session's execution backends (e.g. `dragon`).
        **backend_kwargs: Forwarded to the remote data backend's constructor;
            must be JSON-serializable.
    """

    # Wraps an execution backend; not a store type buildable by name.
    _registrable = False

    def __init__(
        self, execution_backend: Any, kind: str, *, name: str | None = None, **backend_kwargs: Any
    ) -> None:
        super().__init__(name=name or f"{kind}_data")
        self._exec = execution_backend
        self._kind = kind
        self._backend_kwargs = backend_kwargs
        self._started_remote = False

    @property
    def kind(self) -> str:
        return self._kind

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> dict:
        """Run a blocking `RhapsodyClient` data call off the event loop."""
        client = self._exec._rh
        if client is None:
            raise DataBackendStateError("the ORBIT execution backend is shut down")
        return await asyncio.to_thread(getattr(client, method), *args, **kwargs)

    async def _do_start(self, wait: bool) -> list[Endpoint]:
        await self._exec

        try:
            reply = await self._call(
                "data_start", self._kind, name=self.name, **self._backend_kwargs
            )
        except BaseException as e:
            # The remote side may have started the store before the reply got
            # lost (timeout, reset, cancellation): stop it, best effort.  A 4xx
            # is a definite rejection -- e.g. 409 for a name another instance
            # holds, whose store must survive.  A cancellation that overtakes
            # the request can still miss a store that starts later; the remote
            # session close stops that one.
            status = getattr(e, "status_code", None)
            rejected = isinstance(status, int) and 400 <= status < 500
            if not rejected and self._exec._rh is not None:
                with contextlib.suppress(Exception):
                    await self._call("data_shutdown", self.name)
            raise
        self._started_remote = True

        endpoints: list[Endpoint] = [
            OrbitEndpoint(kind=self._kind, descriptor=d) for d in reply["endpoints"]
        ]
        _get_logger().info(
            "Remote data backend %r (%s) ready: %d endpoint(s)",
            self.name,
            self._kind,
            len(endpoints),
        )
        return endpoints

    async def _do_shutdown(self) -> None:
        if not self._started_remote:
            return
        self._started_remote = False
        if self._exec._rh is None:
            # Execution backend closed first (e.g. Session.close order): its
            # remote session is gone, and the store with it.
            _get_logger().debug("Remote data backend %r: session already closed", self.name)
            return
        try:
            await self._call("data_shutdown", self.name)
        except Exception as e:  # noqa: BLE001 - remote session may already be gone
            _get_logger().warning("Remote data backend %r shutdown failed: %s", self.name, e)

    async def _do_ready(self) -> bool:
        if self._exec._rh is None:
            return False
        reply = await self._call("data_endpoints")
        info = reply.get("backends", {}).get(self.name)
        return bool(info) and info.get("state") == "READY"
