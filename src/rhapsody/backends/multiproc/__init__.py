"""Multi-process backend hosting for Rhapsody (rhapsody-partitions).

A ``RemoteBackendProxy`` exposes a child-process-hosted :class:`BaseBackend` to
the driver's :class:`Session` as if it were local.  This lets one application
drive heterogeneous backends (e.g. Dragon + Flux), each in its own OS process
under its own runtime and environment — the precondition for one-partition-per-
backend execution within a single allocation.

See ``rhapsody-partitions.md`` (top of repo) for the full design.
"""

from .proxy import RemoteBackendProxy
from .spawn import spawn_backend_host

__all__ = [
    "RemoteBackendProxy",
    "spawn_backend_host",
]
