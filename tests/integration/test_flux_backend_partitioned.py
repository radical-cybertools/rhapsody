"""Partitioned Flux backend test on a single host.

Exercises the full partition code path of ``FluxExecutionBackend`` —
``resources={"partition": spec}`` → ``_partition_launcher()`` (srun prefix) →
``FluxService`` bootstrap → ssh-reachable ``r_uri`` connection — without a
real cluster:

- A fake ``srun`` shim is prepended to ``PATH``; it strips the srun options
  the backend derives from the partition spec and re-launches the wrapped
  ``flux start`` with ``--test-size=N`` (N from ``--nodes=N``), giving a
  Flux instance with N brokers, all on localhost.
- The partition's node names are all the local hostname, so the ssh
  ``r_uri`` path (``ssh://<host>/...``) resolves to this machine.  The test
  skips when passwordless ssh-to-self is unavailable.
"""

import asyncio
import os
import socket
import stat
import subprocess

import pytest

_FAKE_SRUN = """\
#!/bin/bash
# fake srun for single-host tests: translate
#   srun [--opts...] flux start CMD...
# into
#   flux start --test-size=N CMD...
# where N comes from --nodes=N.
n=1
while [[ $1 == --* ]]; do
    case $1 in
        --nodes=*) n=${1#--nodes=} ;;
    esac
    shift
done
exe=$1
sub=$2
shift 2
exec "$exe" "$sub" --test-size="$n" "$@"
"""


@pytest.fixture
def skip_if_no_ssh_to_self():
    """Skip when passwordless ssh to the local host does not work."""
    host = socket.gethostname()
    try:
        rc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "true"],
            capture_output=True,
            timeout=15,
        ).returncode
    except Exception:
        rc = 1
    if rc != 0:
        pytest.skip(f"passwordless ssh to {host} not available")


class _FakeNode:
    """Duck-typed Node per rhapsody_rm's CONTRACT.md — only ``.name`` is read."""

    def __init__(self, name: str):
        self.name = name


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux", "skip_if_no_ssh_to_self")
async def test_flux_backend_partitioned_local(tmp_path, monkeypatch):
    """4-'node' partition on localhost: r_uri connect, size check, tasks DONE."""
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    # Fake srun on PATH — the backend's derived launcher runs unmodified.
    shim = tmp_path / "srun"
    shim.write_text(_FAKE_SRUN)
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")

    host = socket.gethostname()
    spec = {"nodelist": [_FakeNode(host) for _ in range(4)], "env": {}}

    backend = FluxExecutionBackend(resources={"partition": spec})
    await backend._async_init()
    try:
        # The partition launch must connect through the ssh-reachable URI.
        assert backend._uri.startswith("ssh://"), backend._uri

        # The instance must span the partition: 4 brokers.
        size = int(backend._flux_helper._handle.attr_get("size"))
        assert size == 4

        states: dict[str, str] = {}
        backend.register_callback(lambda t, s: states.__setitem__(t["uid"], s))

        tasks = [
            {"uid": f"part_task_{i}", "executable": "/bin/echo", "arguments": [str(i)]}
            for i in range(8)
        ]
        await backend.submit_tasks(tasks)

        for _ in range(150):
            done = [s for s in states.values() if s in ("DONE", "FAILED", "CANCELED")]
            if len(done) == len(tasks):
                break
            await asyncio.sleep(0.1)

        assert len(states) == len(tasks), f"missing callbacks: {states}"
        assert all(s == "DONE" for s in states.values()), states
    finally:
        await backend.shutdown()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.usefixtures("skip_if_no_flux", "skip_if_no_ssh_to_self")
async def test_flux_backend_two_partitions_local(tmp_path, monkeypatch):
    """Two co-resident 2-'node' partitions, each its own Flux instance.

    The multi-partition topology of examples/07: two partition-launched
    Flux instances live side by side on the same host, each constrained
    to its partition size, with tasks flowing through both concurrently.
    """
    from rhapsody.backends.execution.flux.flux_backend import FluxExecutionBackend

    shim = tmp_path / "srun"
    shim.write_text(_FAKE_SRUN)
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")

    host = socket.gethostname()
    specs = {
        name: {"nodelist": [_FakeNode(host) for _ in range(2)], "env": {}} for name in ("p0", "p1")
    }

    backends = {
        name: FluxExecutionBackend(name=name, resources={"partition": spec})
        for name, spec in specs.items()
    }
    await asyncio.gather(*(b._async_init() for b in backends.values()))
    try:
        uris = {name: b._uri for name, b in backends.items()}
        for name, uri in uris.items():
            assert uri.startswith("ssh://"), (name, uri)
        # Two distinct instances, not one shared one.
        assert uris["p0"] != uris["p1"]

        for name, backend in backends.items():
            size = int(backend._flux_helper._handle.attr_get("size"))
            assert size == 2, (name, size)

        states: dict[str, str] = {}
        for backend in backends.values():
            backend.register_callback(lambda t, s: states.__setitem__(t["uid"], s))

        tasks = {
            name: [
                {"uid": f"{name}_task_{i}", "executable": "/bin/echo", "arguments": [name, str(i)]}
                for i in range(4)
            ]
            for name in backends
        }
        await asyncio.gather(*(backends[name].submit_tasks(tasks[name]) for name in backends))

        n_total = sum(len(t) for t in tasks.values())
        for _ in range(150):
            done = [s for s in states.values() if s in ("DONE", "FAILED", "CANCELED")]
            if len(done) == n_total:
                break
            await asyncio.sleep(0.1)

        assert len(states) == n_total, f"missing callbacks: {states}"
        assert all(s == "DONE" for s in states.values()), states
    finally:
        await asyncio.gather(*(b.shutdown() for b in backends.values()))
