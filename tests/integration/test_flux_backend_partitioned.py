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
