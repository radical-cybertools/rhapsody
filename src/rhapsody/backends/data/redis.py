"""RedisDataBackend: launches and owns the lifecycle of one or more
independent `redis-server` instances."""

from __future__ import annotations

import asyncio
import dataclasses
import shlex
import shutil
import socket
import time
from typing import Mapping
from typing import Sequence

from rhapsody.backends.data.base import DataBackend
from rhapsody.backends.data.base import DataBackendStartupError
from rhapsody.backends.data.base import Endpoint

_PING = b"*1\r\n$4\r\nPING\r\n"


@dataclasses.dataclass(frozen=True)
class RedisEndpoint(Endpoint):
    """Connection information for one independent Redis node.

    `serialize()` returns a `host:port` string. `RedisEndpoint` never
    constructs a client itself -- build whichever client you need directly,
    e.g. `redis.Redis(host=endpoint.host, port=endpoint.port)`.
    """

    host: str
    port: int

    def serialize(self) -> str:
        return f"{self.host}:{self.port}"


class RedisDataBackend(DataBackend):
    """A DataBackend backed by N independent per-node `redis-server` instances.

    This models independent, unrelated keyspaces -- not a Redis Cluster.
    `backend.endpoints` is a list with one `RedisEndpoint` per host in
    `hosts`.

    `RedisDataBackend()` with no arguments spawns a single local
    `redis-server` on an auto-picked free port -- it works out of the box
    locally. Multi-node (HPC) usage requires an explicit `cmd`
    launch-command template (e.g.
    `"srun --nodelist={host} redis-server --port {port}"`, formatted per
    host/port and executed directly, never via a shell) plus an explicit
    `port`, since a free port picked on the launching host says nothing
    about availability on a remote target host.

    Launching is done via a plain `asyncio` subprocess for now.
    """

    def __init__(
        self,
        *,
        hosts: Sequence[str] | None = None,
        port: int | None = None,
        cmd: str | None = None,
        redis_server_path: str = "redis-server",
        extra_args: Sequence[str] = (),
        env: Mapping[str, str] | None = None,
        connect_timeout: float = 5.0,
        startup_timeout: float = 30.0,
        poll_interval: float = 0.2,
        shutdown_grace_period: float = 5.0,
    ) -> None:
        """Initialize a RedisDataBackend.

        Args:
            hosts: Hostnames to launch a `redis-server` on, one per entry.
                Defaults to a single `"localhost"` node.
            port: Port to bind on every host. Required when `cmd` is given
                (a free port picked on the launching host says nothing
                about a remote target host); auto-picked per host
                otherwise.
            cmd: Launch-command template, formatted with `host`/`port` and
                executed directly (never via a shell), e.g.
                `"srun --nodelist={host} redis-server --port {port}"`.
            redis_server_path: Path to the `redis-server` executable, used
                when `cmd` is not given.
            extra_args: Extra CLI arguments appended when `cmd` is not
                given.
            env: Environment variables for the launched process(es).
            connect_timeout: Per-attempt timeout for the readiness PING.
            startup_timeout: Overall timeout to wait for each node to
                become ready.
            poll_interval: Delay between readiness poll attempts.
            shutdown_grace_period: Time to wait after SIGTERM before
                escalating to SIGKILL.

        Raises:
            ValueError: If `hosts` is empty, or `cmd` is given without an
                explicit `port`.
        """
        super().__init__()
        self._hosts = list(hosts) if hosts is not None else ["localhost"]
        if not self._hosts:
            raise ValueError("`hosts` must be non-empty if provided")
        if cmd is not None and port is None:
            raise ValueError(
                "`port` must be given explicitly when `cmd` is provided: "
                "RedisDataBackend cannot safely auto-pick a free port on a "
                "remote host from the launching process."
            )
        self._cmd_template = cmd
        self._explicit_port = port
        self._redis_server_path = redis_server_path
        self._extra_args = list(extra_args)
        self._env_overrides = dict(env) if env is not None else None
        self._connect_timeout = connect_timeout
        self._startup_timeout = startup_timeout
        self._poll_interval = poll_interval
        self._shutdown_grace_period = shutdown_grace_period
        self._processes: dict[int, asyncio.subprocess.Process] = {}
        self._planned: list[tuple[str, int]] = []

    def _resolve_port(self) -> int:
        if self._explicit_port is not None:
            return self._explicit_port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("", 0))
            return sock.getsockname()[1]

    def _build_argv(self, host: str, port: int) -> list[str]:
        if self._cmd_template is not None:
            try:
                formatted = self._cmd_template.format(host=host, port=port)
            except (KeyError, IndexError) as exc:
                raise ValueError(
                    f"Invalid `cmd` template {self._cmd_template!r}: {exc}"
                ) from exc
            return shlex.split(formatted)
        return [self._redis_server_path, "--port", str(port), *self._extra_args]

    async def _do_start(self, wait: bool) -> list[Endpoint]:
        if self._cmd_template is None and shutil.which(self._redis_server_path) is None:
            raise DataBackendStartupError(
                f"'{self._redis_server_path}' not found on PATH; install "
                "redis-server, or pass redis_server_path=/cmd= explicitly."
            )

        self._processes = {}
        self._planned = [(host, self._resolve_port()) for host in self._hosts]

        launch_results = await asyncio.gather(
            *(self._launch_one(i, h, p) for i, (h, p) in enumerate(self._planned)),
            return_exceptions=True,
        )
        launch_errors = [r for r in launch_results if isinstance(r, BaseException)]
        if launch_errors:
            await self._terminate_all()
            raise DataBackendStartupError(
                f"Failed to launch {len(launch_errors)}/{len(self._planned)} "
                f"redis node(s): {launch_errors}"
            )

        if not wait:
            return [RedisEndpoint(host=h, port=p) for h, p in self._planned]

        ready_results = await asyncio.gather(
            *(
                self._wait_ready_one(i, h, p)
                for i, (h, p) in enumerate(self._planned)
            ),
            return_exceptions=True,
        )
        ready_errors = [r for r in ready_results if isinstance(r, BaseException)]
        if ready_errors:
            await self._terminate_all()
            raise DataBackendStartupError(
                f"{len(ready_errors)}/{len(self._planned)} redis node(s) "
                f"failed to become ready: {ready_errors}"
            )
        return list(ready_results)

    async def _launch_one(self, index: int, host: str, port: int) -> None:
        argv = self._build_argv(host, port)
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env_overrides,
        )
        self._processes[index] = proc

    async def _wait_ready_one(self, index: int, host: str, port: int) -> RedisEndpoint:
        deadline = time.monotonic() + self._startup_timeout
        proc = self._processes[index]
        while True:
            if proc.returncode is not None:
                tail = await self._read_stderr_tail(proc)
                raise DataBackendStartupError(
                    f"redis-server for node {index} ({host}:{port}) exited "
                    f"early with code {proc.returncode}: {tail}"
                )
            if await self._ping(host, port):
                return RedisEndpoint(host=host, port=port)
            if time.monotonic() >= deadline:
                raise DataBackendStartupError(
                    f"Timed out after {self._startup_timeout}s waiting for "
                    f"redis node {index} ({host}:{port}) to become ready"
                )
            await asyncio.sleep(self._poll_interval)

    async def _read_stderr_tail(self, proc: asyncio.subprocess.Process, n: int = 2000) -> str:
        try:
            assert proc.stderr is not None
            data = await asyncio.wait_for(proc.stderr.read(n), timeout=1.0)
            return data.decode(errors="replace")
        except Exception:
            return "<stderr unavailable>"

    async def _ping(self, host: str, port: int) -> bool:
        def _do_ping() -> bool:
            try:
                with socket.create_connection(
                    (host, port), timeout=self._connect_timeout
                ) as sock:
                    sock.sendall(_PING)
                    reply = sock.recv(64)
                    return reply.startswith(b"+PONG")
            except OSError:
                return False

        return await asyncio.to_thread(_do_ping)

    async def _do_shutdown(self) -> None:
        await self._terminate_all()

    async def _terminate_all(self) -> None:
        await asyncio.gather(
            *(self._terminate_one(p) for p in self._processes.values()),
            return_exceptions=True,
        )
        self._processes = {}

    async def _terminate_one(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=self._shutdown_grace_period)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()

    async def _do_ready(self) -> bool:
        if not self._endpoints:
            return False
        results = await asyncio.gather(
            *(self._ping(ep.host, ep.port) for ep in self._endpoints)
        )
        return all(results)
