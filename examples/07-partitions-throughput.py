#!/usr/bin/env python3
"""Throughput across two partitions of one SLURM allocation — Flux vs Dragon.

Splits a 4-node SLURM allocation into two disjoint 2-node partitions via
``rhapsody_rm`` and runs the same no-op-task throughput measurement on both,
concurrently:

- partition ``p_dragon``: a rhapsody ``dragon`` backend hosted in a child
  process behind ``RemoteBackendProxy``.  The Dragon runtime is constrained
  to the partition's hosts via the backend's ``build_launch_prefix`` hook
  (``dragon -N 2 --wlm slurm --hostlist ...``); tasks are rhapsody
  ``ComputeTask`` no-op functions submitted through a ``Session``.

- partition ``p_flux``: rhapsody has **no flux backend yet**, so this side
  drives Flux directly.  ``srun --overlap ... flux start`` bootstraps a
  2-node Flux instance on the partition's hosts; a small worker script
  (written to the working directory, which therefore must be on a shared
  filesystem) runs inside the instance, submits N ``/bin/true`` jobs via
  ``flux.job.FluxExecutor``, and reports tasks/s on stdout.

Both measurements run concurrently on their disjoint node sets; the driver
prints a two-row comparison table.  The numbers are *indicative*, not a fair
scheduler shoot-out: the flux number is Flux's native scheduler throughput,
the dragon number is rhapsody-over-Dragon-Batch including the proxy IPC hop.

========================================================================
TESTING NOTES — Perlmutter (transient; remove this section before merge)
========================================================================

Repos / stack this example was written against (2026-07-20):

- rhapsody: branch ``feature/partitions-orbit`` (Dragon V3 backend,
  registry name ``dragon_v3``; the 0.14.1 migration was retracted 2026-07-22
  — do NOT use dragonhpc 0.14.1 pre-releases) with the partition consumer
  hooks: ``resources={"partition": spec}`` + ``build_launch_prefix``.
- rhapsody_rm: repo checked out next to rhapsody (``../rhapsody_rm``); the
  partition contract is ``../rhapsody_rm/CONTRACT.md``.  Producer API used
  here: ``RMConfig(requested_nodes=4)``,
  ``ResourceManager.get_instance(name="SLURM", cfg=cfg)``,
  ``partition_spec(rm, part_id=..., n_nodes=2)`` →
  ``{"nodelist": [Node...], "env": {...}}``.  rhapsody only duck-types
  ``Node`` (``.name``, ``.index``, ``.cores``, ``.gpus``) — do not import
  rhapsody_rm inside rhapsody.
- dragonhpc == 0.14.0 (the pinned release; the V3 backend targets its Batch
  API); the ``dragon`` CLI must be on PATH.  The launch prefix built by the backend
  offsets the default ports (7575/6565/6566) per instance so co-resident
  front-ends on the head node don't clash.
- Flux: ``flux`` CLI + its Python bindings must be available *inside the
  srun'd environment* (e.g. ``module load flux`` if NERSC provides one, or
  a spack/conda install).  Smoke test:
  ``srun -N1 --overlap flux start flux resource list``.

Suggested setup on Perlmutter:

    salloc -N 4 -C cpu -q interactive -t 00:30:00 -A <account>
    module load python                      # or your python env
    python -m venv ve && source ve/bin/activate
    pip install dragonhpc==0.14.0 cloudpickle
    # rhapsody + rhapsody_rm from source (no editable installs):
    export PYTHONPATH=$HOME/radical/rhapsody/src:$HOME/radical/rhapsody_rm/src

Run (from the head node of the allocation, cwd on a shared FS):

    python examples/07-partitions-throughput.py [--n-tasks N]

Known constraints / things to verify on the machine:

- The AF_UNIX control socket of RemoteBackendProxy lives on the driver's
  host; driver and the Dragon front-end both run on the allocation's head
  node, so no cross-node IPC is needed for the proxy itself.
- Two concurrent srun steps (Flux bootstrap and Dragon's WLM launch) share
  the allocation on disjoint nodes; ``--overlap`` is passed to the Flux
  step to avoid step-resource contention.
- If Dragon startup fails with port clashes after repeated runs, ports are
  offset per *process*, not globally — rerun from a fresh driver process.
- SLURM partition env changes (SLURM_NODELIST etc.) are merged into the
  Flux srun's environment from ``spec["env"]``.
========================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

import rhapsody_rm

from rhapsody import ComputeTask
from rhapsody import Session
from rhapsody.backends.multiproc import RemoteBackendProxy

NODES_PER_PARTITION = 2
DEFAULT_N_TASKS = 256
WARMUP_TASKS = 8
TIMEOUT = 600  # seconds, per partition measurement

# Worker script executed inside the Flux instance (rank-0 broker).  Written
# to the cwd at runtime so the compute nodes can read it via the shared FS.
_FLUX_WORKER_SRC = """\
import sys
import time

from flux.job import FluxExecutor
from flux.job import JobspecV1

n_tasks = int(sys.argv[1])
warmup = int(sys.argv[2])
spec = JobspecV1.from_command(["/bin/true"])

with FluxExecutor() as ex:
    # Warmup round: absorb scheduler cold-start effects.
    for fut in [ex.submit(spec) for _ in range(warmup)]:
        fut.result()

    t0 = time.perf_counter()
    for fut in [ex.submit(spec) for _ in range(n_tasks)]:
        fut.result()
    secs = time.perf_counter() - t0

print(f"FLUX_RESULT tasks={n_tasks} secs={secs:.3f} rate={n_tasks / secs:.1f}", flush=True)
"""


def no_op():
    pass


def _hostlist(spec: dict) -> str:
    return ",".join(node.name for node in spec["nodelist"])


async def run_flux(spec: dict, n_tasks: int) -> dict:
    """Bootstrap a Flux instance on the partition's nodes, measure tasks/s."""
    n_nodes = len(spec["nodelist"])
    worker = Path.cwd() / f"_flux_throughput_worker_{os.getpid()}.py"
    worker.write_text(_FLUX_WORKER_SRC)

    cmd = [
        "srun",
        "--overlap",
        f"--nodes={n_nodes}",
        f"--ntasks={n_nodes}",
        "--ntasks-per-node=1",
        f"--nodelist={_hostlist(spec)}",
        "flux",
        "start",
        sys.executable,
        str(worker),
        str(n_tasks),
        str(WARMUP_TASKS),
    ]
    env = {**os.environ, **(spec["env"] or {})}
    print(f"p_flux: {' '.join(cmd)}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT)
    finally:
        worker.unlink(missing_ok=True)

    out = stdout.decode()
    if proc.returncode != 0:
        raise RuntimeError(
            f"flux measurement failed (exit {proc.returncode}):\n{out}\n{stderr.decode()}"
        )
    for line in out.splitlines():
        if line.startswith("FLUX_RESULT"):
            fields = dict(kv.split("=") for kv in line.split()[1:])
            return {"tasks": int(fields["tasks"]), "secs": float(fields["secs"])}
    raise RuntimeError(f"no FLUX_RESULT line in flux worker output:\n{out}")


async def run_dragon(spec: dict, n_tasks: int) -> dict:
    """Host a dragon backend on the partition via proxy, measure tasks/s."""
    proxy = await RemoteBackendProxy(
        name="p_dragon",
        backend="dragon_v3",
        resources={"partition": spec},
    )
    print(f"p_dragon: proxy up on {_hostlist(spec)}")
    try:
        async with Session(backends=[proxy]) as session:
            # Warmup round: absorb Batch pipeline cold-start effects.
            warmup = [ComputeTask(function=no_op, backend="p_dragon") for _ in range(WARMUP_TASKS)]
            await session.submit_tasks(warmup)
            await session.wait_tasks(warmup, timeout=TIMEOUT)

            tasks = [ComputeTask(function=no_op, backend="p_dragon") for _ in range(n_tasks)]
            t0 = time.perf_counter()
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks, timeout=TIMEOUT)
            secs = time.perf_counter() - t0

            n_done = sum(1 for t in tasks if t["state"] == "DONE")
            if n_done != n_tasks:
                raise RuntimeError(f"p_dragon: only {n_done}/{n_tasks} tasks DONE")
            return {"tasks": n_tasks, "secs": secs}
    finally:
        await proxy.shutdown()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Throughput across two partitions of one SLURM allocation — Flux vs Dragon."
    )
    parser.add_argument("--n-tasks", type=int, default=DEFAULT_N_TASKS)
    args = parser.parse_args()

    # --- 1. Carve the 4-node allocation into two 2-node partitions --------
    cfg = rhapsody_rm.RMConfig(requested_nodes=2 * NODES_PER_PARTITION)
    rm = rhapsody_rm.ResourceManager.get_instance(name="SLURM", cfg=cfg)
    print(f"RM detected {len(rm.node_list)} nodes:", [n.name for n in rm.node_list])

    specs = {
        name: rhapsody_rm.partition_spec(rm, part_id=name, n_nodes=NODES_PER_PARTITION)
        for name in ("p_flux", "p_dragon")
    }
    for name, spec in specs.items():
        print(f"partition {name}: {_hostlist(spec)} (env keys: {sorted(spec['env'])})")

    # --- 2. Run both measurements concurrently on disjoint nodes ----------
    results = await asyncio.gather(
        run_flux(specs["p_flux"], args.n_tasks),
        run_dragon(specs["p_dragon"], args.n_tasks),
        return_exceptions=True,
    )

    # --- 3. Report --------------------------------------------------------
    print()
    print(f"{'partition':<10} {'runtime':<8} {'tasks':<7} {'secs':<9} tasks/s")
    print("-" * 50)
    failures = []
    for name, runtime, res in (
        ("p_flux", "flux", results[0]),
        ("p_dragon", "dragon", results[1]),
    ):
        if isinstance(res, BaseException):
            print(f"{name:<10} {runtime:<8} FAILED: {res}")
            failures.append((name, res))
        else:
            rate = res["tasks"] / res["secs"]
            print(f"{name:<10} {runtime:<8} {res['tasks']:<7} {res['secs']:<9.3f} {rate:.1f}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
