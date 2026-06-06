#!/usr/bin/env python3
"""Two Dragon partitions on an 8-node SLURM allocation — placement-test.

Splits an 8-node allocation into two disjoint 4-node partitions via
rhapsody_rm, launches one dragon_v3 child per partition (each Dragon runtime
constrained to its partition's hosts via ``dragon --hostlist``), and submits
one function task per node — each task pinned to a specific node via Dragon's
``Policy(host_name=...)``.  Each task returns ``{partition, expected_host,
actual_host, pid, cpu_affinity, ROCR_VISIBLE_DEVICES, HIP_VISIBLE_DEVICES}``;
the driver prints a summary table at the end.

PRECONDITIONS
=============
- An 8-node interactive SLURM allocation: ``salloc -N 8 -t HH:MM:SS ...``
- An active venv with rhapsody-py, rhapsody-rm, and dragonhpc installed.
- ``dragon`` and ``python`` on PATH (the venv's bin/).
- Running from the head node of the allocation (the usual interactive case).

PLACEMENT MODEL
===============
- Partition selection is done by ``rhapsody_rm`` (SLURM RM auto-detects nodes).
- Process placement is done **solely by Dragon**: each task carries
  ``task_backend_specific_kwargs={"process_template": {"policy":
  Policy(host_name=<target>)}}``.  No ``srun`` wrappers, no network-config —
  Dragon's global auto-detection is used.

RUN
===
    PYTHONPATH=src python examples/06-partitions.py

Notes:
- The two RemoteBackendProxy children are spawned by the driver process; the
  Dragon launcher prefix (``dragon -N 4 --wlm slurm --hostlist <p?-hosts>
  --port ... --overlay-port ... --frontend-port ...``) is constructed by
  ``DragonExecutionBackendV3.build_launch_prefix(partition)`` — no Dragon-CLI
  knowledge at the call site.  The AF_UNIX control socket lives on the
  driver's host (the allocation's head node), where both Dragon head
  processes also run, so cross-node IPC is not needed.
- The partition spec is passed to dragon_v3 via ``resources={"partition":
  spec}``.  The backend honours it (informationally + for ``num_nodes``
  defaulting); the actual node constraint is enforced by the launcher's
  ``--hostlist`` per ``build_launch_prefix``.
"""

from __future__ import annotations

import asyncio
import os
import socket

import rhapsody_rm
from rhapsody import ComputeTask
from rhapsody import Session
from rhapsody.backends.multiproc import RemoteBackendProxy

NODES_PER_PARTITION = 4
PARTITIONS = ("p0", "p1")


def report(partition: str, expected_host: str) -> dict:
    """Task body.  Returns what the worker actually sees at runtime — the
    driver compares ``expected_host`` (the policy target) against
    ``actual_host`` (what the OS reports) to confirm Dragon honored placement.
    """
    return {
        "partition": partition,
        "expected_host": expected_host,
        "actual_host": socket.gethostname(),
        "pid": os.getpid(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "rocr_visible_devices": os.environ.get("ROCR_VISIBLE_DEVICES"),
        "hip_visible_devices": os.environ.get("HIP_VISIBLE_DEVICES"),
    }


def _hostlist_str(nodes) -> str:
    return ",".join(n.name for n in nodes)


async def main() -> None:
    # --- 1. Build the SLURM RM and carve the allocation into 2 partitions -
    cfg = rhapsody_rm.RMConfig(requested_nodes=NODES_PER_PARTITION * len(PARTITIONS))
    rm = rhapsody_rm.ResourceManager.get_instance(name="SLURM", cfg=cfg)
    print(f"RM detected {len(rm.node_list)} nodes:",
          [n.name for n in rm.node_list])

    specs = {
        name: rhapsody_rm.partition_spec(rm, part_id=name, n_nodes=NODES_PER_PARTITION)
        for name in PARTITIONS
    }
    for name, spec in specs.items():
        print(f"partition {name}: {_hostlist_str(spec['nodelist'])} "
              f"(env keys: {sorted(spec['env'])})")

    # --- 2. Spawn one dragon_v3 child per partition ------------------------
    # The proxy derives the launcher prefix (dragon --hostlist ... --port ...)
    # and the child env from the partition spec via the backend class's
    # ``build_launch_prefix`` hook — no Dragon-CLI knowledge at the call site.
    proxies = {}
    for name in PARTITIONS:
        proxies[name] = await RemoteBackendProxy(
            name=name,
            backend="dragon_v3",
            resources={"partition": specs[name]},
        )
        print(f"proxy[{name}] ready")

    try:
        # --- 3. Build pinned tasks, one per node per partition -------------
        # Per-task placement uses Dragon's Policy(host_name=...).  Policy is
        # a plain class — importable on the driver without bootstrapping the
        # Dragon runtime, and cloudpickled across the proxy boundary.
        from dragon.infrastructure.policy import Policy

        async with Session(backends=list(proxies.values())) as session:
            tasks: list[ComputeTask] = []
            for name in PARTITIONS:
                for node in specs[name]["nodelist"]:
                    tasks.append(ComputeTask(
                        function=report,
                        args=(name, node.name),
                        backend=name,
                        task_backend_specific_kwargs={
                            "process_template": {
                                "policy": Policy(host_name=node.name),
                            },
                        },
                    ))

            print(f"submitting {len(tasks)} tasks "
                  f"({NODES_PER_PARTITION} per partition × {len(PARTITIONS)})")
            await session.submit_tasks(tasks)
            await session.wait_tasks(tasks, timeout=300)

        # --- 4. Print results ---------------------------------------------
        print()
        print(f"{'partition':<6} {'expected':<24} {'actual':<24} "
              f"{'pid':<8} {'cores':<22} ROCR / HIP")
        print("-" * 130)
        n_ok = n_mismatch = n_failed = 0
        for t in tasks:
            rv = t.get("return_value")
            if not isinstance(rv, dict):
                print(f"FAILED {t['uid']:<14} state={t['state']!s:<10} "
                      f"exit={t.get('exit_code')} exc={t.get('exception')!r}")
                n_failed += 1
                continue
            aff = rv["cpu_affinity"]
            aff_str = (f"{aff[0]}-{aff[-1]} ({len(aff)})" if aff else "?")
            match = (rv["expected_host"] == rv["actual_host"])
            tag = "OK" if match else "MISMATCH"
            if match:
                n_ok += 1
            else:
                n_mismatch += 1
            gpus = f"{rv['rocr_visible_devices']} / {rv['hip_visible_devices']}"
            print(f"{rv['partition']:<6} {rv['expected_host']:<24} "
                  f"{rv['actual_host']:<24} {rv['pid']:<8} {aff_str:<22} "
                  f"{gpus}  [{tag}]")

        print()
        print(f"summary: {n_ok} OK, {n_mismatch} placement mismatch, {n_failed} failed")
    finally:
        for name, proxy in proxies.items():
            try:
                await proxy.shutdown()
            except Exception as exc:
                print(f"shutdown[{name}] error: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
