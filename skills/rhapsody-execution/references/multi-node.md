# Running RHAPSODY on multiple nodes (Dragon)

The procedure below applies to any machine. Only the values in angle brackets are
site-specific: take them from the machine's guide in `$RHAPSODY_REPO/docs/hpc-machines/` when
one exists, otherwise ask the user. Never invent them.

The machine guides are worked examples of exactly these steps. When the target machine has one,
follow it; use this page to understand the choices it makes, or to set up a machine without a
guide.

## Values to collect first

| Value | Typical source | Used in |
|---|---|---|
| Scheduler and allocation command (nodes, time, account, whole vs partial nodes) | User / site docs | Step 1 |
| Python and MPI runtime setup (modules, vendor Python, software stack) | Machine guide / user | Steps 2, 7 |
| High-speed fabric library directory, if any | Machine guide / site docs | Step 3 |
| Network interface names on compute nodes | `dragon-network-ifaddrs` (step 4) | Step 4 |
| Whether Dragon's native launcher works with this scheduler | Machine guide / trial | Step 5 |
| MPI implementation (Cray MPICH, Open MPI, …) | User / site docs | Step 7 |

## Steps

### 1. Allocate nodes

Request the nodes from the site scheduler. Prefer **whole nodes**. On Slurm that means
`--exclusive`, plus `--mem=0` where the default memory per CPU would exceed the node's memory.

Partial-node allocations work too, for example a QOS that caps a job at part of a node, or
`-c <n>` slices:
- **Pool sizing:** `DragonExecutionBackend` sizes each node's worker pool to the cores the job may use, and logs a warning when it does.
- **Overrides:** `batch_kwargs={"workers_per_node": <n>}` to choose the size, or `size_pool_to_affinity=False` to keep Dragon's own sizing.
- **Rank limit:** process and MPI jobs are still placed as if the job had whole nodes. Keep their total ranks per node within the CPUs the job holds.

### 2. Python environment

Create the environment on a filesystem that every node can see, then:

```bash
pip install "rhapsody-py[dragon]"
```

If the site provides a vendor Python stack that bundles its MPI and fabric libraries:
1. load the site's MPI runtime module;
2. create the venv from that Python with `--system-site-packages`, so those libraries are inherited.

### 3. Interconnect (once per environment)

**Fabric library.** If the machine has a high-speed fabric library (libfabric/OFI or UCX), point
Dragon at it once:

```bash
dragon-config add --ofi-runtime-lib=<fabric lib dir>
# some sites also need: --ofi-build-lib=<fabric lib dir> --ofi-include=<fabric include dir>
# UCX equivalents: --ucx-runtime-lib / --ucx-build-lib / --ucx-include
```

Without this step Dragon falls back to a slower transport, or fails to start its transport
across nodes.

**No fabric library?** If the machine has none, skip this step and use `-t tcp` in step 6.

### 4. Network interfaces

Dragon builds its node-to-node connections over the interfaces whose names match
`--network-prefix`. The default, `^(hsn|ipogif|ib)\d+$`, only matches HPE-Cray and InfiniBand
names.

**Listing a compute node's interfaces.** Run through the scheduler so it executes on a compute
node, not the login node:

```bash
srun -N 1 dragon-network-ifaddrs --ip --no-loopback --up --running
```

**Testing a pattern.** On Ethernet, bridged or other networks, write a regex that matches the
interface you want, and test it against those names. Dragon's own `-n` filter in this tool is
not reliable, so test with Python, which matches the same way Dragon does:

```bash
srun -N 1 dragon-network-ifaddrs --ip --no-loopback --up --running \
  | python3 -c "import json, re, sys; p = re.compile(sys.argv[1]); print([i['name'] for i in json.load(sys.stdin) if p.match(i['name'])])" '<regex>'
```

**Using it.** Pass the same `--network-prefix '<regex>'` to **both** `dragon-network-config`
(step 5) and `dragon`.

### 5. Choose the launcher (`-w`)

**Native scheduler launcher.** Use it when the scheduler exposes what Dragon needs. Run inside the
allocation:

```bash
dragon -w <slurm|pbs+pals> [-t ...] [--network-prefix '<regex>'] app.py
```

**SSH launcher.** Use it otherwise, or when the native launcher fails:
1. Generate a network configuration **inside the allocation**. On the login node it comes out empty or single-node.
2. Regenerate it for every new allocation, because the node names change.

```bash
dragon-network-config --wlm <scheduler> --output-to-yaml [--network-prefix '<regex>']
dragon -w ssh --network-config <generated>.yaml [-t ...] [--network-prefix '<regex>'] app.py
```

**Slurm and interactive shells.** `dragon-network-config` and `dragon` start their helpers with
`srun`, and hide its messages. Inside an interactive `srun --pty bash` shell, that outer step
already holds the job's CPUs, so the inner `srun` waits forever with no output. Either:
- run from an `sbatch` script; or
- `export SLURM_OVERLAP=1` in the interactive shell before launching.

### 6. Choose the transport (`-t`)

| Transport | Use when |
|---|---|
| `hsta` | A high-speed fabric is configured (step 3). |
| `tcp` | Anywhere else. It is correct everywhere, but slower. |

### 7. MPI jobs

MPI programs run as `process_templates` jobs. Choose the PMI to match the MPI the program was
built with, and always set a `timeout`:

| MPI | Task setting | Runtime requirement |
|---|---|---|
| Cray MPICH | `"pmi": "cray"` | The site's Cray MPICH runtime module loaded |
| Open MPI and other PMIx-based MPIs | `"pmi": "pmix"` | The site's Open MPI loaded; the directory holding its `libpmix.so` on `LD_LIBRARY_PATH` |
| Parallel processes that are not MPI | `"pmi": "none"` | None |

If the machine has no system MPI, use one from the site's module system or software stack.

```python
ComputeTask(
    executable="<mpi program>",
    task_backend_specific_kwargs={"process_templates": [(<ranks>, {})], "pmi": "pmix", "timeout": 600},
)
```

### 8. In the script

- **All nodes:** `DragonExecutionBackend()` uses every node the runtime started on.
- **Fewer nodes:** use `batch_kwargs={"num_nodes": <n>}`, or the launcher's `-N <n>`.
- **Placing work on specific nodes:** build `Policy(placement=Policy.Placement.HOST_NAME, host_name=...)` from `dragon.native.machine.System().nodes`. See "CPU and GPU affinity (Dragon)" in `SKILL.md`.

### 9. Verify the run spans the nodes

```bash
<launch command from step 5> assets/workload_template.py --backend dragon --pin-cpus   # path inside this skill
```

The report's `host` column must list more than one host, and every pinned task must show
`[placement ok]`. For MPI, also run one small MPI program with `process_templates` and the PMI
from step 7. Check that it ends `DONE` and that each rank reports the expected rank count.

## When something goes wrong

See [troubleshooting.md](troubleshooting.md), section "Multi-node Dragon".
