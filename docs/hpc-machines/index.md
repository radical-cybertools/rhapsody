# RHAPSODY on NSF and DOE Machines

This section provides machine-specific setup guides and verified examples for running RHAPSODY on NSF and DOE leadership computing facilities. These machines are periodically tested and verified against RHAPSODY and its pre-release changes.

!!! note
    All of the tutorials in this section target **multi-node** runs using `DragonExecutionBackend`. For single-node runs, testing, or debugging, it is recommended to use `ConcurrentExecutionBackend` (`ProcessPoolExecutor`) instead.

## Backend compatibility

**`ConcurrentExecutionBackend (designed for debug and tests single node only)`** works out of the box on every machine listed here — no machine-specific configuration is required. Install RHAPSODY and run.

**`DragonExecutionBackend (designed for large scale multi-node)`** requires machine-specific setup: each HPC system has its own software stack, MPI library, interconnect, and job launcher. The exact steps — module loading, virtual environment creation, transport configuration, and launch command — differ per machine and are documented in each section below.

Each guide covers environment setup, module loading, job allocation, and working examples.

!!! warning "Partial-node allocations"
    Dragon's `Batch` sizes each node's worker pool from the node's CPU count (`os.cpu_count() // 2`), not from the CPUs the job may use. On a Slurm job that holds part of a node (for example `--ntasks=4` on a 512-CPU node), that puts hundreds of workers on a few CPUs, and each task can take minutes. `DragonExecutionBackend` therefore sizes the pools from the CPUs Dragon may use on each node and logs a warning when it does. Request whole nodes (`--exclusive`, plus `--mem=0` where the default memory per CPU would exceed the node) to get the full pool; pass `size_pool_to_affinity=False` to keep Batch's own sizing.

## Supported Machines

| Machine | Facility | Stack | Status |
|---------|----------|-------|--------|
| [NCSA Delta](ncsa-delta.md) | NSF / NCSA | Cray MPICH, SSH launcher | Verified |
| [NERSC Perlmutter — Scalable Execution](perlmutter-scalable.md) | DOE / NERSC | Cray libfabric + CUDA, SLURM launcher | Verified |
| [NERSC Perlmutter — AI-HPC](perlmutter-ai-hpc.md) | DOE / NERSC | Cray libfabric + CUDA, SLURM launcher | Verified |
| [PSC Bridges-2](bridges2.md) | NSF / PSC | Standard SLURM, SSH launcher, TCP | Verified |
| [Purdue Anvil](anvil.md) | NSF / ACCESS / Purdue | Cray MPICH, SSH launcher (HSTA) | Verified |

More machines will be added as they are tested and validated.
