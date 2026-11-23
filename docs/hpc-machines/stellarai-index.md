# Princeton StellarAI

StellarAI is a Slurm cluster at Princeton University / PPPL. Its `gpu` partition has 20 nodes, each with 96 CPU cores (2 sockets, one thread per core), about 980 GB of memory and 8x NVIDIA B200 GPUs (180 GB each). Its `cpu` partition has 95 nodes with 96 cores and about 370 GB each. Nodes are connected over Ethernet (`eno8303`) and a bonded link (`bond0`); there is no Cray PE or libfabric stack, so Dragon uses its TCP transport.

GPU jobs choose a QOS by length: `gpu-test` (up to 30 minutes), `gpu-short` (up to 4 hours), `gpu-medium` (up to 1 day) and `gpu-long`.

There are two paths for running RHAPSODY on StellarAI:

- **[Scalable Execution](stellarai-scalable.md)** — large-scale, multi-node task execution via `DragonExecutionBackend`.
- **[AI-HPC](stellarai-ai-hpc.md)** — AI inference workloads using `DragonVllmInferenceBackend` on StellarAI's B200 GPU partition.
