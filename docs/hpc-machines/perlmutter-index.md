# NERSC Perlmutter

[Perlmutter](https://docs.nersc.gov/systems/perlmutter/architecture/) is a Cray EX supercomputer at the National Energy Research Scientific Computing Center (NERSC), DOE. It features both GPU and CPU partitions, with GPU nodes equipped with 4x NVIDIA A100 40GB GPUs each. It runs a Cray PE software stack with Cray MPICH and Cray libfabric for high-speed interconnect.

There are two paths for running RHAPSODY on Perlmutter:

- **[Scalable Execution](perlmutter-scalable.md)** — large-scale, multi-node task execution via `DragonExecutionBackend`.
- **[AI-HPC](perlmutter-ai-hpc.md)** — AI inference and fine-tuning workloads using `DragonVllmInferenceBackend` on Perlmutter's GPU partition.
