# RHAPSODY Tutorials

This directory contains Jupyter notebook tutorials for learning RHAPSODY.

## Launching the Notebook

### With Dragon Backend (Required for HPC execution)

The tutorial uses `DragonExecutionBackendV3`, which requires the Dragon runtime. You **must** launch the notebook server using `dragon-jupyter` instead of the standard `jupyter` command:

```bash
dragon-jupyter
```

This starts a Jupyter server within the Dragon-managed runtime, giving your notebook access to Dragon's process management, distributed execution, and Batch API. Without it, any cell that initializes a Dragon backend will fail.

> **Why not regular `jupyter notebook`?**
> Dragon requires its own managed runtime environment. The `dragon-jupyter` command bootstraps this environment before starting the Jupyter server, so all kernels inherit the necessary Dragon context.

### Quick Start

```bash
# 1. Activate your environment
source /path/to/your/venv/bin/activate

# 2. Launch with Dragon support
dragon-jupyter

# 3. Open rhapsody-tutorial.ipynb in the browser
```

## Contents

| Notebook | Description |
|----------|-------------|
| `rhapsody-tutorial.ipynb` | Progressive tutorial covering basic usage, heterogeneous workloads, and AI-HPC workflows with vLLM. |
