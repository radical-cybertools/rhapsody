# Princeton StellarAI — AI-HPC

StellarAI is a Slurm cluster at Princeton University / PPPL. Its `gpu` partition has 20 nodes, each with 96 CPU cores, about 980 GB of memory and 8x NVIDIA B200 GPUs (180 GB each). Nodes are connected over Ethernet and a bonded link, without a Cray PE or libfabric stack, so Dragon uses its TCP transport.

This guide covers running RHAPSODY's AI inference workloads using the `DragonVllmInferenceBackend` on StellarAI's GPU partition. For large-scale, multi-node task execution via `DragonExecutionBackend`, see [Scalable Execution](stellarai-scalable.md).

## Environment Setup

### 1. Create and activate a virtual environment using the system Python

```bash
/usr/bin/python3.12 -m venv ~/envs/rhapsody-ai
source ~/envs/rhapsody-ai/bin/activate
```

### 2. Install RHAPSODY with Dragon AI (vLLM) support

```bash
pip install "rhapsody-py[ai]"
```

This installs vLLM, PyTorch and FlashInfer, so no separate install step is needed. The setup on this page was verified with vLLM 0.19.1, PyTorch 2.10.0 (CUDA 12.8), FlashInfer 0.6.6 and Dragon 0.15.0.

### 3. Load the CUDA toolkit

```bash
module load cudatoolkit/13.1
```

!!! note
    PyTorch ships its own CUDA runtime, but on the B200 nodes vLLM uses FlashInfer's attention kernels, which FlashInfer compiles with `nvcc` the first time they run (and caches in `~/.cache/flashinfer`). Without the module the compile has no `nvcc`. Expect the first launch to be slower than later ones.

### 4. Provide the compiler headers missing on the compute nodes

!!! warning "Missing headers on the `sai-*` compute nodes"
    The compute nodes lack two sets of headers that the login node has, and inference fails while vLLM starts:

    - `Python.h` (from `python3-devel`). Triton fails with `cuda_utils.c: fatal error: Python.h: No such file or directory`.
    - The cuBLAS headers of CUDA 13.1. FlashInfer fails with `cudaUtils.h: fatal error: cublasLt.h: No such file or directory`.

    Until they are installed on the compute nodes, copy them from the login node to your home directory once:

    ```bash
    mkdir -p ~/stellarai-include
    cp -r /usr/include/python3.12 ~/stellarai-include/
    cp /usr/local/cuda-13.1/include/cublas*.h ~/stellarai-include/
    ```

    and point the compilers at them in every job:

    ```bash
    export CPATH=$HOME/stellarai-include:$HOME/stellarai-include/python3.12
    export C_INCLUDE_PATH=$CPATH
    ```

    Dragon passes the environment on to its workers, so exporting these before `dragon` is enough. If FlashInfer failed before, remove its half-built kernels with `rm -rf ~/.cache/flashinfer` before the next run. Ask the StellarAI admins to install `python3-devel` and the CUDA cuBLAS headers on the compute nodes to make this step unnecessary.

### 5. Download the model

`DragonVllmInferenceBackend` takes model settings as a `ModelConfig` object (see the [AI guide](../ai/index.md#dragon-vllm-inference-backend)). Download the model on the login node and pass the local snapshot path as `ModelConfig(model_name=...)`:

```bash
hf download Qwen/Qwen2.5-0.5B-Instruct --local-dir ~/models/Qwen2.5-0.5B-Instruct
```

Then set, before launching your job, so the model loads from the local snapshot with no Hub calls:

```bash
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
```

### 6. Allocate GPU nodes

```bash
salloc -N 2 -p gpu --qos=gpu-short --gres=gpu:1 --ntasks-per-node=1 -t 1:00:00 --account=<your_account>
```

Once the allocation is granted, launch with the `dragon` command, the TCP transport and a network prefix that matches StellarAI's interfaces:

```bash
dragon -t tcp --network-prefix '^(eno|bond)\w*$' <script.py>
```

!!! warning "`-t tcp` is required"
    Without `-t tcp`, a multi-node launch fails within seconds with `RuntimeError: Abnormal exit detected` from `dragon-backend`, before any of your code runs.

!!! note
    vLLM prints `UserWarning: Failed to get the IP address, using 0.0.0.0 by default`. It is harmless here; set `VLLM_HOST_IP` to the node's address to silence it.

---

## Example

### Simulation via Inference on 2 Nodes

This example runs a batch of scientific "simulations" through LLM inference: each `AITask` sends a domain-specific prompt to `DragonVllmInferenceBackend`, which serves the model on one B200 on each of the 2 allocated nodes, and the generated outputs are collected as synthetic simulation data.

```python title="ai-inference.py"
import asyncio
import logging
import multiprocessing as mp
import os

import rhapsody
from rhapsody.api import AITask, Session
from rhapsody.backends import DragonVllmInferenceBackend
from rhapsody.backends.ai.config import HardwareConfig, ModelConfig

rhapsody.enable_logging(level=logging.INFO)

# Scientific simulation prompts, each represents one simulation scenario
SIMULATION_PROMPTS = [
    "Simulate the energy minimization of a water molecule using DFT. Report bond lengths and angles.",
    "Describe the outcome of a molecular dynamics simulation of a lipid bilayer at 310K over 100ns.",
    "Run a Monte Carlo simulation of a 2D Ising model at the critical temperature. Report magnetization.",
    "Simulate heat diffusion in a 1D rod with fixed boundary conditions. Report steady-state profile.",
    "Describe the result of a lattice Boltzmann simulation of fluid flow through a porous medium.",
    "Simulate protein folding of a 50-residue helix using coarse-grained MD. Report RMSD over time.",
    "Run a finite element simulation of stress distribution in a titanium beam under axial load.",
    "Simulate a 2D incompressible Navier-Stokes flow around a cylinder at Re=100. Report drag coefficient.",
]


async def main():
    mp.set_start_method("dragon")

    inference_backend = await DragonVllmInferenceBackend(
        model=ModelConfig(
            model_name=os.path.expanduser("~/models/Qwen2.5-0.5B-Instruct"),
            hf_token="",
            tp_size=1,
        ),
        # One GPU on each of the 2 nodes
        hardware=HardwareConfig(num_nodes=2, num_gpus=1, node_offset=0),
        max_batch_size=32,
        max_batch_wait_ms=500,
        use_service=False,  # engine mode, no HTTP server
    )

    session = Session([inference_backend])

    async with session:
        print(f"--- Running {len(SIMULATION_PROMPTS)} simulations ---", flush=True)

        sim_tasks = [
            AITask(prompt=prompt, backend=inference_backend.name)
            for prompt in SIMULATION_PROMPTS
        ]

        futures = await session.submit_tasks(sim_tasks)
        await asyncio.gather(*futures)

        simulation_outputs = [t.response for t in sim_tasks if t.response]
        print(f"Collected {len(simulation_outputs)} simulation outputs", flush=True)

        for i, out in enumerate(simulation_outputs):
            print(f"  Sim {i}: {out[:80]}...", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
```

Run with:

```bash
module load cudatoolkit/13.1
source ~/envs/rhapsody-ai/bin/activate
export CPATH=$HOME/stellarai-include:$HOME/stellarai-include/python3.12
export C_INCLUDE_PATH=$CPATH
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

dragon -t tcp --network-prefix '^(eno|bond)\w*$' ai-inference.py
```

!!! success "Expected Output"
    ```text
    --- Running 8 simulations ---
    Collected 8 simulation outputs
      Sim 0: The DFT energy minimization of a water molecule yields an O-H bond length of 0.96 Å...
      Sim 1: The lipid bilayer simulation at 310K shows stable membrane structure with an area per...
      ...
      Sim 7: The drag coefficient Cd ≈ 1.38, consistent with experimental values at Re=100. Karman...
    ```
    The generated text varies from run to run.

!!! tip "Scaling up"
    - Increase `SIMULATION_PROMPTS` to hundreds or thousands of scenarios; the backend batches them up to `max_batch_size` per request
    - Each StellarAI GPU node has 8 B200s: request more with `--gres=gpu:<n>` and raise `HardwareConfig(num_gpus=...)`
    - For larger models, set `ModelConfig(tp_size=...)` to split one model across several GPUs of a node

### Batch job

The same run as an `sbatch` script:

```bash title="ai-inference.sbatch"
#!/bin/bash
#SBATCH --job-name=rhapsody-ai
#SBATCH --partition=gpu
#SBATCH --qos=gpu-short
#SBATCH --account=<your_account>
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=16
#SBATCH --time=01:00:00

module load cudatoolkit/13.1
source ~/envs/rhapsody-ai/bin/activate

# Headers missing on the compute nodes (see step 4)
export CPATH=$HOME/stellarai-include:$HOME/stellarai-include/python3.12
export C_INCLUDE_PATH=$CPATH
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

dragon -t tcp --network-prefix '^(eno|bond)\w*$' ai-inference.py
```

Submit with `sbatch ai-inference.sbatch`.

---

## Contributors

<div class="grid cards" markdown>

-   :fontawesome-solid-user-tie: **Aymen Alsaadi**

    ---

    RADICAL Lab, Rutgers University

    [:fontawesome-solid-arrow-up-right-from-square: Profile](https://radical.rutgers.edu/people/aymen-alsaadi)

</div>
