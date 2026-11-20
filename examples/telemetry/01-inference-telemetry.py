"""Inference telemetry: run AI tasks on the vLLM backend and look at what telemetry recorded.

A set of scientific prompts runs through ``DragonVllmInferenceBackend`` with telemetry enabled.
Afterwards the script prints the batches the backend sent (from the ``chat`` spans), the per-task
attributes and the inference metrics. The telemetry is also saved as JSONL in ``--out``, and
``01-inference-telemetry-plot.py`` draws it.

Run it on a GPU allocation with the Dragon launcher, for example:

    dragon <launcher options for your machine> examples/telemetry/01-inference-telemetry.py \\
        --model /path/to/Qwen2.5-0.5B-Instruct --nodes 2 --gpus-per-node 1 --tasks 64
"""

import argparse
import asyncio
import logging
import multiprocessing as mp
import os

import rhapsody
from rhapsody.api import AITask
from rhapsody.api import Session
from rhapsody.backends import DragonVllmInferenceBackend
from rhapsody.backends.ai.config import HardwareConfig
from rhapsody.backends.ai.config import ModelConfig

SCOPE = "rhapsody.instrumentation.inference"
PREFIX = "rhapsody.inference."

# Scientific simulation prompts: each one is a task.
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


def summarize(telemetry, events):
    """Return the lines that describe what telemetry recorded for the inference backend."""
    lines = []
    spans = list(telemetry.read_traces())

    # 1. One "chat <model>" span per batch the backend sent to the model.
    batches = [s for s in spans if s.name == "chat" or s.name.startswith("chat ")]
    batches.sort(key=lambda s: s.start_time)
    lines.append(f"{len(batches)} batches (one span each):")
    for span in batches:
        a = span.attributes
        lines.append(
            f"  {a.get('rhapsody.inference.batch.id')}: "
            f"{a.get('rhapsody.inference.batch.requests')} requests, "
            f"{a.get('rhapsody.inference.batch.size')} prompts, "
            f"trigger={a.get('rhapsody.inference.batch.trigger')}, "
            f"{(span.end_time - span.start_time) / 1e6:.0f} ms, "
            f"model {a.get('rhapsody.inference.dragon.model.duration', float('nan')):.2f} s, "
            f"host {a.get('rhapsody.inference.dragon.worker.host')}"
        )

    # 2. Each task span carries the batch it ran in and how long it waited in the queue.
    tasks = [s for s in spans if s.name == "task" and PREFIX + "batch.id" in s.attributes]
    lines.append(f"{len(tasks)} task spans with inference attributes, the first three:")
    for span in tasks[:3]:
        a = span.attributes
        lines.append(
            f"  {a.get('task_id')}: batch {a.get(PREFIX + 'batch.id')}, "
            f"{a.get(PREFIX + 'prompt_count')} prompts, "
            f"queue wait {a.get(PREFIX + 'request.queue.duration', 0.0):.3f} s"
        )

    # 3. Metrics of the inference scope (counters have a value, histograms a count and a sum).
    lines.append("inference metrics:")
    data = telemetry.read_metrics()
    for resource_metrics in data.resource_metrics if data else ():
        for scope_metrics in resource_metrics.scope_metrics:
            if scope_metrics.scope.name != SCOPE:
                continue
            for metric in scope_metrics.metrics:
                points = metric.data.data_points
                unit = metric.unit or ""
                if not points:
                    continue
                if hasattr(points[0], "count"):
                    count = sum(p.count for p in points)
                    mean = sum(p.sum for p in points) / count if count else 0.0
                    lines.append(f"  {metric.name}: {count} samples, mean {mean:.3f} {unit}")
                else:
                    lines.append(f"  {metric.name}: {sum(p.value for p in points)} {unit}")

    # 4. The same facts as events (what subscribe() delivers and the JSONL file holds).
    started = sum(e.event_type == PREFIX + "BatchStarted" for e in events)
    done = sum(e.event_type == PREFIX + "BatchDone" for e in events)
    failed = sum(e.event_type == PREFIX + "BatchFailed" for e in events)
    lines.append(f"events seen: {started} BatchStarted, {done} BatchDone, {failed} BatchFailed")
    return lines


async def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default=os.environ.get("MODEL_PATH"), help="model name or path")
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--gpus-per-node", type=int, default=1)
    parser.add_argument("--tasks", type=int, default=64, help="number of AI tasks")
    parser.add_argument("--out", default="inference-telemetry", help="directory for the JSONL file")
    args = parser.parse_args()
    if not args.model:
        parser.error("give --model or set MODEL_PATH")

    rhapsody.enable_logging(level=logging.INFO)
    mp.set_start_method("dragon")

    inference_backend = await DragonVllmInferenceBackend(
        model=ModelConfig(
            model_name=args.model,
            hf_token=os.environ.get("HF_TOKEN", ""),
            tp_size=1,
        ),
        hardware=HardwareConfig(num_nodes=args.nodes, num_gpus=args.gpus_per_node, node_offset=0),
        # Small batches, so a short run still shows several of them (the default is 1024).
        max_batch_size=16,
        max_batch_wait_ms=500,
        port=8001,
    )

    session = Session([inference_backend])
    events = []
    # Telemetry must start before the tasks are submitted.
    telemetry = await session.start_telemetry(checkpoint_path=args.out)
    telemetry.subscribe(events.append)

    async with session:
        tasks = [
            AITask(
                prompt=SIMULATION_PROMPTS[i % len(SIMULATION_PROMPTS)],
                backend=inference_backend.name,
            )
            for i in range(args.tasks)
        ]
        await session.submit_tasks(tasks)
        await session.wait_tasks(tasks, timeout=3600)  # does not raise for failed tasks

        outputs = [t.response for t in tasks if t.response]  # one list of answers per task
        print(f"Collected {len(outputs)} simulation outputs", flush=True)
        for i, answers in enumerate(outputs[:3]):
            first = answers[0] if isinstance(answers, list) else answers
            print(f"  Sim {i}: {first[:80]}...", flush=True)

    # Leaving the block stopped telemetry, so the recorded data is complete.
    print("\n".join(summarize(telemetry, events)), flush=True)
    print(f"JSONL checkpoint written to {args.out}/", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
