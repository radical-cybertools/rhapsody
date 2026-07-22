import asyncio
import logging
import multiprocessing as mp

import rhapsody
from rhapsody.api import AITask
from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonExecutionBackendV3
from rhapsody.backends import DragonVllmInferenceBackend

rhapsody.enable_logging(level=logging.DEBUG)

logger = logging.getLogger(__name__)


async def main():
    mp.set_start_method("dragon")

    execution_backend = await DragonExecutionBackendV3()

    inference_backend = DragonVllmInferenceBackend(
        config_file="config.yaml",
        model_name="Qwen2.5-0.5B-Instruct",
        num_nodes=1,
        num_gpus=1,
        tp_size=1,
        port=8001,
        offset=0,  # Change this to control the number of nodes each inference pipeline takes
    )

    # Initialize ALL services concurrently
    logger.info("Initializing 1 service...")
    await inference_backend.initialize()

    # Define multiple tasks with single or multiple prompts
    # Note: Explicit backend mapping by user
    tasks = [
        AITask(prompt="What is the capital of France?", backend=inference_backend.name),
        AITask(prompt=["Tell me a joke", "What is 2+2?"], backend=inference_backend.name),
        ComputeTask(
            executable="/usr/bin/echo",
            arguments=["Hello from Dragon backend!"],
            backend=execution_backend.name,
        ),
    ]

    session = Session([execution_backend, inference_backend])

    # Submit all tasks at once via session - they will be routed correctly!
    print(f"Submitting {len(tasks)} mixed tasks via Session...")
    await session.submit_tasks(tasks)

    # Gather results using standard asyncio
    results = await asyncio.gather(*tasks)

    for i, task in enumerate(results):
        backend_name = task.get("backend")
        if isinstance(task, AITask):
            # AITask: model response is in task.response
            print(f"Task {i + 1} [AI] ({backend_name}): {task.response}", flush=True)
        elif task.get("function"):
            # ComputeTask (function): return value is in task.return_value
            print(f"Task {i + 1} [Compute/fn] ({backend_name}): {task.return_value}", flush=True)
        else:
            # ComputeTask (executable): output is in task.stdout / task.stderr
            print(
                f"Task {i + 1} [Compute/exec] ({backend_name}): "
                f"stdout={task.stdout.strip()!r}  exit_code={task.exit_code}",
                flush=True,
            )

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
