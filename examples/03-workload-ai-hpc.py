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
        num_gpus=0,
        tp_size=1,
        port=8002,
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
    logger.info(f"Submitting {len(tasks)} mixed tasks via Session...")
    await session.submit_tasks(tasks)

    # Gather results using standard asyncio
    results = await asyncio.gather(*tasks)

    for i, task in enumerate(results):
        rtype = "AI" if "prompt" in task else "Compute"
        logger.info(f"Task {i + 1} [{rtype}] ({task.get('backend')}): {task.return_value}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
