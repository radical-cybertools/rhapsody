"""
Dragon V3 Workflow with Multiple VLLM Services (Node-Partitioned)
Each service uses different nodes via offset
"""
import json
import asyncio
import logging
import rhapsody
import multiprocessing as mp
from typing import List
import itertools


from rhapsody.api.session import Session
from rhapsody.backends.inference.vllm import DragonVllmInferenceBackend
from rhapsody.backends import DragonExecutionBackendV3

rhapsody.enable_logging(level=logging.DEBUG)

logger = logging.getLogger(__name__)

async def main():
    """
    + Start Radical.Asyncflow with Dragon execution backend.
    + Start the VLLM inference engine as a service using Dragon
    + Distribuite the workflows across the services in RR fashion via Asyncflow 
    + Service endpoints are centralized on the head node for:
        - Easier endpoint management
        - Simpler load balancing
        - Centralized logging
        - Better control plane
    
    Note: Only send batch tasks. I.e, tasks that can send N requests.
    """
    mp.set_start_method("dragon")

    execution_backend = await DragonExecutionBackendV3()

    inference_backend = DragonVllmInferenceBackend(
        config_file="/home/aymen/RADICAL/models/config.yaml",
        model_name="/home/aymen/RADICAL/models/Qwen2.5-0.5B-Instruct",
        num_nodes=1,
        num_gpus=0,
        tp_size=1,
        port=8002,
        offset=0  # Change this to control the number of nodes each inference pipeline takes
    )

    # Initialize ALL services concurrently
    logger.info(f"Initializing 1 service...")
    await inference_backend.initialize()

    # Define multiple tasks with single or multiple prompts
    # Note: Explicit backend mapping by user
    tasks = [
        rhapsody.AITask(
            prompt='What is the capital of France?', 
            backend=inference_backend.name
        ),
        rhapsody.AITask(
            prompt=['Tell me a joke', 'What is 2+2?'], 
            backend=inference_backend.name
        ),
        rhapsody.ComputeTask(
            executable='/usr/bin/echo',
            arguments=['Hello from Dragon backend!'],
            backend=execution_backend.name
        )
    ]

    session = Session([execution_backend, inference_backend])

    # Submit all tasks at once via session - they will be routed correctly!
    logger.info(f"Submitting {len(tasks)} mixed tasks via Session...")
    await session.submit_tasks(tasks)

    # Gather results using standard asyncio
    results = await asyncio.gather(*tasks)

    print("\nMixed Results:")
    for i, task in enumerate(results):
        rtype = "AI" if 'prompt' in task else "Compute"
        print(f"Task {i+1} [{rtype}] ({task.get('backend')}): {task.return_value}")

    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
