import argparse
import asyncio
import time

from dragon.workflows.batch import Batch

from rhapsody.api import ComputeTask
from rhapsody.api import Session
from rhapsody.backends import DragonExecutionBackend
from rhapsody.backends import DragonNativeExecutionBackend

width_num_tasks = 10
width_throughput = 20


def no_op():
    pass


async def run_tasks_with_rhapsody(
    session: Session, num_tasks: int, warmup: bool = False, backend=None
) -> None:
    start_time = time.time()

    tasks = [ComputeTask(function=no_op) for _ in range(num_tasks)]
    await session.submit_tasks(tasks)

    if isinstance(backend, DragonExecutionBackend):
        backend.wait()  # high performance wait loop
    else:
        # the native backend has no fence: wait until the Session has every result
        await session.wait_tasks(tasks)

    end_time = time.time()

    if not warmup:
        runtime = end_time - start_time
        throughput = num_tasks / runtime
        print(
            f"{num_tasks:<{width_num_tasks}} {throughput:<{width_throughput}}",
            flush=True,
        )

    if isinstance(backend, DragonExecutionBackend):
        # the backend's monitor thread may still be reading results from the DDict that
        # clear_results() clears: wait until the Session has every result first
        await session.wait_tasks(tasks)
        backend.batch.clear_results()


async def run_tasks_with_native_batch(batch, num_tasks: int, warmup: bool = False) -> None:
    start_time = time.time()

    for _ in range(num_tasks):
        batch.function(no_op)

    batch.fence()  # wait for all submitted tasks to complete

    end_time = time.time()

    if not warmup:
        runtime = end_time - start_time
        throughput = num_tasks / runtime
        print(
            f"{num_tasks:<{width_num_tasks}} {throughput:<{width_throughput}}",
            flush=True,
        )

    batch.clear_results()


async def run_bench_with_rhapsody(
    session: Session, min_tasks: int, max_tasks: int, backend=None
) -> None:
    print("task throughput benchmark", flush=True)
    print("-------------------------", flush=True)

    await run_tasks_with_rhapsody(session, min_tasks, warmup=True, backend=backend)

    num_tasks_str = "num tasks"
    throughput_str = "throughput [tasks/s]"
    print(
        f"{num_tasks_str.ljust(width_num_tasks)} {throughput_str.ljust(width_throughput)}",
        flush=True,
    )

    num_tasks = min_tasks

    while num_tasks <= max_tasks:
        await run_tasks_with_rhapsody(session, num_tasks, backend=backend)
        num_tasks *= 2

    print("", flush=True)


async def run_bench_with_native_batch(min_tasks: int, max_tasks: int, batch):
    print("task throughput benchmark", flush=True)
    print("-------------------------", flush=True)

    await run_tasks_with_native_batch(batch, min_tasks, warmup=True)

    num_tasks_str = "num tasks"
    throughput_str = "throughput [tasks/s]"
    print(
        f"{num_tasks_str.ljust(width_num_tasks)} {throughput_str.ljust(width_throughput)}",
        flush=True,
    )

    num_tasks = min_tasks

    while num_tasks <= max_tasks:
        await run_tasks_with_native_batch(batch, num_tasks)
        num_tasks *= 2

    print("", flush=True)


async def main(
    min_tasks: int,
    max_tasks: int,
    with_rhapsody: bool,
    with_native: bool,
    workers_per_node: int | None,
) -> None:
    if with_native:
        backend = await DragonNativeExecutionBackend(workers_per_node=workers_per_node)
        async with Session(backends=[backend]) as session:
            await run_bench_with_rhapsody(session, min_tasks, max_tasks, backend=backend)
    elif with_rhapsody:
        backend = await DragonExecutionBackend(batch_kwargs={"task_logs": False})
        async with Session(backends=[backend]) as session:
            await run_bench_with_rhapsody(session, min_tasks, max_tasks, backend=backend)
    else:
        batch = Batch(task_logs=False)
        try:
            await run_bench_with_native_batch(min_tasks, max_tasks, batch)
        finally:
            # same teardown as DragonExecutionBackend.shutdown()
            batch.join(timeout=10.0)
            batch.destroy(timeout=15.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task throughput benchmark")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--with-rhapsody", action="store_true", help="submit through Rhapsody")
    mode.add_argument(
        "--with-batch", action="store_true", help="submit through native Dragon Batch"
    )
    mode.add_argument(
        "--with-rhapsody-dragon-native",
        action="store_true",
        help="submit through Rhapsody with DragonNativeExecutionBackend",
    )
    parser.add_argument("--min_tasks", type=int, default=4, help="minimum number of tasks to run")
    parser.add_argument("--max_tasks", type=int, default=128, help="maximum number of tasks to run")
    parser.add_argument(
        "--workers_per_node",
        type=int,
        default=None,
        help="pool size per node, with --with-rhapsody-dragon-native (default: one per core)",
    )
    args = parser.parse_args()

    if args.workers_per_node is not None and not args.with_rhapsody_dragon_native:
        parser.error("--workers_per_node needs --with-rhapsody-dragon-native")

    if args.max_tasks < args.min_tasks:
        args.max_tasks = args.min_tasks

    asyncio.run(
        main(
            args.min_tasks,
            args.max_tasks,
            args.with_rhapsody,
            args.with_rhapsody_dragon_native,
            args.workers_per_node,
        )
    )
