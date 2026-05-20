"""Demo (c): invoke a Julia function as a Rhapsody task.

Args are marshalled to/from JSON via argv/stdout. Each call spawns a fresh
Julia process; per-call JIT overhead (~0.3-1 s) is the obvious target for a
real integration (sysimage + persistent worker).

Run:
    python examples/julia/03_function_task.py            # Concurrent
    dragon examples/julia/03_function_task.py dragon     # Dragon (CLI arg picks backend)
"""

import asyncio
import json
import sys
import os

from rhapsody.api import ComputeTask, Session

HERE = os.path.dirname(os.path.abspath(__file__))


def julia_call(fn: str, *args) -> ComputeTask:
    payload = json.dumps({"fn": fn, "args": list(args)})
    return ComputeTask(
        executable="julia",
        arguments=[
            f"--project={HERE}",
            os.path.join(HERE, "call_fn.jl"),
            payload,
        ],
    )


async def main():

    if len(sys.argv) > 1 and sys.argv[1] == "dragon":
        from rhapsody.backends import DragonExecutionBackendV3 as Backend
    else:
        from rhapsody.backends import ConcurrentExecutionBackend as Backend

    backend = await Backend()
    session = Session([backend])

    tasks = [
        julia_call("greet", "rhapsody"),
        julia_call("add", 17, 25),
    ]

    await session.submit_tasks(tasks)
    await session.wait_tasks(tasks)

    for task in tasks:
        out = (task.stdout or "").strip()
        if not out:
            print(f"{task.uid}: NO OUTPUT (stderr={task.stderr!r})")
            continue
        result = json.loads(out)["result"]
        print(f"{task.uid}: {result}")

    await backend.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
