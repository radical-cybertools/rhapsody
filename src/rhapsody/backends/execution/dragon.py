import asyncio
import glob
import json
import logging
import os
import queue
import socket
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import Callable
from typing import Optional

import psutil
import typeguard

from ..base import BaseExecutionBackend
from ..constants import BackendMainStates
from ..constants import StateMapper

try:
    import multiprocessing as mp

    import dragon
    from dragon.data.ddict.ddict import DDict
    from dragon.infrastructure.gpu_desc import AccVendor
    from dragon.infrastructure.gpu_desc import find_accelerators
    from dragon.infrastructure.policy import Policy

    # Node Telemetry only
    from dragon.native.event import Event
    from dragon.native.machine import System
    from dragon.native.process import Popen
    from dragon.native.process import Process
    from dragon.native.process import ProcessTemplate
    from dragon.native.process_group import DragonUserCodeError
    from dragon.native.process_group import ProcessGroup
    from dragon.native.queue import Queue
    from dragon.telemetry import Telemetry
    from dragon.workflows.batch import Batch

except ImportError:  # pragma: no cover - environment without Dragon
    dragon = None
    Process = None
    ProcessTemplate = None
    ProcessGroup = None
    Popen = None
    Queue = None
    DDict = None
    System = None
    Policy = None
    Batch = None
    Event = None
    Telemetry = None
    AccVendor = None
    find_accelerators = None


def _get_logger() -> logging.Logger:
    """Get logger for dragon backend module.

    This function provides lazy logger evaluation, ensuring the logger
    is created after the user has configured logging, not at module import time.
    """
    return logging.getLogger(__name__)


DRAGON_DEFAULT_REF_THRESHOLD = int(os.environ.get("DRAGON_DEFAULT_REF_THRESHOLD", 1024 * 1024))

# ============================================================================
# V1 Integration Helper classes
# ============================================================================


class TaskTypeV1(Enum):
    """Enumeration of supported task types."""

    SINGLE_FUNCTION = "single_function"
    SINGLE_EXECUTABLE = "single_executable"
    MULTI_FUNCTION = "multi_function"
    MULTI_EXECUTABLE = "multi_executable"
    MPI_FUNCTION = "mpi_function"
    MPI_EXECUTABLE = "mpi_executable"


@dataclass
class TaskInfoV1:
    """Container for task runtime information."""

    task_type: TaskTypeV1
    ranks: int
    start_time: float
    canceled: bool = False
    process: Optional[Process] = None
    group: Optional[ProcessGroup] = None


@dataclass
class ExecutableTaskCompletionV1:
    """Task completion data from executable process."""

    task_uid: str
    rank: int
    process_id: int
    stdout: str
    stderr: str
    exit_code: int
    timestamp: float

    def to_result_dict(self) -> dict:
        """Convert to task result dictionary."""
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "return_value": None,
            "exception": None
            if self.exit_code == 0
            else f"Process exited with code {self.exit_code}",
        }


@dataclass
class FunctionTaskCompletionV1:
    """Task completion data from function process - sent via queue."""

    task_uid: str
    rank: int
    process_id: int
    stdout: str
    stderr: str
    exit_code: int
    timestamp: float
    success: bool
    exception: Optional[str]
    traceback: Optional[str]
    return_value: Any  # Actual value or DataReference
    stored_in_ddict: bool  # True if return_value is in DDict

    def to_result_dict(self) -> dict:
        """Convert to task result dictionary."""
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "return_value": self.return_value,
            "exception": self.exception,
            "success": self.success,
        }


class DataReferenceV1:
    """Zero-copy reference to function results stored in DDict.

    Points directly to result keys written by function wrapper.
    User controls when to resolve and fetch data from DDict.
    """

    def __init__(self, task_uid: str, ranks: int, ddict: DDict, backend_id: str):
        self._task_uid = task_uid
        self._ranks = ranks
        self._ddict = ddict
        self._backend_id = backend_id

    @property
    def task_uid(self) -> str:
        return self._task_uid

    @property
    def ranks(self) -> int:
        return self._ranks

    @property
    def backend_id(self) -> str:
        return self._backend_id

    def resolve(self) -> Any:
        """Resolve reference to actual return values from DDict.

        Fetches data directly from function-wrapper-written keys in DDict:
        - Single rank (ranks=1): Returns single return_value
        - Multi-rank (ranks>1): Returns list of return_values indexed by rank
        """
        if self._ranks == 1:
            # Single rank - return single value
            result_key = f"return_{self._task_uid}_rank_0"
            if result_key not in self._ddict:
                raise KeyError(f"Result data not found for task: {self._task_uid}")

            return self._ddict[result_key]
        else:
            # Multi-rank - return list of values
            return_values = []
            for rank in range(self._ranks):
                result_key = f"return_{self._task_uid}_rank_{rank}"
                if result_key not in self._ddict:
                    raise KeyError(f"Result data not found for task {self._task_uid} rank {rank}")

                return_values.append(self._ddict[result_key])

            return return_values

    def __repr__(self) -> str:
        return f"DataReferenceV1(task_uid='{self._task_uid}', ranks={self._ranks}, backend_id='{self._backend_id}')"


class SharedMemoryManagerV1:
    """Manages optional DDict storage for large function results."""

    def __init__(self, ddict: DDict, system: System, logger: logging.Logger):
        self.ddict = ddict
        self.system = system
        self.logger = logger
        self.backend_id = f"dragon_{uuid.uuid4().hex[:8]}"

    async def initialize(self):
        """Initialize the storage manager."""
        self.logger.debug("SharedMemoryManagerV1 initialized with optional DDict storage")

    def create_data_reference(self, task_uid: str, ranks: int) -> DataReferenceV1:
        """Create a zero-copy reference to existing result keys in DDict.

        Creates a reference object that points to
        keys: return_{task_uid}_rank_{i}
        """
        return DataReferenceV1(task_uid, ranks, self.ddict, self.backend_id)

    def cleanup_reference(self, ref: DataReferenceV1):
        """Clean up reference data from DDict."""
        try:
            for rank in range(ref.ranks):
                result_key = f"return_{ref.task_uid}_rank_{rank}"
                if result_key in self.ddict:
                    del self.ddict[result_key]
        except Exception as e:
            self.logger.warning(f"Error cleaning up reference {ref.task_uid}: {e}")

    # DDict operations for direct task data sharing
    def store_task_data(self, key: str, data: Any) -> None:
        """Store data in shared DDict."""
        self.ddict.pput(key, data)

    def get_task_data(self, key: str, default=None) -> Any:
        """Retrieve data from shared DDict."""
        try:
            if key in self.ddict:
                return self.ddict[key]
            return default
        except Exception:
            return default

    def list_task_data_keys(self) -> list:
        """List all keys in the shared DDict."""
        try:
            return list(self.ddict.keys())
        except Exception:
            return []

    def clear_task_data(self, key: str = None) -> None:
        """Clear specific key or all data from shared DDict."""
        try:
            if key:
                if key in self.ddict:
                    del self.ddict[key]
            else:
                self.ddict.clear()
        except Exception as e:
            self.logger.warning(f"Error clearing DDict data: {e}")


class ResultCollectorV1:
    """Unified queue-based result collection for both executable and function tasks."""

    def __init__(
        self,
        shared_memory_manager: SharedMemoryManagerV1,
        result_queue: Queue,
        logger: logging.Logger,
    ):
        self.shared_memory = shared_memory_manager
        self.ddict = shared_memory_manager.ddict
        self.result_queue = result_queue
        self.logger = logger

        # Track completion for all task types
        self.completion_counts: dict[str, int] = {}  # task_uid -> received_count
        self.expected_counts: dict[str, int] = {}  # task_uid -> expected_count
        self.aggregated_results: dict[str, list] = {}  # task_uid -> [completions]

    def register_task(self, task_uid: str, ranks: int):
        """Register any task (executable or function) for result tracking."""
        self.expected_counts[task_uid] = ranks
        self.completion_counts[task_uid] = 0
        self.aggregated_results[task_uid] = []

    def try_consume_result(self) -> Optional[str]:
        """Try to consume one result from queue. Returns task_uid if task completed, None otherwise."""
        try:
            result = self.result_queue.get(block=False)
            if result == "SHUTDOWN":
                return None

            if isinstance(result, (ExecutableTaskCompletionV1, FunctionTaskCompletionV1)):
                return self._process_completion(result)

        except Exception:
            # Queue empty
            pass
        return None

    def _process_completion(self, completion) -> Optional[str]:
        """Process completion and return task_uid if task is now complete."""
        task_uid = completion.task_uid

        if task_uid not in self.expected_counts:
            self.logger.warning(f"Received completion for unregistered task {task_uid}")
            return None

        # Increment completion count
        self.completion_counts[task_uid] += 1
        self.aggregated_results[task_uid].append(completion)

        expected = self.expected_counts[task_uid]
        received = self.completion_counts[task_uid]

        if received >= expected:
            return task_uid

        return None

    def get_completed_task(self, task_uid: str) -> Optional[dict]:
        """Get completed task data and clean up tracking."""
        if task_uid not in self.expected_counts:
            return None

        results = self.aggregated_results.get(task_uid, [])
        if not results:
            return None

        # Sort by rank
        results.sort(key=lambda r: r.rank)

        # Determine if this is function or executable task
        is_function_task = isinstance(results[0], FunctionTaskCompletionV1)

        if is_function_task:
            result_data = self._aggregate_function_results(task_uid, results)
        else:
            result_data = self._aggregate_executable_results(results)

        # Clean up tracking
        self.cleanup_task(task_uid)
        return result_data

    def _aggregate_function_results(
        self, task_uid: str, results: list[FunctionTaskCompletionV1]
    ) -> dict:
        """Aggregate function task results."""
        if len(results) == 1:
            # Single rank - return as-is
            result = results[0]
            return {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "return_value": result.return_value,
                "exception": result.exception,
                "success": result.success,
            }
        else:
            # Multi-rank - aggregate
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in results]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in results]
            max_exit_code = max(r.exit_code for r in results)
            all_successful = all(r.success for r in results)

            # Collect return values
            return_values = [r.return_value for r in results]

            # If any stored in DDict, create single reference for all ranks
            if any(r.stored_in_ddict for r in results):
                return_value = self.shared_memory.create_data_reference(task_uid, len(results))
            else:
                return_value = return_values

            return {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": return_value,
                "exception": None
                if all_successful
                else "; ".join(str(r.exception) for r in results if not r.success),
                "success": all_successful,
            }

    def _aggregate_executable_results(self, results: list[ExecutableTaskCompletionV1]) -> dict:
        """Aggregate executable task results."""
        if len(results) == 1:
            return results[0].to_result_dict()
        else:
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in results]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in results]
            max_exit_code = max(r.exit_code for r in results)

            return {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": None,
                "exception": None if max_exit_code == 0 else "One or more processes failed",
            }

    def cleanup_task(self, task_uid: str):
        """Clean up task tracking data."""
        self.completion_counts.pop(task_uid, None)
        self.expected_counts.pop(task_uid, None)
        self.aggregated_results.pop(task_uid, None)

    def is_task_complete(self, task_uid: str) -> bool:
        """Check if task is complete based on completion counts."""
        if task_uid not in self.expected_counts:
            return False

        expected = self.expected_counts[task_uid]
        received = self.completion_counts.get(task_uid, 0)
        return received >= expected


class TaskLauncherV1:
    """Unified task launching for all task types."""

    def __init__(self, ddict: DDict, result_queue: Queue, working_dir: str, logger: logging.Logger):
        self.ddict = ddict
        self.result_queue = result_queue
        self.working_dir = working_dir
        self.logger = logger

    async def launch_task(self, task: dict) -> TaskInfoV1:
        """Launch any type of task and return TaskInfo."""
        task_type = self._determine_task_type(task)

        if task_type.name.startswith("SINGLE_"):
            return await self._launch_single_task(task, task_type)
        else:
            return await self._launch_group_task(task, task_type)

    def _determine_task_type(self, task: dict) -> TaskTypeV1:
        """Determine task type based on task configuration."""
        is_function = bool(task.get("function"))
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        ranks = int(backend_kwargs.get("ranks", 1))
        mpi = backend_kwargs.get("pmi", None)

        if mpi:
            if ranks < 2:
                raise ValueError("MPI tasks must have ranks > 1")
            return TaskTypeV1.MPI_FUNCTION if is_function else TaskTypeV1.MPI_EXECUTABLE
        if ranks == 1:
            return TaskTypeV1.SINGLE_FUNCTION if is_function else TaskTypeV1.SINGLE_EXECUTABLE
        else:  # ranks > 1 and not MPI
            return TaskTypeV1.MULTI_FUNCTION if is_function else TaskTypeV1.MULTI_EXECUTABLE

    async def _launch_single_task(self, task: dict, task_type: TaskTypeV1) -> TaskInfoV1:
        """Launch single-rank task."""
        uid = task["uid"]

        if task_type == TaskTypeV1.SINGLE_FUNCTION:
            process = await self._create_function_process(task, 0)
        else:
            process = self._create_executable_process(task, 0)

        process.start()
        self.logger.debug(f"Started single-rank Dragon process for task {uid}")

        return TaskInfoV1(task_type=task_type, ranks=1, start_time=time.time(), process=process)

    async def _launch_group_task(self, task: dict, task_type: TaskTypeV1) -> TaskInfoV1:
        """Launch multi-rank or MPI task."""
        uid = task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        ranks = int(backend_kwargs.get("ranks", 2))

        if task_type.name.startswith("MPI_"):
            mpi = backend_kwargs.get("pmi", None)
            if mpi is None:
                raise ValueError("Missing required 'pmi' value in backend_kwargs.")

            group = ProcessGroup(restart=False, policy=None, pmi=mpi)
            self.logger.debug(f"Started MPI group task {uid} ({mpi}) with {ranks} ranks")
        else:
            group = ProcessGroup(restart=False, policy=None)
            self.logger.debug(f"Started group task {uid} with {ranks} ranks")

        if task_type.name.endswith("_FUNCTION"):
            await self._add_function_processes_to_group(group, task, ranks)
        else:
            self._add_executable_processes_to_group(group, task, ranks)

        group.init()
        group.start()

        self.logger.debug(f"Started group task {uid} with {ranks} ranks")

        return TaskInfoV1(task_type=task_type, ranks=ranks, start_time=time.time(), group=group)

    async def _create_function_process(self, task: dict, rank: int) -> Process:
        """Create a single function process."""
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        use_ddict_storage = backend_kwargs.get("use_ddict_storage", False)

        return Process(
            target=_function_wrapper_v1,
            args=(self.result_queue, self.ddict, task, rank, use_ddict_storage),
        )

    def _create_executable_process(self, task: dict, rank: int) -> Process:
        """Create a single executable process."""
        executable = task["executable"]
        args = list(task.get("arguments", []))
        uid = task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        execute_in_shell = backend_kwargs.get("shell", False)

        return Process(
            target=_executable_wrapper_v1,
            args=(self.result_queue, executable, args, uid, rank, self.working_dir, execute_in_shell),
        )

    async def _add_function_processes_to_group(
        self, group: ProcessGroup, task: dict, ranks: int
    ) -> None:
        """Add function processes to process group."""
        task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        use_ddict_storage = backend_kwargs.get("use_ddict_storage", False)

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=_function_wrapper_v1,
                args=(self.result_queue, self.ddict, task, rank, use_ddict_storage),
                env=env,
                cwd=self.working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
            )
            group.add_process(nproc=1, template=template)

    def _add_executable_processes_to_group(
        self, group: ProcessGroup, task: dict, ranks: int
    ) -> None:
        """Add executable processes to process group."""
        executable = task["executable"]
        args = list(task.get("arguments", []))
        uid = task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        execute_in_shell = backend_kwargs.get("shell", False)

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=_executable_wrapper_v1,
                args=(self.result_queue, executable, args, uid, rank, self.working_dir, execute_in_shell),
                env=env,
                cwd=self.working_dir,
            )
            group.add_process(nproc=1, template=template)


def _executable_wrapper_v1(
    result_queue: Queue,
    executable: str,
    args: list,
    task_uid: str,
    rank: int,
    working_dir: str,
    execute_in_shell: bool = False,
):
    """wrapper function that executes executable and pushes completion to queue."""
    import subprocess
    import time

    try:
        # Execute the process and capture output
        if execute_in_shell:
            # Shell mode: join executable and arguments into single command string
            cmd = " ".join([executable] + args)
            result = subprocess.run(
                cmd,
                cwd=working_dir,
                capture_output=True,
                shell=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )
        else:
            # Exec mode: pass executable and arguments separately (no shell)
            result = subprocess.run(
                [executable] + args,
                cwd=working_dir,
                capture_output=True,
                shell=False,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

        # Create completion object
        completion = ExecutableTaskCompletionV1(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=result.stdout,
            stderr=result.stderr,
            exit_code=result.returncode,
            timestamp=time.time(),
        )

        # Push completion directly to queue
        result_queue.put(completion, block=True)

    except subprocess.TimeoutExpired:
        completion = ExecutableTaskCompletionV1(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout="",
            stderr="Process timed out",
            exit_code=124,
            timestamp=time.time(),
        )
        result_queue.put(completion, block=True)

    except Exception as e:
        completion = ExecutableTaskCompletionV1(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout="",
            stderr=f"Execution error: {str(e)}",
            exit_code=1,
            timestamp=time.time(),
        )
        result_queue.put(completion, block=True)


def _function_wrapper_v1(
    result_queue: Queue, ddict: DDict, task: dict, rank: int, use_ddict_storage: bool
):
    """wrapper function that executes user functions and pushes completion to queue.

    DDict is only used when user explicitly sets use_ddict_storage=True.
    Otherwise, return value is sent directly via queue.
    """
    import io
    import traceback

    task_uid = task["uid"]
    os.environ["DRAGON_RANK"] = str(rank)

    # Capture stdout/stderr
    old_out, old_err = sys.stdout, sys.stderr
    out_buf, err_buf = io.StringIO(), io.StringIO()

    function = task["function"]
    args = task.get("args", ())
    kwargs = task.get("kwargs", {})

    try:
        sys.stdout, sys.stderr = out_buf, err_buf

        # Execute function
        if asyncio.iscoroutinefunction(function):
            result = asyncio.run(function(*args, **kwargs))
        else:
            raise RuntimeError("Sync functions are not supported, please define it as async")

        # Store in DDict only if user requested
        stored_in_ddict = False
        return_value_for_queue = result

        if use_ddict_storage:
            result_key = f"return_{task_uid}_rank_{rank}"
            ddict.pput(result_key, result)
            stored_in_ddict = True
            return_value_for_queue = None

        # Create completion and send via queue
        completion = FunctionTaskCompletionV1(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=out_buf.getvalue(),
            stderr=err_buf.getvalue(),
            exit_code=0,
            timestamp=time.time(),
            success=True,
            exception=None,
            traceback=None,
            return_value=return_value_for_queue,
            stored_in_ddict=stored_in_ddict,
        )

    except Exception as e:
        # Error case
        completion = FunctionTaskCompletionV1(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=out_buf.getvalue(),
            stderr=err_buf.getvalue(),
            exit_code=1,
            timestamp=time.time(),
            success=False,
            exception=str(e),
            traceback=traceback.format_exc(),
            return_value=None,
            stored_in_ddict=False,
        )

    finally:
        sys.stdout, sys.stderr = old_out, old_err

        # Send completion to queue
        try:
            result_queue.put(completion, block=True, timeout=30)
        except Exception as queue_error:
            print(f"Failed to send completion to queue: {queue_error}", file=sys.stderr)

        # Detach from DDict if used
        try:
            if stored_in_ddict:
                ddict.detach()
        except Exception:
            pass


# ============================================================================
# V2 Integration Helper Classes
# ============================================================================


class TaskTypeV2(Enum):
    """Enumeration of supported task types."""

    FUNCTION = "function"
    EXECUTABLE = "executable"


class WorkerPinningPolicyV2(Enum):
    """Worker pinning policy for task assignment."""

    STRICT = "strict"  # Wait indefinitely for hinted worker
    SOFT = "soft"  # Wait N seconds, then fallback to any worker
    AFFINITY = "affinity"  # Prefer hinted worker, use others if not immediately available
    EXCLUSIVE = "exclusive"  # Only hinted worker can run, reject if insufficient capacity


class WorkerTypeV2(Enum):
    """Worker type enumeration."""

    COMPUTE = "compute"
    TRAINING = "training"


@dataclass
class WorkerRequestV2:
    """Request sent to worker pool."""

    task_uid: str
    task_type: TaskTypeV2
    rank: int
    total_ranks: int
    gpu_ids: list[int] = field(default_factory=list)
    use_ddict_storage: bool = False
    function: Optional[Callable] = None
    args: tuple = ()
    kwargs: dict = None
    executable: Optional[str] = None
    exec_args: list = None
    working_dir: str = "."
    execute_in_shell: bool = False

    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        if self.exec_args is None:
            self.exec_args = []


@dataclass
class WorkerResponseV2:
    """Response from worker."""

    task_uid: str
    rank: int
    success: bool
    worker_name: str = ""
    return_value: Any = None
    stored_ref_key: Optional[str] = None
    is_reference: bool = False
    stdout: str = ""
    stderr: str = ""
    exception: Optional[str] = None
    exit_code: int = 0
    timestamp: float = 0.0


@dataclass
class TaskInfoV2:
    """Container for task runtime information."""

    task_type: TaskTypeV2
    ranks: int
    start_time: float
    worker_name: str = ""
    gpu_allocations: dict[int, list[int]] = field(default_factory=dict)
    canceled: bool = False
    completed_ranks: int = 0


@dataclass
class PolicyConfigV2:
    """Configuration for a single policy.

    For COMPUTE workers: nprocs MUST be specified
    For TRAINING workers: nprocs MUST be None (omitted)
    """

    policy: Optional[Policy] = None
    nprocs: Optional[int] = None
    ngpus: int = 0


@dataclass
class WorkerGroupConfigV2:
    """Unified configuration for worker groups.

    Attributes:
        name: Worker group name
        worker_type: COMPUTE or TRAINING
        policies: List of PolicyConfig objects
        kwargs: Additional config (for training: passed to configure_training_group)

    Examples:
        # Compute worker
        WorkerGroupConfigV2(
            name="compute",
            worker_type=WorkerTypeV2.COMPUTE,
            policies=[PolicyConfigV2(policy=p, nprocs=128, ngpus=2)]
        )

        # Training worker
        WorkerGroupConfigV2(
            name="training",
            worker_type=WorkerTypeV2.TRAINING,
            policies=[PolicyConfigV2(policy=p1), PolicyConfigV2(policy=p2)],
            kwargs={'nprocs': 2, 'ppn': 32}
        )
    """

    name: str
    worker_type: WorkerTypeV2 = WorkerTypeV2.COMPUTE
    policies: list[PolicyConfigV2] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration."""
        if self.worker_type == WorkerTypeV2.TRAINING:
            for i, pc in enumerate(self.policies):
                if pc.nprocs is not None:
                    raise ValueError(
                        f"Training worker '{self.name}': PolicyConfig[{i}] must NOT specify nprocs"
                    )
        else:  # COMPUTE
            for i, pc in enumerate(self.policies):
                if pc.nprocs is None:
                    raise ValueError(
                        f"Compute worker '{self.name}': PolicyConfig[{i}] must specify nprocs"
                    )

    def total_slots(self) -> int:
        """Total CPU slots."""
        if self.worker_type == WorkerTypeV2.TRAINING:
            if "nprocs" in self.kwargs:
                return int(self.kwargs["nprocs"])
            return len(self.policies)
        else:
            return sum(p.nprocs for p in self.policies)

    def total_gpus(self) -> int:
        """Total GPUs."""
        if self.worker_type == WorkerTypeV2.TRAINING:
            return 0  # Training workers manage GPUs via policies
        else:
            return sum(p.ngpus for p in self.policies)


class DataReferenceV2:
    """Reference to data stored in Cross Node Distributed Dict."""

    def __init__(
        self, ref_id: str, backend_id: str, ddict: DDict, rank_info: Optional[dict] = None
    ):
        self._ref_id = ref_id
        self._backend_id = backend_id
        self._ddict = ddict
        self._rank_info = rank_info

    @property
    def ref_id(self) -> str:
        return self._ref_id

    @property
    def backend_id(self) -> str:
        return self._backend_id

    def resolve(self) -> Any:
        """Resolve reference to actual data."""
        if self._rank_info is None:
            data_key = f"data_{self._ref_id}"
            if data_key not in self._ddict:
                raise KeyError(f"Reference data not found: {self._ref_id}")
            return self._ddict[data_key]
        else:
            rank_keys = self._rank_info["rank_keys"]
            results = []
            for ref_key in rank_keys:
                if ref_key is None:
                    results.append(None)
                else:
                    data_key = f"data_{ref_key}"
                    if data_key in self._ddict:
                        results.append(self._ddict[data_key])
                    else:
                        results.append(None)
            return results

    def __repr__(self) -> str:
        if self._rank_info:
            return f"DataReferenceV2(ref_id='{self._ref_id}', backend_id='{self._backend_id}', ranks={len(self._rank_info['rank_keys'])})"
        return f"DataReferenceV2(ref_id='{self._ref_id}', backend_id='{self._backend_id}')"


class SharedMemoryManagerV2:
    """Manages data storage using DDict."""

    def __init__(
        self,
        ddict: DDict,
        system: System,
        logger: logging.Logger,
        reference_threshold: int = DRAGON_DEFAULT_REF_THRESHOLD,
    ):
        self.ddict = ddict
        self.system = system
        self.logger = logger
        self.backend_id = f"dragon_{uuid.uuid4().hex[:8]}"
        self.reference_threshold = reference_threshold

    async def initialize(self):
        """Initialize storage manager."""
        self.logger.debug(
            f"SharedMemoryManagerV2 initialized (threshold: {self.reference_threshold} bytes)"
        )

    async def store_data(self, data: Any, node_id: int = 0) -> DataReferenceV2:
        """Store data in DDict and return reference."""
        ref_id = f"ref_{uuid.uuid4().hex}"
        try:
            self._store_in_ddict(ref_id, data)
            self.logger.debug(f"Stored data {ref_id} in DDict")
        except Exception as e:
            self.logger.error(f"Failed to store data {ref_id}: {e}")
            raise
        return DataReferenceV2(ref_id, self.backend_id, self.ddict)

    def _store_in_ddict(self, ref_id: str, data: Any):
        """Store data in DDict."""
        self.ddict.pput(f"data_{ref_id}", data)
        self.ddict.pput(f"meta_{ref_id}", {"backend_id": self.backend_id, "stored_at": time.time()})

    def cleanup_reference(self, ref: DataReferenceV2):
        """Clean up reference data."""
        try:
            for key in [f"meta_{ref.ref_id}", f"data_{ref.ref_id}"]:
                if key in self.ddict:
                    del self.ddict[key]
        except Exception as e:
            self.logger.warning(f"Error cleaning up reference {ref.ref_id}: {e}")


def _worker_loop_v2(
    worker_id: int,
    worker_name: str,
    input_queue: Queue,
    output_queue: Queue,
    ddict: DDict,
    working_dir: str,
) -> None:
    """Persistent worker that processes tasks from queue.

    Handles both function and executable tasks:
    - Functions: Execute Python callables
    - Executables: Run external processes via subprocess
    """
    import io
    import subprocess
    import traceback

    os.environ["DRAGON_WORKER_ID"] = str(worker_id)
    os.environ["DRAGON_WORKER_NAME"] = worker_name

    backend_id = os.environ.get("DRAGON_BACKEND_ID", f"dragon_{uuid.uuid4().hex[:8]}")
    dragon_cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    # Map Dragon training env vars to PyTorch standard names
    if "DRAGON_PG_RANK" in os.environ:
        os.environ["RANK"] = os.environ["DRAGON_PG_RANK"]
    if "DRAGON_PG_LOCAL_RANK" in os.environ:
        os.environ["LOCAL_RANK"] = os.environ["DRAGON_PG_LOCAL_RANK"]
    if "DRAGON_PG_WORLD_SIZE" in os.environ:
        os.environ["WORLD_SIZE"] = os.environ["DRAGON_PG_WORLD_SIZE"]
    if "DRAGON_PG_MASTER_ADDR" in os.environ and "MASTER_ADDR" not in os.environ:
        os.environ["MASTER_ADDR"] = os.environ["DRAGON_PG_MASTER_ADDR"]
    if "DRAGON_PG_MASTER_PORT" in os.environ and "MASTER_PORT" not in os.environ:
        os.environ["MASTER_PORT"] = os.environ["DRAGON_PG_MASTER_PORT"]

    try:
        while True:
            # Get task from queue (blocking)
            request = input_queue.get()

            # Shutdown signal
            if request is None:
                break

            if not isinstance(request, WorkerRequestV2):
                continue

            # Set rank environment variable for the task
            os.environ["DRAGON_RANK"] = str(request.rank)

            # Set GPU visibility for this rank
            if request.gpu_ids:
                os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, request.gpu_ids))
            elif dragon_cuda_visible is not None:
                os.environ["CUDA_VISIBLE_DEVICES"] = dragon_cuda_visible

            response = WorkerResponseV2(
                task_uid=request.task_uid,
                rank=request.rank,
                worker_name=worker_name,
                success=False,
                timestamp=time.time(),
            )

            try:
                if request.task_type == TaskTypeV2.FUNCTION:
                    # Execute Python function
                    old_out, old_err = sys.stdout, sys.stderr
                    out_buf, err_buf = io.StringIO(), io.StringIO()

                    try:
                        sys.stdout, sys.stderr = out_buf, err_buf

                        # Handle async functions
                        if asyncio.iscoroutinefunction(request.function):
                            result = asyncio.run(request.function(*request.args, **request.kwargs))
                        else:
                            result = request.function(*request.args, **request.kwargs)

                        if result is not None and request.use_ddict_storage:
                            ref_key = f"return_{request.task_uid}_rank_{request.rank}"
                            try:
                                ddict.pput(f"data_{ref_key}", result)
                                ddict.pput(
                                    f"meta_{ref_key}",
                                    {"backend_id": backend_id, "stored_at": time.time()},
                                )
                                response.stored_ref_key = ref_key
                                response.is_reference = True
                                response.return_value = None
                            except Exception as store_error:
                                raise RuntimeError(
                                    f"Failed to store return value in DDict: {store_error}"
                                )
                        else:
                            response.return_value = result
                            response.is_reference = False

                        response.success = True
                        response.stdout = out_buf.getvalue()
                        response.stderr = err_buf.getvalue()
                        response.exit_code = 0

                    except Exception as e:
                        response.success = False
                        response.exception = str(e)
                        response.stderr = err_buf.getvalue() + "\n" + traceback.format_exc()
                        response.exit_code = 1

                    finally:
                        sys.stdout, sys.stderr = old_out, old_err

                elif request.task_type == TaskTypeV2.EXECUTABLE:
                    # Execute external process
                    if request.execute_in_shell:
                        # Shell mode: join executable and arguments into single command string
                        cmd = " ".join([request.executable] + request.exec_args)
                        result = subprocess.run(
                            cmd,
                            cwd=request.working_dir,
                            capture_output=True,
                            text=True,
                            shell=True,
                            timeout=3600,  # FIXME: should be user defined
                        )
                    else:
                        # Exec mode: pass executable and arguments separately (no shell)
                        result = subprocess.run(
                            [request.executable] + request.exec_args,
                            cwd=request.working_dir,
                            capture_output=True,
                            text=True,
                            shell=False,
                            timeout=3600,  # FIXME: should be user defined
                        )

                    response.success = result.returncode == 0
                    response.stdout = result.stdout
                    response.stderr = result.stderr
                    response.exit_code = result.returncode
                    if result.returncode != 0:
                        response.exception = f"Process exited with code {result.returncode}"

            except subprocess.TimeoutExpired:
                response.exception = "Process timed out"
                response.exit_code = 124
                response.stderr = "Process timed out"

            except Exception as e:
                response.exception = f"Worker error: {str(e)}"
                response.exit_code = 1
                response.stderr = traceback.format_exc()

            # Send response back
            output_queue.put(response)

    except Exception as e:
        print(f"Worker {worker_id} ({worker_name}) fatal error: {e}")
        raise
    finally:
        try:
            ddict.detach()
        except Exception:
            pass


class WorkerPoolV2:
    """Manages persistent worker pool with per-worker queues and slot reservation.

    Key features:
    - Each worker group has its own input queue
    - Slot tracking per worker for load balancing
    - Tasks are assigned to workers with sufficient free slots
    """

    def __init__(
        self,
        worker_configs: list[WorkerGroupConfigV2],
        ddict: DDict,
        working_dir: str,
        logger: logging.Logger,
        system: System,
        backend_id: str,
    ):
        self.worker_configs = worker_configs
        self.ddict = ddict
        self.working_dir = working_dir
        self.logger = logger
        self.system = system
        self.backend_id = backend_id

        self.output_queue: Optional[Queue] = None
        self.worker_queues: dict[str, Queue] = {}
        # CPU slot tracking
        self.worker_slots: dict[str, int] = {}
        self.worker_free_slots: dict[str, int] = {}
        # GPU tracking
        self.worker_gpus: dict[str, int] = {}
        self.worker_free_gpus: dict[str, list[int]] = {}
        self.worker_types: dict[str, WorkerTypeV2] = {}

        self.process_groups: list[ProcessGroup] = []
        self.total_workers = len(worker_configs)
        self.total_slots = sum(cfg.total_slots() for cfg in worker_configs)
        self.total_gpus = sum(cfg.total_gpus() for cfg in worker_configs)
        self.initialized = False

        # Thread-safe resource management
        self._resource_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize worker pool."""
        if self.initialized:
            return

        try:
            # Single shared output queue
            self.output_queue = Queue()

            worker_id = 0

            for worker_config in self.worker_configs:
                worker_name = worker_config.name
                worker_type = worker_config.worker_type

                # Create dedicated input queue for this worker
                input_queue = Queue()
                self.worker_queues[worker_name] = input_queue
                self.worker_types[worker_name] = worker_type

                # Track CPU slots
                total_worker_slots = worker_config.total_slots()
                self.worker_slots[worker_name] = total_worker_slots
                self.worker_free_slots[worker_name] = total_worker_slots

                # Track GPUs - assign sequential IDs
                total_worker_gpus = worker_config.total_gpus()
                self.worker_gpus[worker_name] = total_worker_gpus
                self.worker_free_gpus[worker_name] = list(range(total_worker_gpus))

                if worker_type == WorkerTypeV2.TRAINING:
                    # Training worker: extract policies and pass kwargs to configure_training_group
                    config_kwargs = {
                        "training_fn": _worker_loop_v2,
                        "training_args": (
                            worker_id,
                            worker_name,
                            input_queue,
                            self.output_queue,
                            self.ddict,
                            self.working_dir,
                        ),
                        "policies": [pc.policy for pc in worker_config.policies],
                    }
                    config_kwargs.update(worker_config.kwargs)

                    self.logger.info(
                        f"Creating training worker '{worker_name}' with {total_worker_slots} slots"
                    )

                    process_group = ProcessGroup.configure_training_group(**config_kwargs)
                    process_group.init()
                    process_group.start()
                    self.process_groups.append(process_group)

                else:
                    # Compute worker: use PolicyConfig.nprocs
                    process_group = ProcessGroup(restart=False)

                    for policy_config in worker_config.policies:
                        env = os.environ.copy()
                        env["DRAGON_WORKER_ID"] = str(worker_id)
                        env["DRAGON_WORKER_NAME"] = worker_name
                        env["DRAGON_BACKEND_ID"] = self.backend_id

                        template = ProcessTemplate(
                            target=_worker_loop_v2,
                            args=(
                                worker_id,
                                worker_name,
                                input_queue,
                                self.output_queue,
                                self.ddict,
                                self.working_dir,
                            ),
                            env=env,
                            cwd=self.working_dir,
                            policy=policy_config.policy,
                        )

                        process_group.add_process(nproc=policy_config.nprocs, template=template)

                    process_group.init()
                    process_group.start()
                    self.process_groups.append(process_group)

                worker_id += 1

            self.initialized = True

            config_summary = []
            for cfg in self.worker_configs:
                config_summary.append(
                    f"{cfg.name} ({cfg.worker_type.value}): {cfg.total_slots()} slots, {cfg.total_gpus()} GPUs"
                )

            self.logger.info(
                f"Worker pool initialized: {self.total_workers} workers, "
                f"{self.total_slots} total slots, {self.total_gpus} total GPUs. "
                f"Config: {'; '.join(config_summary)}"
            )

        except Exception as e:
            self.logger.exception(f"Failed to initialize worker pool: {e}")
            raise

    def find_worker_for_task(
        self,
        ranks: int,
        gpus_per_rank: int = 0,
        preferred_worker: Optional[str] = None,
        worker_type_hint: Optional[str] = None,
    ) -> Optional[str]:
        """Find worker with sufficient capacity using least-loaded strategy."""
        total_gpus_needed = ranks * gpus_per_rank

        # Check preferred worker first
        if preferred_worker and preferred_worker in self.worker_free_slots:
            worker_type = self.worker_types.get(preferred_worker)
            if worker_type == WorkerTypeV2.TRAINING:
                if self.worker_free_slots[preferred_worker] >= ranks:
                    return preferred_worker
            else:
                if (
                    self.worker_free_slots[preferred_worker] >= ranks
                    and len(self.worker_free_gpus[preferred_worker]) >= total_gpus_needed
                ):
                    return preferred_worker

        # Find all eligible workers and pick the least loaded one
        eligible_workers = []

        for worker_name in self.worker_free_slots.keys():
            # Filter by worker type if specified
            if worker_type_hint:
                try:
                    target_type = WorkerTypeV2(worker_type_hint.lower())
                    if self.worker_types.get(worker_name) != target_type:
                        continue
                except ValueError:
                    pass

            worker_type = self.worker_types.get(worker_name)

            # Check if worker has sufficient capacity
            if worker_type == WorkerTypeV2.TRAINING:
                if self.worker_free_slots[worker_name] >= ranks:
                    eligible_workers.append((worker_name, self.worker_free_slots[worker_name]))
            else:
                if (
                    self.worker_free_slots[worker_name] >= ranks
                    and len(self.worker_free_gpus[worker_name]) >= total_gpus_needed
                ):
                    eligible_workers.append((worker_name, self.worker_free_slots[worker_name]))

        # Return the worker with the most free slots (least loaded)
        if eligible_workers:
            # Sort by free slots (descending) to get least loaded worker
            eligible_workers.sort(key=lambda x: x[1], reverse=True)
            return eligible_workers[0][0]

        return None

    def worker_has_capacity(self, worker_name: str, ranks: int, gpus_per_rank: int = 0) -> bool:
        """Check if worker has capacity."""
        if worker_name not in self.worker_free_slots:
            return False

        worker_type = self.worker_types.get(worker_name)
        if worker_type == WorkerTypeV2.TRAINING:
            return self.worker_free_slots[worker_name] >= ranks

        total_gpus_needed = ranks * gpus_per_rank
        return (
            self.worker_free_slots[worker_name] >= ranks
            and len(self.worker_free_gpus[worker_name]) >= total_gpus_needed
        )

    def worker_exists(self, worker_name: str) -> bool:
        """Check if worker exists."""
        return worker_name in self.worker_slots

    async def reserve_resources(
        self, worker_name: str, ranks: int, gpus_per_rank: int = 0
    ) -> tuple[bool, dict[int, list[int]]]:
        """Reserve resources (thread-safe)."""
        async with self._resource_lock:
            if worker_name not in self.worker_free_slots:
                return False, {}

            worker_type = self.worker_types.get(worker_name)

            if worker_type == WorkerTypeV2.TRAINING:
                if self.worker_free_slots[worker_name] < ranks:
                    return False, {}

                self.worker_free_slots[worker_name] -= ranks
                return True, {}

            total_gpus_needed = ranks * gpus_per_rank
            if (
                self.worker_free_slots[worker_name] < ranks
                or len(self.worker_free_gpus[worker_name]) < total_gpus_needed
            ):
                return False, {}

            self.worker_free_slots[worker_name] -= ranks

            gpu_allocations = {}
            if gpus_per_rank > 0:
                for rank in range(ranks):
                    allocated_gpus = []
                    for _ in range(gpus_per_rank):
                        gpu_id = self.worker_free_gpus[worker_name].pop(0)
                        allocated_gpus.append(gpu_id)
                    gpu_allocations[rank] = allocated_gpus

            return True, gpu_allocations

    async def release_resources(
        self, worker_name: str, ranks: int, gpu_allocations: dict[int, list[int]]
    ):
        """Release resources (thread-safe)."""
        async with self._resource_lock:
            if worker_name in self.worker_free_slots:
                self.worker_free_slots[worker_name] += ranks

                worker_type = self.worker_types.get(worker_name)
                if worker_type != WorkerTypeV2.TRAINING:
                    for rank_gpus in gpu_allocations.values():
                        self.worker_free_gpus[worker_name].extend(rank_gpus)
                    self.worker_free_gpus[worker_name].sort()

    def submit_request(self, worker_name: str, request: WorkerRequestV2):
        """Submit task request to worker."""
        if not self.initialized:
            raise RuntimeError("Worker pool not initialized")

        if worker_name not in self.worker_queues:
            raise ValueError(f"Unknown worker: {worker_name}")

        try:
            self.worker_queues[worker_name].put(request, timeout=10)
        except Exception as e:
            self.logger.error(f"Failed to submit request to {worker_name}: {e}")
            raise

    def try_get_response(self) -> Optional[WorkerResponseV2]:
        """Try to get response from output queue."""
        try:
            return self.output_queue.get(block=False)
        except Exception:
            return None

    async def shutdown(self):
        """Shutdown worker pool gracefully."""
        if not self.initialized:
            return

        try:
            self.logger.info("Initiating worker pool shutdown...")

            # Send shutdown signal (None) to each worker process
            # Each worker has multiple processes, so we need to send one None per process
            for worker_name, input_queue in self.worker_queues.items():
                worker_slots = self.worker_slots[worker_name]
                self.logger.debug(f"Sending {worker_slots} shutdown signals to {worker_name}")
                for _ in range(worker_slots):
                    try:
                        input_queue.put(None, timeout=1.0)
                    except Exception as e:
                        self.logger.warning(f"Failed to send shutdown signal to {worker_name}: {e}")

            # Give processes time to finish current work and exit cleanly
            await asyncio.sleep(0.5)

            # Join and stop all process groups
            for idx, process_group in enumerate(self.process_groups):
                try:
                    self.logger.debug(f"Stopping ProcessGroup {idx}")

                    # Join first to wait for processes to exit
                    try:
                        process_group.join(timeout=5.0)
                        self.logger.debug(f"ProcessGroup {idx} joined successfully")
                    except Exception as e:
                        self.logger.warning(f"ProcessGroup {idx} join timeout or error: {e}")

                    # Then stop and close
                    try:
                        process_group.stop()
                        self.logger.debug(f"ProcessGroup {idx} stopped")
                    except Exception as e:
                        self.logger.debug(
                            f"ProcessGroup {idx} stop error (may already be stopped): {e}"
                        )

                    try:
                        process_group.close()
                        self.logger.debug(f"ProcessGroup {idx} closed")
                    except Exception as e:
                        self.logger.debug(f"ProcessGroup {idx} close error: {e}")

                except DragonUserCodeError as e:
                    self.logger.debug(f"ProcessGroup {idx} user code error during shutdown: {e}")
                except Exception as e:
                    self.logger.warning(f"Error stopping ProcessGroup {idx}: {e}")

            self.logger.info("Worker pool shutdown complete")

        except Exception as e:
            self.logger.exception(f"Error shutting down worker pool: {e}")
        finally:
            self.initialized = False
            self.process_groups.clear()
            self.worker_queues.clear()


class ResultCollectorV2:
    """Collects and aggregates results from worker pool."""

    def __init__(self, shared_memory_manager: SharedMemoryManagerV2, logger: logging.Logger):
        self.shared_memory = shared_memory_manager
        self.logger = logger

        # Track task completions
        self.task_responses: dict[str, list[WorkerResponseV2]] = {}
        self.task_expected: dict[str, int] = {}

    def register_task(self, task_uid: str, ranks: int):
        """Register a task for result tracking."""
        self.task_expected[task_uid] = ranks
        self.task_responses[task_uid] = []

    def process_response(self, response: WorkerResponseV2) -> Optional[str]:
        """Process worker response."""
        task_uid = response.task_uid

        if task_uid not in self.task_expected:
            self.logger.warning(f"Received response for unregistered task {task_uid}")
            return None

        self.task_responses[task_uid].append(response)

        if len(self.task_responses[task_uid]) >= self.task_expected[task_uid]:
            return task_uid

        return None

    async def get_task_result(self, task_uid: str) -> Optional[dict]:
        """Get aggregated task result."""
        if task_uid not in self.task_responses:
            return None

        responses = self.task_responses[task_uid]
        responses.sort(key=lambda r: r.rank)

        if len(responses) == 1:
            r = responses[0]

            if r.is_reference:
                final_return = DataReferenceV2(
                    ref_id=r.stored_ref_key,
                    backend_id=self.shared_memory.backend_id,
                    ddict=self.shared_memory.ddict,
                )
            else:
                final_return = r.return_value

            result = {
                "stdout": r.stdout,
                "stderr": r.stderr,
                "exit_code": r.exit_code,
                "return_value": final_return,
                "exception": r.exception,
            }
        else:
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in responses]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in responses]
            max_exit_code = max(r.exit_code for r in responses)
            all_successful = all(r.success for r in responses)

            has_any_reference = any(r.is_reference for r in responses if r.success)

            if has_any_reference:
                rank_keys = []
                for r in responses:
                    if r.success and r.is_reference:
                        rank_keys.append(r.stored_ref_key)
                    else:
                        rank_keys.append(None)

                final_return = DataReferenceV2(
                    ref_id=f"unified_{task_uid}",
                    backend_id=self.shared_memory.backend_id,
                    ddict=self.shared_memory.ddict,
                    rank_info={"task_uid": task_uid, "rank_keys": rank_keys},
                )
            else:
                return_values = [r.return_value for r in responses]
                final_return = return_values[0] if len(return_values) == 1 else return_values

            result = {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": final_return,
                "exception": None
                if all_successful
                else "; ".join(r.exception for r in responses if r.exception),
            }

        # Cleanup
        self.cleanup_task(task_uid)
        return result

    def cleanup_task(self, task_uid: str):
        """Clean up task tracking."""
        self.task_responses.pop(task_uid, None)
        self.task_expected.pop(task_uid, None)


# ============================================================================
# V3 Integration Helper Classes
# ============================================================================


class TaskStateMapperV3:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    terminal_states = {DONE, FAILED, CANCELED}


# ============================================================================
# Main Backend Classes
# V1 Integrates with Dragon HPC Native API
# V2 Integrates with Dragon API and builds Compute and Training Workers
# V3 Integrates with Dragon.Batch API (Most performant)
# ============================================================================


class DragonExecutionBackendV1(BaseExecutionBackend):
    """Dragon execution backend with unified queue-based architecture

                ┌────────────────────────────┐
                │  DRAGON EXECUTION BACKEND  │
                └────────────────────────────┘
                              |
                       ┌──────────────┐
                       │ TaskLauncher │
                       └──────┬───────┘
              ┌────────────-──┴──────────────┐
              ▼                              ▼
     ┌────────────────────┐       ┌────────────────────┐
     │ Executable Tasks   │       │ Function Tasks     │
     │ _executable_wrapper │       │ _function_wrapper   │
     └──────────┬─────────┘       └──────────┬─────────┘
                ▼                            ▼
     ┌──────────────────────────────────────────┐
     │         DRAGON QUEUE (Unified)           │
     │  ExecutableCompletion | FunctionCompletion │
     └──────────────────┬───────────────────────┘
                        ▼
              ┌───────────────────┐
              │  ResultCollector  │
              │   (Unified Logic) │
              └─────────┬─────────┘
                        ▼
           ┌────────────┴────────────┐
           ▼                         ▼
    ┌──────────────┐      ┌──────────────────┐
    │ Small Results│      │ Large Results    │
    │ (via Queue)  │      │ (DDict optional) │
    └──────────────┘      └──────────────────┘
                        ▼
              ┌──────────────┐
              │ DataReference│
              │  .resolve()  │
              └──────────────┘

    Characteristics:
    - Single unified queue for all tasks
    - DDict only for large results or user-requested storage
    - Consistent result collection pattern
    - Lower latency for small function results
    """

    @typeguard.typechecked
    def __init__(self,
     resources: Optional[dict] = None,
     ddict: Optional[DDict] = None,
     name: Optional[str] = "dragon"):

        if dragon is None:
            raise ImportError("Dragon is required for DragonExecutionBackendV1.")
        if DDict is None:
            raise ImportError("Dragon DDict is required for this backend version.")
        if System is None:
            raise ImportError("Dragon System is required for this backend version.")

        super().__init__(name=name)

        self.logger = _get_logger()
        self.tasks: dict[str, dict[str, Any]] = {}
        self._callback_func: Callable = lambda t, s: None
        self._resources = resources or {}
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED

        # Resource management
        self._slots: int = int(self._resources.get("slots", mp.cpu_count() or 1))
        self._free_slots: int = self._slots
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())

        # Task tracking
        self._running_tasks: dict[str, TaskInfoV1] = {}

        # Dragon components
        self._ddict: Optional[DDict] = ddict
        self._system_alloc: Optional[System] = None
        self._result_queue: Optional[Queue] = None

        # Shared memory manager
        self._shared_memory: Optional[SharedMemoryManagerV1] = None

        # Utilities
        self._result_collector: Optional[ResultCollectorV1] = None
        self._task_launcher: Optional[TaskLauncherV1] = None

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    # --------------------------- Lifecycle ---------------------------
    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components
        """
        if not self._initialized:
            try:
                self.logger.debug("Starting Dragon backend V1 async initialization...")

                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                # Step 3: Set backend state to INITIALIZED
                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                # Step 4: Initialize backend components
                await self._initialize()
                self._initialized = True
                self.logger.info("Dragon backend V1 fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dragon backend V1 initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self) -> None:
        try:
            self.logger.debug("Initializing Dragon backend V1 with unified queue architecture...")
            await self._initialize_dragon()

            # Initialize system allocation
            self.logger.debug("Creating System allocation...")
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            self.logger.debug(f"System allocation created with {nnodes} nodes")

            # Initialize DDict (optional - only for large results)
            self.logger.debug("Creating DDict")
            if not self._ddict:
                self._ddict = DDict(
                    n_nodes=nnodes,
                    total_mem=nnodes * int(4 * 1024 * 1024 * 1024),  # 4GB per node
                    wait_for_keys=True,
                    working_set_size=4,
                    timeout=200,
                )
            self.logger.debug("DDict created successfully")

            # Initialize global result queue (unified for all task types)
            self.logger.debug("Creating unified result queue...")
            self._result_queue = Queue()
            self.logger.debug("Result queue created successfully")

            # Initialize shared memory manager
            self._shared_memory = SharedMemoryManagerV1(self._ddict, self._system_alloc, self.logger)
            await self._shared_memory.initialize()

            # Initialize utilities
            self._result_collector = ResultCollectorV1(
                self._shared_memory, self._result_queue, self.logger
            )
            self._task_launcher = TaskLauncherV1(
                self._ddict, self._result_queue, self._working_dir, self.logger
            )

            # Start task monitoring
            self.logger.debug("Starting unified task monitoring...")
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            await asyncio.sleep(0.1)

            self.logger.info(
                f"Dragon backend V1 initialized with {self._slots} slots, "
                f"unified queue architecture"
            )
        except Exception as e:
            self.logger.exception(f"Failed to initialize Dragon backend V1: {str(e)}")
            raise

    async def _initialize_dragon(self):
        """Ensure start method is 'dragon' and proceed."""
        try:
            current_method = mp.get_start_method()
            self.logger.debug(f"Current multiprocessing start method: {current_method}")
            if current_method != "dragon":
                mp.set_start_method("dragon", force=True)
        except RuntimeError:
            pass
        self.logger.debug("Dragon backend V1 active with unified queue-based architecture.")

    def get_task_states_map(self):
        return StateMapper(backend=self)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        self._ensure_initialized()

        # Set backend state to RUNNING when tasks are submitted
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            # Validate task
            is_valid, error_msg = self._validate_task(task)
            if not is_valid:
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue

            self.tasks[task["uid"]] = task

            try:
                await self._submit_task(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _submit_task(self, task: dict[str, Any]) -> None:
        """Submit a single task for execution."""
        uid = task["uid"]
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        ranks = int(backend_kwargs.get("ranks", 1))

        # Wait for available slots
        while self._free_slots < ranks:
            self.logger.debug(f"Waiting for {ranks} slots for task {uid}, {self._free_slots} free")
            await asyncio.sleep(0.1)

        self._free_slots -= ranks

        try:
            # Register all tasks with unified result collector
            self._result_collector.register_task(uid, ranks)

            # Launch task using unified launcher
            task_info = await self._task_launcher.launch_task(task)
            self._running_tasks[uid] = task_info
            self._callback_func(task, "RUNNING")

        except Exception:
            self._free_slots += ranks
            raise

    async def _monitor_tasks(self) -> None:
        """Monitor running tasks with unified queue consumption."""
        self.logger.debug("Monitor task started")
        while not self._shutdown_event.is_set():
            try:
                # Batch consume queue results (unified for all task types)
                completed_tasks = []

                for _ in range(1000):  # Process up to 1000 results per iteration
                    completed_task_uid = self._result_collector.try_consume_result()
                    if completed_task_uid:
                        completed_tasks.append(completed_task_uid)
                    else:
                        break

                # Process all completed tasks
                for uid in completed_tasks:
                    if uid in self._running_tasks:
                        task_info = self._running_tasks[uid]
                        task = self.tasks.get(uid)

                        if task:
                            # Get aggregated results from collector
                            result_data = self._result_collector.get_completed_task(uid)
                            if result_data:
                                task.update(result_data)

                            # Determine task status and notify callback
                            if task.get("canceled", False):
                                self._callback_func(task, "CANCELED")
                            elif task.get("exception") or task.get("exit_code", 0) != 0:
                                self._callback_func(task, "FAILED")
                            else:
                                self._callback_func(task, "DONE")

                        # Free up slots
                        self._free_slots += task_info.ranks

                        # Remove from running tasks
                        self._running_tasks.pop(uid, None)

                await asyncio.sleep(0.01)  # Short sleep for responsiveness

            except Exception as e:
                self.logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a specific running task with proper cleanup."""
        self._ensure_initialized()

        task_info = self._running_tasks.get(uid)
        if not task_info:
            return False

        try:
            success = await self._cancel_task_by_info(task_info)

            if success:
                task_info.canceled = True

                # Clean up result collector tracking
                self._result_collector.cleanup_task(uid)

                # Clean up data references if stored in DDict
                try:
                    task_result = self.tasks.get(uid, {}).get("return_value")
                    if isinstance(task_result, DataReferenceV1):
                        self._shared_memory.cleanup_reference(task_result)
                except Exception as e:
                    self.logger.warning(f"Error cleaning up references for task {uid}: {e}")

            return success

        except Exception as e:
            self.logger.exception(f"Error cancelling task {uid}: {e}")
            return False

    async def _cancel_task_by_info(self, task_info: TaskInfoV1) -> bool:
        """Cancel task based on TaskInfo."""
        proc, group = task_info.process, task_info.group

        if proc:
            if proc.is_alive:
                proc.terminate()
                proc.join(2.0)
                if proc.is_alive:
                    proc.kill()
                    proc.join(1.0)
            return True

        if group and not group.inactive_puids:
            try:
                group.stop()
                group.close()
            except DragonUserCodeError:
                pass
            return True
        return False

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        self._ensure_initialized()
        canceled = 0
        for task_uid in list(self._running_tasks.keys()):
            try:
                if await self.cancel_task(task_uid):
                    canceled += 1
            except Exception:
                pass
        return canceled

    def _validate_task(self, task: dict) -> tuple[bool, str]:
        """Validate task configuration before submission."""
        uid = task.get("uid")
        if not uid:
            return False, "Task must have a 'uid' field"

        function = task.get("function")
        executable = task.get("executable")

        if not function and not executable:
            return False, "Task must specify either 'function' or 'executable'"

        if function and executable:
            return False, "Task cannot specify both 'function' and 'executable'"

        if function and not callable(function):
            return False, "Task 'function' must be callable"

        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        ranks = backend_kwargs.get("ranks", 1)

        try:
            ranks = int(ranks)
            if ranks < 1:
                return False, "Task 'ranks' must be >= 1"
        except (ValueError, TypeError):
            return False, "Task 'ranks' must be a valid integer"

        return True, ""

    def _ensure_initialized(self):
        """Ensure backend is properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DragonExecutionBackendV1 must be awaited before use. "
                "Use: backend = await DragonExecutionBackendV1(resources)"
            )

    def get_ddict(self) -> DDict:
        """Get the shared DDict for cross-task data sharing."""
        self._ensure_initialized()
        return self._ddict

    def get_result_queue(self) -> Queue:
        """Get the global result queue."""
        self._ensure_initialized()
        return self._result_queue

    # Data management methods delegated to SharedMemoryManager
    def store_task_data(self, key: str, data: Any) -> None:
        """Store data in shared DDict for cross-task access."""
        self._ensure_initialized()
        self._shared_memory.store_task_data(key, data)

    def get_task_data(self, key: str, default=None) -> Any:
        """Retrieve data from shared DDict."""
        self._ensure_initialized()
        return self._shared_memory.get_task_data(key, default)

    def list_task_data_keys(self) -> list:
        """List all keys in the shared DDict."""
        self._ensure_initialized()
        return self._shared_memory.list_task_data_keys()

    def clear_task_data(self, key: str = None) -> None:
        """Clear specific key or all data from shared DDict."""
        self._ensure_initialized()
        self._shared_memory.clear_task_data(key)

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Link explicit data dependencies between tasks."""
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks."""
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    async def task_state_cb(self, task: dict, state: str) -> None:
        """Task state callback."""
        pass

    async def build_task(self, task: dict) -> None:
        """Build task."""
        pass

    async def shutdown(self) -> None:
        """Shutdown with proper cleanup."""
        if not self._initialized:
            return

        try:
            # Set backend state to SHUTDOWN
            self._backend_state = BackendMainStates.SHUTDOWN
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

            self._shutdown_event.set()
            await self.cancel_all_tasks()

            # Signal result collector to stop
            try:
                if self._result_queue:
                    self._result_queue.put("SHUTDOWN", block=False)
            except Exception as e:
                self.logger.warning(f"Error signaling queue shutdown: {e}")

            # Stop monitoring task
            if self._monitor_task and not self._monitor_task.done():
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._monitor_task.cancel()

            # Clean up result queue
            try:
                if self._result_queue:
                    while True:
                        try:
                            self._result_queue.get(block=False)
                        except Exception:
                            break
                    self._result_queue = None
                self.logger.debug("Result queue cleaned up")
            except Exception as e:
                self.logger.warning(f"Error cleaning up result queue: {e}")

            # Clean up DDict
            try:
                if self._ddict:
                    self._ddict.clear()
                    self._ddict.destroy()
                    self._ddict = None
                self.logger.debug("DDict cleaned up and destroyed")
            except Exception as e:
                self.logger.warning(f"Error cleaning up DDict: {e}")

            # Clean up system allocation
            try:
                if self._system_alloc:
                    self._system_alloc = None
                self.logger.debug("System allocation cleaned up")
            except Exception as e:
                self.logger.warning(f"Error cleaning up system allocation: {e}")

            self.logger.info("Dragon execution backend V1 shutdown complete")

        except Exception as e:
            self.logger.exception(f"Error during shutdown: {e}")
        finally:
            self.tasks.clear()
            self._running_tasks.clear()
            self._initialized = False

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    @classmethod
    async def create(cls, resources: Optional[dict] = None):
        """Create and initialize a DragonExecutionBackendV1."""
        backend = cls(resources)
        return await backend


class DragonExecutionBackendV2(BaseExecutionBackend):
    """Dragon execution backend with parallel scheduling.

    Features:
    - Per-worker queue architecture for true load balancing
    - Slot reservation ensures tasks go to workers with capacity
    - Tasks with same rank requirement run in parallel on different workers
    - High performance with minimal coordination overhead
    - Support for both compute and training workers

    Example configuration:
        from radical.asyncflow.backends.execution.dragon import (
            WorkerGroupConfigV2, PolicyConfigV2, WorkerTypeV2
        )
        from dragon.infrastructure.policy import Policy

        # Define policies for node placement
        policy_n0 = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)
        policy_n1 = Policy(host_id=1, distribution=Policy.Distribution.BLOCK)

        # Compute worker
        compute_worker = WorkerGroupConfigV2(
            name="cpu_worker",
            worker_type=WorkerTypeV2.COMPUTE,
            policies=[
                PolicyConfigV2(policy=policy_n0, nprocs=128),
                PolicyConfigV2(policy=policy_n1, nprocs=128)
            ]
        )

        # GPU compute worker
        gpu_worker = WorkerGroupConfigV2(
            name="gpu_worker",
            worker_type=WorkerTypeV2.COMPUTE,
            policies=[
                PolicyConfigV2(policy=policy_n0, nprocs=128, ngpus=2),
                PolicyConfigV2(policy=policy_n1, nprocs=128, ngpus=2)
            ]
        )

        # Training worker (for distributed training with DDP/NCCL)
        import socket
        hostname = socket.gethostname()

        policy_rank0 = Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=hostname,
            gpu_affinity=[0]
        )
        policy_rank1 = Policy(
            placement=Policy.Placement.HOST_NAME,
            host_name=hostname,
            gpu_affinity=[1]
        )

        training_worker = WorkerGroupConfigV2(
            name="training_worker",
            worker_type=WorkerTypeV2.TRAINING,
            policies=[
                PolicyConfigV2(policy=policy_rank0),
                PolicyConfigV2(policy=policy_rank1)
            ],
            kwargs={'nprocs': 2, 'ppn': 32, 'port': 29500}
        )

        # Create backend
        resources = {"workers": [compute_worker, gpu_worker, training_worker]}
        backend = await DragonExecutionBackendV2(resources)
    """

    @typeguard.typechecked
    def __init__(self,
    resources: Optional[dict] = None,
    ddict: Optional[DDict] = None,
    name: Optional[str] = "dragon"):

        if dragon is None:
            raise ImportError("Dragon is required for DragonExecutionBackendV2.")

        super().__init__(name=name)

        self.logger = _get_logger()
        self.tasks: dict[str, dict[str, Any]] = {}
        self._callback_func: Callable = lambda t, s: None
        self._resources = resources or {}
        self._initialized = False
        self._backend_state = BackendMainStates.INITIALIZED
        self._canceled_tasks = set()

        # Parse worker configuration
        self._worker_configs = self._parse_worker_config(self._resources)
        self._total_slots = sum(cfg.total_slots() for cfg in self._worker_configs)
        self._total_gpus = sum(cfg.total_gpus() for cfg in self._worker_configs)

        # Other resources
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())
        self._reference_threshold: int = int(
            self._resources.get("reference_threshold", DRAGON_DEFAULT_REF_THRESHOLD)
        )

        # Task tracking
        self._running_tasks: dict[str, TaskInfoV2] = {}
        self._pending_tasks: asyncio.Queue = asyncio.Queue()

        # Dragon components
        self._ddict: Optional[DDict] = ddict
        self._system_alloc: Optional[System] = None
        self._shared_memory: Optional[SharedMemoryManagerV2] = None
        self._result_collector: Optional[ResultCollectorV2] = None
        self._worker_pool: Optional[WorkerPoolV2] = None
        self._backend_id: str = f"dragon_{uuid.uuid4().hex[:8]}"

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    def _parse_worker_config(self, resources: dict) -> list[WorkerGroupConfigV2]:
        """Parse worker configuration from resources.

        If no workers specified, creates a default compute worker.
        """
        if "workers" in resources:
            workers = resources["workers"]
            if not isinstance(workers, list):
                raise TypeError(
                    "resources['workers'] must be a list of WorkerGroupConfigV2 objects"
                )

            if not workers:
                raise ValueError("resources['workers'] cannot be empty")

            for idx, worker in enumerate(workers):
                if not isinstance(worker, WorkerGroupConfigV2):
                    raise TypeError(
                        f"Worker at index {idx} must be a WorkerGroupConfigV2 object. "
                        f"Got {type(worker).__name__} instead."
                    )

            return workers
        else:
            # No workers specified - create default compute worker
            slots = int(resources.get("slots", mp.cpu_count() or 1))
            self.logger.info(f"No workers specified, creating default compute worker with {slots} slots")
            return [
                WorkerGroupConfigV2(
                    name="default_worker",
                    worker_type=WorkerTypeV2.COMPUTE,
                    policies=[PolicyConfigV2(nprocs=slots, policy=None, ngpus=0)],
                )
            ]

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components
        """
        if not self._initialized:
            try:
                self.logger.debug("Starting Dragon backend V2 async initialization...")

                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                # Step 3: Set backend state to INITIALIZED
                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                # Step 4: Initialize backend components
                await self._initialize()
                self._initialized = True
                self.logger.info("Dragon backend V2 fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dragon backend V2 initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self) -> None:
        try:
            # Set multiprocessing method
            try:
                if mp.get_start_method() != "dragon":
                    mp.set_start_method("dragon", force=True)
            except RuntimeError:
                pass

            # Initialize system
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            self.logger.debug(f"System allocation created with {nnodes} nodes")

            # Initialize DDict
            if not self._ddict:
                self._ddict = DDict(
                    n_nodes=nnodes,
                    total_mem=nnodes * int(4 * 1024 * 1024 * 1024),
                    wait_for_keys=True,
                    working_set_size=4,
                    timeout=200,
                )
            self.logger.debug("DDict initialized")

            # Initialize shared memory manager
            self._shared_memory = SharedMemoryManagerV2(
                self._ddict, self._system_alloc, self.logger, self._reference_threshold
            )
            await self._shared_memory.initialize()

            # Initialize result collector
            self._result_collector = ResultCollectorV2(self._shared_memory, self.logger)

            # Initialize worker pool with per-worker queues
            self._worker_pool = WorkerPoolV2(
                self._worker_configs,
                self._ddict,
                self._working_dir,
                self.logger,
                self._system_alloc,
                self._backend_id,
            )
            await self._worker_pool.initialize()

            # Start monitoring and scheduling
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            self._scheduler_task = asyncio.create_task(self._schedule_tasks())

            self.logger.info(
                f"Dragon backend V2 initialized: {len(self._worker_configs)} workers, "
                f"{self._total_slots} total slots, {self._total_gpus} total GPUs, "
                f"reference threshold: {self._reference_threshold} bytes"
            )

        except Exception as e:
            self.logger.exception(f"Failed to initialize Dragon backend V2: {e}")
            raise

    def get_task_states_map(self):
        return StateMapper(backend=self)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        self._ensure_initialized()

        # Set backend state to RUNNING when tasks are submitted
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        for task in tasks:
            is_valid, error_msg = self._validate_task(task)
            if not is_valid:
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue

            self.tasks[task["uid"]] = task

            try:
                # Add to pending queue for scheduler
                await self._pending_tasks.put(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _schedule_tasks(self) -> None:
        """Parallel scheduler with multiple worker coroutines."""

        num_scheduler_workers = min(32, max(4, self._total_slots // 4))

        self.logger.info(f"Starting parallel scheduler with {num_scheduler_workers} scheduler workers")

        async def scheduler_worker(worker_id: int):
            """Individual scheduler worker."""
            tasks_processed = 0

            while not self._shutdown_event.is_set():
                try:
                    try:
                        task = await asyncio.wait_for(self._pending_tasks.get(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue

                    task["uid"]
                    backend_kwargs = task.get("task_backend_specific_kwargs", {})
                    ranks = int(backend_kwargs.get("ranks", 1))
                    gpus_per_rank = int(backend_kwargs.get("gpus_per_rank", 0))
                    worker_hint = backend_kwargs.get("worker_hint")
                    worker_type_hint = backend_kwargs.get("worker_type")

                    pinning_policy_str = backend_kwargs.get("pinning_policy", "").lower()
                    try:
                        pinning_policy = (
                            WorkerPinningPolicyV2(pinning_policy_str)
                            if pinning_policy_str
                            else None
                        )
                    except ValueError:
                        pinning_policy = None

                    pinning_timeout = float(backend_kwargs.get("pinning_timeout", 30.0))

                    worker_name = await self._apply_pinning_policy(
                        task,
                        ranks,
                        gpus_per_rank,
                        worker_hint,
                        worker_type_hint,
                        pinning_policy,
                        pinning_timeout,
                    )

                    if not worker_name:
                        continue

                    max_retries = 3
                    retry_count = 0
                    success = False
                    gpu_allocations = {}

                    while retry_count < max_retries and not self._shutdown_event.is_set():
                        success, gpu_allocations = await self._worker_pool.reserve_resources(
                            worker_name, ranks, gpus_per_rank
                        )

                        if success:
                            break

                        retry_count += 1
                        await asyncio.sleep(0.001 * retry_count)

                    if not success:
                        await self._pending_tasks.put(task)
                        await asyncio.sleep(0.005)
                        continue

                    try:
                        await self._submit_task_to_worker(task, worker_name, ranks, gpu_allocations)
                        tasks_processed += 1

                    except Exception as e:
                        await self._worker_pool.release_resources(
                            worker_name, ranks, gpu_allocations
                        )
                        task["exception"] = e
                        self._callback_func(task, "FAILED")

                except Exception as e:
                    self.logger.exception(f"Scheduler worker {worker_id}: Unexpected error: {e}")
                    await asyncio.sleep(0.1)

            self.logger.debug(
                f"Scheduler worker {worker_id} shutting down (processed {tasks_processed} tasks)"
            )

        scheduler_tasks = []
        for worker_id in range(num_scheduler_workers):
            task = asyncio.create_task(scheduler_worker(worker_id))
            scheduler_tasks.append(task)

        self.logger.info(f"Scheduler worker pool active with {num_scheduler_workers} workers")

        await self._shutdown_event.wait()

        self.logger.info("Shutdown signal received, stopping scheduler workers...")

        try:
            await asyncio.wait_for(
                asyncio.gather(*scheduler_tasks, return_exceptions=True), timeout=10.0
            )
        except asyncio.TimeoutError:
            self.logger.warning("Scheduler workers did not complete within timeout, canceling...")
            for task in scheduler_tasks:
                if not task.done():
                    task.cancel()

            await asyncio.gather(*scheduler_tasks, return_exceptions=True)

        self.logger.info("All scheduler workers stopped")

    async def _apply_pinning_policy(
        self,
        task: dict,
        ranks: int,
        gpus_per_rank: int,
        worker_hint: Optional[str],
        worker_type_hint: Optional[str],
        pinning_policy: Optional[WorkerPinningPolicyV2],
        timeout: float,
    ) -> Optional[str]:
        """Apply worker pinning policy to find appropriate worker."""

        if not worker_hint or not pinning_policy:
            worker_name = self._worker_pool.find_worker_for_task(
                ranks, gpus_per_rank, worker_type_hint=worker_type_hint
            )
            while not worker_name and not self._shutdown_event.is_set():
                await asyncio.sleep(0.01)
                worker_name = self._worker_pool.find_worker_for_task(
                    ranks, gpus_per_rank, worker_type_hint=worker_type_hint
                )
            return worker_name

        if not self._worker_pool.worker_exists(worker_hint):
            error_msg = f"Worker hint '{worker_hint}' does not exist. Available workers: {list(self._worker_pool.worker_slots.keys())}"
            self.logger.error(error_msg)
            task["exception"] = ValueError(error_msg)
            self._callback_func(task, "FAILED")
            return None

        if pinning_policy == WorkerPinningPolicyV2.AFFINITY:
            if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                self.logger.debug(
                    f"Task {task['uid']}: AFFINITY policy - using preferred worker {worker_hint}"
                )
                return worker_hint
            else:
                worker_name = self._worker_pool.find_worker_for_task(
                    ranks, gpus_per_rank, worker_type_hint=worker_type_hint
                )
                if worker_name:
                    self.logger.debug(f"Task {task['uid']}: AFFINITY policy - fallback to {worker_name}")
                    return worker_name
                while not worker_name and not self._shutdown_event.is_set():
                    await asyncio.sleep(0.01)
                    worker_name = self._worker_pool.find_worker_for_task(
                        ranks, gpus_per_rank, worker_type_hint=worker_type_hint
                    )
                return worker_name

        elif pinning_policy == WorkerPinningPolicyV2.STRICT:
            self.logger.debug(f"Task {task['uid']}: STRICT policy - waiting for worker {worker_hint}")
            while not self._shutdown_event.is_set():
                if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                    self.logger.debug(
                        f"Task {task['uid']}: STRICT policy - worker {worker_hint} now available"
                    )
                    return worker_hint
                await asyncio.sleep(0.01)
            return None

        elif pinning_policy == WorkerPinningPolicyV2.SOFT:
            self.logger.debug(
                f"Task {task['uid']}: SOFT policy - waiting {timeout}s for worker {worker_hint}"
            )
            start_time = time.time()

            while time.time() - start_time < timeout:
                if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                    self.logger.debug(
                        f"Task {task['uid']}: SOFT policy - worker {worker_hint} available"
                    )
                    return worker_hint
                await asyncio.sleep(0.01)
                if self._shutdown_event.is_set():
                    return None

            self.logger.debug(f"Task {task['uid']}: SOFT policy - timeout reached, using fallback")
            worker_name = self._worker_pool.find_worker_for_task(
                ranks, gpus_per_rank, worker_type_hint=worker_type_hint
            )
            while not worker_name and not self._shutdown_event.is_set():
                await asyncio.sleep(0.01)
                worker_name = self._worker_pool.find_worker_for_task(
                    ranks, gpus_per_rank, worker_type_hint=worker_type_hint
                )

            if worker_name:
                self.logger.debug(f"Task {task['uid']}: SOFT policy - fallback to {worker_name}")
            return worker_name

        elif pinning_policy == WorkerPinningPolicyV2.EXCLUSIVE:
            if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                self.logger.debug(f"Task {task['uid']}: EXCLUSIVE policy - using worker {worker_hint}")
                return worker_hint
            else:
                total_capacity = self._worker_pool.worker_slots.get(worker_hint, 0)
                total_gpu_capacity = self._worker_pool.worker_gpus.get(worker_hint, 0)
                total_gpus_needed = ranks * gpus_per_rank

                if ranks > total_capacity or total_gpus_needed > total_gpu_capacity:
                    error_msg = (
                        f"Task {task['uid']}: EXCLUSIVE policy - worker '{worker_hint}' "
                        f"has insufficient total capacity ({total_capacity} slots, {total_gpu_capacity} GPUs) "
                        f"for {ranks} ranks × {gpus_per_rank} GPUs/rank"
                    )
                else:
                    error_msg = (
                        f"Task {task['uid']}: EXCLUSIVE policy - worker '{worker_hint}' "
                        f"currently has insufficient free resources"
                    )

                self.logger.error(error_msg)
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                return None

        return None

    async def _submit_task_to_worker(
        self,
        task: dict[str, Any],
        worker_name: str,
        ranks: int,
        gpu_allocations: dict[int, list[int]],
    ) -> None:
        """Submit task to specific worker."""
        uid = task["uid"]

        try:
            # Register task with result collector
            self._result_collector.register_task(uid, ranks)

            # Determine task type
            is_function = bool(task.get("function"))
            task_type = TaskTypeV2.FUNCTION if is_function else TaskTypeV2.EXECUTABLE

            backend_kwargs = task.get("task_backend_specific_kwargs", {})
            use_ddict_storage = backend_kwargs.get("use_ddict_storage", False)
            execute_in_shell = backend_kwargs.get("shell", False)

            for rank in range(ranks):
                gpu_ids = gpu_allocations.get(rank, [])

                if is_function:
                    request = WorkerRequestV2(
                        task_uid=uid,
                        task_type=TaskTypeV2.FUNCTION,
                        rank=rank,
                        total_ranks=ranks,
                        gpu_ids=gpu_ids,
                        use_ddict_storage=use_ddict_storage,
                        function=task["function"],
                        args=task.get("args", ()),
                        kwargs=task.get("kwargs", {}),
                    )
                else:
                    request = WorkerRequestV2(
                        task_uid=uid,
                        task_type=TaskTypeV2.EXECUTABLE,
                        rank=rank,
                        total_ranks=ranks,
                        gpu_ids=gpu_ids,
                        use_ddict_storage=False,
                        executable=task["executable"],
                        exec_args=list(task.get("arguments", [])),
                        working_dir=self._working_dir,
                        execute_in_shell=execute_in_shell,
                    )

                # Submit to specific worker's queue
                self._worker_pool.submit_request(worker_name, request)

            # Track task
            self._running_tasks[uid] = TaskInfoV2(
                task_type=task_type,
                ranks=ranks,
                worker_name=worker_name,
                gpu_allocations=gpu_allocations,
                start_time=time.time(),
            )

            self._callback_func(task, "RUNNING")

        except Exception:
            raise

    async def _monitor_tasks(self) -> None:
        """Monitor tasks by consuming responses from worker pool."""
        while not self._shutdown_event.is_set():
            try:
                completed_tasks = []
                # Consume responses from worker pool (batch processing)
                for _ in range(1000):
                    response = self._worker_pool.try_get_response()
                    if response:
                        completed_uid = self._result_collector.process_response(response)
                        if completed_uid and completed_uid not in completed_tasks:
                            completed_tasks.append(completed_uid)
                    else:
                        break

                for uid in completed_tasks:
                    # Check if task was canceled
                    if uid in self._canceled_tasks:
                        self.logger.debug(f"Ignoring response for canceled task {uid}")
                        # Now clean it up from result collector
                        self._result_collector.cleanup_task(uid)
                        self._canceled_tasks.discard(uid)
                        continue

                    # Check if task is still being tracked
                    task_info = self._running_tasks.get(uid)
                    if not task_info:
                        # Already processed or never tracked
                        continue

                    if task_info.canceled:
                        # Redundant check, but safe
                        self.logger.debug(f"Skipping already-canceled task {uid}")
                        self._result_collector.cleanup_task(uid)
                        self._running_tasks.pop(uid, None)
                        continue

                    # Normal task completion processing
                    task = self.tasks.get(uid)
                    if task:
                        result = await self._result_collector.get_task_result(uid)
                        if result:
                            task.update(result)

                        await self._worker_pool.release_resources(
                            task_info.worker_name, task_info.ranks, task_info.gpu_allocations
                        )

                        # Send appropriate callback
                        if task.get("exception") or task.get("exit_code", 0) != 0:
                            self._callback_func(task, "FAILED")
                        else:
                            self._callback_func(task, "DONE")

                    self._running_tasks.pop(uid, None)

                await asyncio.sleep(0.001)
            except Exception as e:
                self.logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a running task.
        Note: With worker pool architecture, cancellation is best-effort.
        Workers that already picked up the task will complete it.
        """
        task_info = self._running_tasks.get(uid)
        if not task_info:
            return False

        # Mark as canceled FIRST
        task_info.canceled = True
        self._canceled_tasks.add(uid)  # Track it

        # Update task dict with canceled flag
        if uid in self.tasks:
            self.tasks[uid]["canceled"] = True

        # Send immediate CANCELED callback
        task = self.tasks.get(uid)
        if task:
            self._callback_func(task, "CANCELED")

        # Release resources
        await self._worker_pool.release_resources(
            task_info.worker_name, task_info.ranks, task_info.gpu_allocations
        )

        # Remove from running tasks
        self._running_tasks.pop(uid, None)

        return True

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        canceled = 0
        for uid in list(self._running_tasks.keys()):
            if await self.cancel_task(uid):
                canceled += 1
        return canceled

    def _validate_task(self, task: dict) -> tuple[bool, str]:
        """Validate task configuration."""
        uid = task.get("uid")
        if not uid:
            return False, "Task must have a 'uid' field"

        function = task.get("function")
        executable = task.get("executable")

        if not function and not executable:
            return False, "Task must specify either 'function' or 'executable'"

        if function and executable:
            return False, "Task cannot specify both 'function' and 'executable'"

        if function and not callable(function):
            return False, "Task 'function' must be callable"

        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        ranks = backend_kwargs.get("ranks", 1)

        try:
            ranks = int(ranks)
            if ranks < 1:
                return False, "Task 'ranks' must be >= 1"
        except (ValueError, TypeError):
            return False, "Task 'ranks' must be a valid integer"

        gpus_per_rank = backend_kwargs.get("gpus_per_rank", 0)
        try:
            gpus_per_rank = int(gpus_per_rank)
            if gpus_per_rank < 0:
                return False, "Task 'gpus_per_rank' must be >= 0"
        except (ValueError, TypeError):
            return False, "Task 'gpus_per_rank' must be a valid integer"

        pinning_policy = backend_kwargs.get("pinning_policy", "").lower()
        if pinning_policy:
            try:
                WorkerPinningPolicyV2(pinning_policy)
            except ValueError:
                valid_policies = [p.value for p in WorkerPinningPolicyV2]
                return (
                    False,
                    f"Invalid pinning_policy '{pinning_policy}'. Must be one of: {valid_policies}",
                )

        worker_hint = backend_kwargs.get("worker_hint")
        if worker_hint and not isinstance(worker_hint, str):
            return False, "worker_hint must be a string"

        timeout = backend_kwargs.get("pinning_timeout")
        if timeout is not None:
            try:
                float(timeout)
            except (ValueError, TypeError):
                return False, "pinning_timeout must be a number"

        return True, ""

    def _ensure_initialized(self):
        """Ensure backend is initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DragonExecutionBackendV2 must be awaited before use. "
                "Use: backend = await DragonExecutionBackendV2(resources)"
            )

    def get_ddict(self) -> DDict:
        """Get shared DDict for cross-task data sharing."""
        self._ensure_initialized()
        return self._ddict

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    async def task_state_cb(self, task: dict, state: str) -> None:
        pass

    async def build_task(self, task: dict) -> None:
        pass

    async def shutdown(self) -> None:
        """Shutdown backend gracefully."""
        if not self._initialized:
            return

        try:
            # Set backend state to SHUTDOWN
            self._backend_state = BackendMainStates.SHUTDOWN
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")
            self.logger.info("Starting Dragon backend V2 shutdown...")
            self._shutdown_event.set()

            # Cancel all running tasks
            canceled = await self.cancel_all_tasks()
            if canceled > 0:
                self.logger.info(f"Canceled {canceled} running tasks")

            # Stop scheduler
            if self._scheduler_task and not self._scheduler_task.done():
                self.logger.debug("Stopping scheduler task...")
                try:
                    await asyncio.wait_for(self._scheduler_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Scheduler task timeout, canceling...")
                    self._scheduler_task.cancel()
                    try:
                        await self._scheduler_task
                    except asyncio.CancelledError:
                        pass

            # Stop monitoring
            if self._monitor_task and not self._monitor_task.done():
                self.logger.debug("Stopping monitor task...")
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Monitor task timeout, canceling...")
                    self._monitor_task.cancel()
                    try:
                        await self._monitor_task
                    except asyncio.CancelledError:
                        pass

            # Shutdown worker pool (this waits for worker processes to exit)
            if self._worker_pool:
                await self._worker_pool.shutdown()

            # Clean up DDict
            if self._ddict:
                try:
                    self.logger.debug("Cleaning up DDict...")
                    self._ddict.clear()
                    self._ddict.destroy()
                    self.logger.debug("DDict cleanup complete")
                except Exception as e:
                    self.logger.warning(f"Error cleaning up DDict: {e}")

            self.logger.info("Dragon backend V2 shutdown complete")

        except Exception as e:
            self.logger.exception(f"Error during shutdown: {e}")
        finally:
            self.tasks.clear()
            self._running_tasks.clear()
            self._initialized = False

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    @classmethod
    async def create(cls, resources: Optional[dict] = None):
        """Create and initialize backend."""
        backend = cls(resources)
        return await backend


class DragonExecutionBackendV3(BaseExecutionBackend):
    """
    Fast Dragon Batch integration using .wait() in threads.

    No polling! Each compiled batch gets a thread that calls .wait()
    and triggers callbacks when done. This is the Dragon-native way.
    """

    def __init__(
        self,
        num_workers: Optional[int] = None,
        working_directory: Optional[str] = None,
        disable_background_batching: bool = False,
        disable_telemetry: bool = False,
        name: Optional[str] = "dragon",
    ):
        if not Batch:
            raise RuntimeError("Dragon Batch not available")

        super().__init__(name=name)

        self.logger = _get_logger()
        self.batch = Batch(
            num_workers=num_workers or 0,
            disable_telem=disable_telemetry,
            disable_background_batching=disable_background_batching,
        )

        self._backend_state = BackendMainStates.INITIALIZED
        self._callback_func = lambda t, s: None
        self._task_registry: dict[str, Any] = {}
        self._task_states = TaskStateMapperV3()
        self._initialized = False
        self._cancelled_tasks = []

        # Thread pool for waiting on batches
        self._wait_executor = ThreadPoolExecutor(
            max_workers=max(8, mp.cpu_count() // 8), thread_name_prefix="dragon_batch_wait"
        )

        self._shutdown_event = threading.Event()

        self.logger.info(
            f"DragonExecutionBackendV3: {self.batch.num_workers} workers, "
            f"{self.batch.num_managers} managers"
        )

    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components (if needed)
        """
        if not self._initialized:
            try:
                self.logger.debug("Starting Dragon backend V3 async initialization...")

                # Step 1: Register backend states
                self.logger.debug("Registering backend states...")
                StateMapper.register_backend_states_with_defaults(backend=self)

                # Step 2: Register task states
                self.logger.debug("Registering task states...")
                StateMapper.register_backend_tasks_states_with_defaults(backend=self)

                # Step 3: Set backend state to INITIALIZED
                self._backend_state = BackendMainStates.INITIALIZED
                self.logger.debug(f"Backend state set to: {self._backend_state.value}")

                # Step 4: Initialize backend components (V3 is already initialized in __init__)
                self._initialized = True
                self.logger.info("Dragon backend V3 fully initialized and ready")

            except Exception as e:
                self.logger.exception(f"Dragon backend V3 initialization failed: {e}")
                self._initialized = False
                raise
        return self

    def _wait_for_batch(self, compiled_tasks, task_uids: list[str]):
        """
        Wait for batch to complete and trigger callbacks.

        Simple: just wait(), then check results. No state tracking needed.
        """
        try:
            # Check for shutdown before starting wait
            if self._shutdown_event.is_set():
                self.logger.debug(f"Shutdown detected, aborting wait for tasks: {task_uids}")
                return

            self.logger.debug(f"Waiting for batch with tasks: {task_uids}")
            # Wait for batch with timeout to allow periodic shutdown checks
            while True:
                try:
                    compiled_tasks.wait(timeout=5)
                    break
                except TimeoutError:
                    pass

                # Check if shutdown was requested during wait
                if self._shutdown_event.is_set():
                    self.logger.debug(f"Shutdown detected during batch wait for tasks: {task_uids}")
                    return

            self.logger.debug(f"Batch wait completed for tasks: {task_uids}")

            # Get results for all tasks
            for uid in task_uids:
                task_info = self._task_registry.get(uid)
                if not task_info:
                    self.logger.warning(f"Task {uid} not found in registry")
                    continue

                if uid in self._cancelled_tasks:
                    self.logger.debug(f"Skipping cancelled task {uid}")
                    continue

                batch_task = task_info["batch_task"]
                task_desc = task_info["description"]

                try:
                    self.logger.debug(f"Getting stdout for task {uid}")
                    # Always try to get stdout first (success/failure)
                    # May be None for failed Python functions
                    stdout = batch_task.stdout.get()
                    task_desc["stdout"] = stdout if stdout is not None else ""

                    self.logger.debug(f"Getting result for task {uid}")
                    # This raises if task failed
                    result = batch_task.result.get()
                    task_desc["return_value"] = result
                    self.logger.debug(f"Task {uid} completed successfully")
                    self._callback_func(task_desc, "DONE")

                except Exception as e:
                    # Task failed - result.get() raised
                    self.logger.exception(f"Task {uid} failed with exception: {e}")
                    task_desc["exception"] = e

                    # Try to get stderr, but may be None for failed tasks
                    stderr = batch_task.stderr.get()
                    if stderr is None:
                        # Dragon includes full traceback in exception message
                        # Extract it for better error reporting
                        stderr = str(e)

                    task_desc["stderr"] = stderr
                    self._callback_func(task_desc, "FAILED")

        except Exception as e:
            # Batch-level failure (wait() failed or catastrophic error)
            self.logger.error(f"Batch wait failed: {e}", exc_info=True)

            for uid in task_uids:
                task_info = self._task_registry.get(uid)
                if task_info:
                    task_info["description"]["exception"] = e
                    task_info["description"]["stderr"] = str(e)
                    self._callback_func(task_info["description"], "FAILED")

    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit a batch of tasks and start a wait thread for them."""
        if self._backend_state == BackendMainStates.SHUTDOWN:
            raise RuntimeError("Cannot submit during shutdown")

        # Set backend state to RUNNING when tasks are submitted
        if self._backend_state != BackendMainStates.RUNNING:
            self._backend_state = BackendMainStates.RUNNING
            self.logger.debug(f"Backend state set to: {self._backend_state.value}")

        # Create Batch tasks (don't start yet)
        batch_tasks = []
        task_uids = []

        for task in tasks:
            try:
                batch_task = await self.build_task(task)
                batch_tasks.append(batch_task)
            except Exception as e:
                self.logger.error(f"Failed to create task {task.get('uid')}: {e}", exc_info=True)
                task["exception"] = e
                self._callback_func(task, "FAILED")

            task_uids.append(task["uid"])

        if not batch_tasks:
            return

        # Compile into a single batch
        compiled_tasks = self.batch.compile(batch_tasks)

        # Start the batch
        compiled_tasks.start()

        self.logger.info(f"Submitted batch of {len(batch_tasks)} tasks for execution")

        # Launch thread to wait for completion
        self._wait_executor.submit(self._wait_for_batch, compiled_tasks, task_uids)

    async def build_task(self, task: dict):
        """
        Translate AsyncFlow task to Dragon Batch task.

        Translation Priority (in order):
        1. If process_templates (list) provided → Job mode (ignore type='mpi', ignore ranks) [function/executable]
        2. If process_template (single) provided → Process mode [function/executable]
        3. If type='mpi' AND ranks provided (no templates) → Job mode (auto-build) [function/executable]
        4. If is_function (no templates, no MPI) → Function mode (native) [function only]
        5. If is_executable (no templates, no MPI) → Process mode (auto-build) [executable only]

        Execution Modes:
        - Function Native: batch.function() - direct Python function call
        - Function Process: batch.process() - function wrapped in ProcessTemplate
        - Function Job: batch.job() - function in MPI job with multiple ranks
        - Executable Process: batch.process() - single executable process
        - Executable Job: batch.job() - executable in MPI job with multiple ranks

        """
        # Fast path: extract everything upfront
        uid = task["uid"]
        is_function = bool(task.get("function"))
        target = task.get("function" if is_function else "executable")
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        name = task.get("name", uid)

        # For functions: use "args" and "kwargs"
        # For executables: use "arguments"
        if is_function:
            task_args = task.get("args", [])
            task_kwargs = task.get("kwargs", {})
        else:
            task_args = tuple(task.get("arguments", []))
            task_kwargs = None

        timeout = backend_kwargs.get("timeout", 1000000000.0)

        # Handle async functions
        if is_function and asyncio.iscoroutinefunction(target):
            original_target = target
            def target(*a, **kw):
                return asyncio.run(original_target(*a, **kw))

        # Get template configs once
        process_templates_config = backend_kwargs.get("process_templates")
        process_template_config = backend_kwargs.get("process_template")

        # Single decision tree - no redundant checks
        if process_templates_config:
            # Priority 1: Job with user templates
            process_templates = [
                (
                    nranks,
                    ProcessTemplate(target, **{**tc, "args": task_args, "kwargs": task_kwargs}),
                )
                for nranks, tc in process_templates_config
            ]
            batch_task = self.batch.job(process_templates, name=name, timeout=timeout)
            execution_mode = "job"

        elif process_template_config:
            # Priority 2: Process with user template
            batch_task = self.batch.process(
                ProcessTemplate(
                    target, **{**process_template_config, "args": task_args, "kwargs": task_kwargs}
                ),
                name=name,
                timeout=timeout,
            )
            execution_mode = "process"

        elif backend_kwargs.get("type") == "mpi":
            # Priority 3: Job auto-build
            batch_task = self.batch.job(
                [
                    (
                        backend_kwargs.get("ranks", 1),
                        ProcessTemplate(target, args=task_args, kwargs=task_kwargs),
                    )
                ],
                name=name,
                timeout=timeout,
            )
            execution_mode = "job"

        elif is_function:
            # Priority 4: Function native
            batch_task = self.batch.function(
                target, *task_args, name=name, timeout=timeout, **task_kwargs
            )
            execution_mode = "function"

        else:
            # Priority 5: Executable process auto-build
            batch_task = self.batch.process(
                ProcessTemplate(target, args=task_args, kwargs=task_kwargs),
                name=name,
                timeout=timeout,
            )
            execution_mode = "process"

        # Register and return
        self._task_registry[uid] = {
            "uid": uid,
            "description": task,
            "batch_task": batch_task,
        }

        self.logger.debug(f"Created {execution_mode} task: {uid}")

        return batch_task

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    async def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return self._backend_state.value

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def task_state_cb(self, task: dict, state: str) -> None:
        self._callback_func(task, state)

    def get_task_states_map(self):
        return self._task_states

    async def cancel_task(self, uid: str) -> bool:
        if uid not in self._task_registry:
            raise ValueError(f"Task {uid} not found")

        # NOTE: dragon.batch does not expose nor support
        # process/function/job cancellation, we just notify
        # the asyncflow that the task is cancelled so not to block the flow
        task = self._task_registry[uid]["description"]
        self._callback_func(task, "CANCELED")
        self._cancelled_tasks.append(uid)

        return True

    async def shutdown(self) -> None:
        if self._backend_state == BackendMainStates.SHUTDOWN:
            return

        # Set backend state to SHUTDOWN
        self._backend_state = BackendMainStates.SHUTDOWN
        self.logger.debug(f"Backend state set to: {self._backend_state.value}")
        self.logger.info("Shutting down V3 backend")
        self._shutdown_event.set()

        # Close Batch FIRST to unblock waiting threads
        if self.batch:
            try:
                self.logger.debug("Closing batch...")
                self.batch.close()
                self.batch.join(timeout=10.0)
                self.logger.debug("Batch closed successfully")
            except Exception as e:
                self.logger.warning(f"Error closing batch gracefully: {e}")
                try:
                    self.logger.debug("Attempting to terminate batch...")
                    self.batch.terminate()
                except Exception as te:
                    self.logger.warning(f"Error terminating batch: {te}")

        # Give threads a moment to detect batch closure and exit cleanly
        time.sleep(0.5)

        # Now shutdown wait executor - threads should be unblocked
        self.logger.debug("Shutting down wait executor...")
        self._wait_executor.shutdown(wait=False)
        self.logger.debug("Wait executor shutdown complete")

        self._task_registry.clear()
        self._state = "idle"
        self.logger.info("Shutdown V3 complete")

    # Batch features
    def fence(self):
        self.batch.fence()

    def create_ddict(self, *args, **kwargs):
        return self.batch.ddict(*args, **kwargs)

    @classmethod
    async def create(
        cls,
        num_workers: Optional[int] = None,
        working_directory: Optional[str] = None,
        disable_background_batching: bool = False,
        disable_telemetry: bool = False,
    ):
        """Create and initialize a DragonExecutionBackendV3."""
        backend = cls(
            num_workers=num_workers,
            working_directory=working_directory,
            disable_background_batching=disable_background_batching,
            disable_telemetry=disable_telemetry,
        )
        return await backend


class DragonTelemetryCollector:
    """
    Telemetry collection class that spawns one collector process per node to monitor
    CPU, GPU memory, and RAM utilization.

    This class integrates with Dragon's built-in Telemetry infrastructure and periodically
    checkpoints all collected metrics (system + user custom) to JSON files for persistence.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        import multiprocessing as mp
        from dragon.telemetry.node_collector import DragonTelemetryCollector

        if __name__ == "__main__":
            mp.set_start_method("dragon")

            # Initialize and start collector
            collector = DragonTelemetryCollector(
                collection_rate=1.0,
                checkpoint_interval=30.0,
                checkpoint_dir="/scratch/telemetry",
                checkpoint_count=10
            )
            collector.start()

            # Run workload
            pool = mp.Pool(16)
            results = pool.map(compute_func, data)
            pool.close()
            pool.join()

            # Add custom metrics
            collector.add_custom_metric("final_accuracy", 0.95)

            # Stop and checkpoint
            collector.stop()
    """

    def __init__(
        self,
        collection_rate: float = 1.0,
        checkpoint_interval: float = 30.0,
        checkpoint_dir: str = "/tmp",
        checkpoint_count: int = 5,
        enable_cpu: bool = True,
        enable_gpu: bool = True,
        enable_memory: bool = True,
        metric_prefix: str = "user",
    ):
        """Initialize the DragonTelemetryCollector.

        :param collection_rate: How often to collect metrics in seconds, defaults to 1.0
        :type collection_rate: float, optional
        :param checkpoint_interval: How often to write checkpoints in seconds, defaults to 30.0
        :type checkpoint_interval: float, optional
        :param checkpoint_dir: Directory to write checkpoint files, defaults to "/tmp"
        :type checkpoint_dir: str, optional
        :param checkpoint_count: Maximum number of checkpoint files to keep (0 for unlimited), defaults to 5
        :type checkpoint_count: int, optional
        :param enable_cpu: Enable CPU metric collection, defaults to True
        :type enable_cpu: bool, optional
        :param enable_gpu: Enable GPU metric collection, defaults to True
        :type enable_gpu: bool, optional
        :param enable_memory: Enable memory metric collection, defaults to True
        :type enable_memory: bool, optional
        :param metric_prefix: Prefix for all metric names, defaults to "user"
        :type metric_prefix: str, optional
        """

        if not Telemetry:
            raise RuntimeError("Dragon Telemetry not available")

        if not AccVendor:
            raise RuntimeError("Dragon AccVendor not available")

        if not find_accelerators:
            raise RuntimeError("Dragon find_accelerators not available")

        # Configuration
        self._collection_rate = collection_rate
        self._checkpoint_interval = checkpoint_interval
        self._checkpoint_dir = checkpoint_dir
        self._checkpoint_count = checkpoint_count
        self._enable_cpu = enable_cpu
        self._enable_gpu = enable_gpu
        self._enable_memory = enable_memory
        self._metric_prefix = metric_prefix

        # Process management
        self._process_group = None
        self._end_event = None
        self._custom_metric_queue = None
        self._started = False

    def start(self):
        """Start telemetry collection on all nodes.

        Spawns one collector process per node using Dragon's ProcessGroup.
        Each collector monitors CPU, GPU, and memory metrics and sends them
        to Dragon's TSDB while also checkpointing to JSON files.

        :raises RuntimeError: if collector is already started
        """
        if self._started:
            raise RuntimeError("Collector already started")

        # Create shutdown event
        self._end_event = Event()

        # Create custom metric queue
        self._custom_metric_queue = Queue()

        # Get one policy per node (hostname-based)
        sys = System()
        policies = sys.hostname_policies()

        # Create process group
        self._process_group = ProcessGroup(restart=False, pmi=None)

        # Add one collector process per node
        for policy in policies:
            self._process_group.add_process(
                nproc=1,
                template=ProcessTemplate(
                    target=self._collector_main,
                    args=(
                        self._end_event,
                        self._custom_metric_queue,
                        self._collection_rate,
                        self._checkpoint_interval,
                        self._checkpoint_dir,
                        self._checkpoint_count,
                        self._enable_cpu,
                        self._enable_gpu,
                        self._enable_memory,
                        self._metric_prefix,
                    ),
                    policy=policy,
                ),
            )

        # Initialize and start
        self._process_group.init()
        self._process_group.start()
        self._started = True

        print(f"DragonTelemetryCollector: Started collection on {len(policies)} node(s)")

    def stop(self, timeout: float = 10.0):
        """Stop telemetry collection with clean shutdown.

        Triggers a final checkpoint before exiting and waits for all
        collector processes to finish.

        :param timeout: Seconds to wait for processes to finish, defaults to 10.0
        :type timeout: float, optional
        """
        if not self._started:
            return

        print("DragonTelemetryCollector: Stopping collection...")

        # Signal shutdown
        self._end_event.set()

        # Wait for processes to complete
        try:
            self._process_group.join(timeout=timeout)
        except TimeoutError:
            print(f"Warning: Processes did not finish within {timeout}s")

        # Cleanup
        self._process_group.close()
        self._custom_metric_queue.close()

        self._started = False
        print("DragonTelemetryCollector: Collection stopped")

    def add_custom_metric(self, metric_name: str, value: float):
        """Add a custom metric to be collected on all nodes.

        The metric will be sent to Dragon's TSDB and included in checkpoints.

        :param metric_name: Name of the metric (will be prefixed with metric_prefix)
        :type metric_name: str
        :param value: Metric value
        :type value: float
        :raises RuntimeError: if collector is not started
        """
        if not self._started:
            raise RuntimeError("Collector not started. Call start() first.")

        # Broadcast to all collector processes
        self._custom_metric_queue.put(
            {"metric_name": metric_name, "value": value, "timestamp": int(time.time())}
        )

    @staticmethod
    def _collector_main(
        end_event,
        custom_metric_queue,
        collection_rate,
        checkpoint_interval,
        checkpoint_dir,
        checkpoint_count,
        enable_cpu,
        enable_gpu,
        enable_memory,
        metric_prefix,
    ):
        """Main collector process - runs on each node.

        This is the entry point for each collector process spawned per node.
        """
        # Initialize telemetry client
        dt = Telemetry()
        hostname = socket.gethostname()

        # Thread-safe storage for all collected data (for checkpointing)
        checkpoint_lock = threading.Lock()
        all_metrics_history = []

        # Start checkpoint thread
        checkpoint_thread = threading.Thread(
            target=DragonTelemetryCollector._checkpoint_worker,
            args=(
                end_event,
                checkpoint_lock,
                all_metrics_history,
                hostname,
                checkpoint_interval,
                checkpoint_dir,
                checkpoint_count,
                collection_rate,
                metric_prefix,
            ),
            daemon=True,
        )
        checkpoint_thread.start()

        # Main collection loop
        last_collection = time.time()

        print(f"DragonTelemetryCollector: Collector started on {hostname}")

        while not end_event.is_set():
            current_time = time.time()

            # Collect at specified rate
            if current_time - last_collection >= collection_rate:
                timestamp = int(current_time)
                metrics_collected = {}

                # Collect CPU metrics
                if enable_cpu:
                    try:
                        cpu_data = DragonTelemetryCollector._collect_cpu_metrics()
                        metrics_collected.update(cpu_data)
                        for metric_name, value in cpu_data.items():
                            dt.add_data(
                                ts_metric_name=f"{metric_prefix}.{metric_name}",
                                ts_data=value,
                                timestamp=timestamp,
                            )
                    except Exception as e:
                        print(f"Warning: Failed to collect CPU metrics on {hostname}: {e}")

                # Collect memory metrics
                if enable_memory:
                    try:
                        mem_data = DragonTelemetryCollector._collect_memory_metrics()
                        metrics_collected.update(mem_data)
                        for metric_name, value in mem_data.items():
                            dt.add_data(
                                ts_metric_name=f"{metric_prefix}.{metric_name}",
                                ts_data=value,
                                timestamp=timestamp,
                            )
                    except Exception as e:
                        print(f"Warning: Failed to collect memory metrics on {hostname}: {e}")

                # Collect GPU metrics
                if enable_gpu:
                    try:
                        gpu_data = DragonTelemetryCollector._collect_gpu_metrics(
                            dt, timestamp, metric_prefix
                        )
                        metrics_collected.update(gpu_data)
                    except Exception as e:
                        print(f"Warning: Failed to collect GPU metrics on {hostname}: {e}")

                # Collect custom user metrics from queue
                try:
                    while True:
                        msg = custom_metric_queue.get_nowait()
                        metric_name = msg["metric_name"]
                        value = msg["value"]
                        msg_timestamp = msg.get("timestamp", timestamp)

                        dt.add_data(
                            ts_metric_name=f"{metric_prefix}.{metric_name}",
                            ts_data=value,
                            timestamp=msg_timestamp,
                        )
                        metrics_collected[metric_name] = value
                except queue.Empty:
                    pass
                except Exception as e:
                    print(f"Warning: Failed to collect custom metrics on {hostname}: {e}")

                # Store for checkpointing
                with checkpoint_lock:
                    all_metrics_history.append(
                        {"timestamp": timestamp, "hostname": hostname, "metrics": metrics_collected}
                    )

                last_collection = current_time

            # Sleep briefly to avoid busy-waiting
            time.sleep(0.1)

        # Wait for checkpoint thread to finish
        print(f"DragonTelemetryCollector: Collector on {hostname} shutting down...")
        checkpoint_thread.join(timeout=5)
        print(f"DragonTelemetryCollector: Collector on {hostname} stopped")

    @staticmethod
    def _collect_cpu_metrics():
        """Collect CPU utilization metrics.

        :return: Dictionary of CPU metrics
        :rtype: dict
        """
        load1, load5, load15 = os.getloadavg()
        cpu_percent = psutil.cpu_percent(interval=None)

        return {
            "cpu_percent": cpu_percent,
            "load_average_1m": load1,
            "load_average_5m": load5,
            "load_average_15m": load15,
        }

    @staticmethod
    def _collect_memory_metrics():
        """Collect memory utilization metrics.

        :return: Dictionary of memory metrics
        :rtype: dict
        """
        mem = psutil.virtual_memory()

        return {
            "memory_percent": mem.percent,
            "memory_available_gb": mem.available / (1024**3),
            "memory_used_gb": mem.used / (1024**3),
            "memory_total_gb": mem.total / (1024**3),
        }

    @staticmethod
    def _collect_gpu_metrics(dt, timestamp, metric_prefix):
        """Collect GPU metrics for all GPUs on this node.

        :param dt: Telemetry client instance
        :type dt: Telemetry
        :param timestamp: Current timestamp
        :type timestamp: int
        :param metric_prefix: Metric name prefix
        :type metric_prefix: str
        :return: Dictionary of GPU metrics
        :rtype: dict
        """
        gpu_metrics = {}

        try:
            # Detect GPU vendor and count
            vendor, gpu_count = DragonTelemetryCollector._identify_gpu()

            if vendor is None or gpu_count == 0:
                return gpu_metrics

            # Collect based on vendor
            if vendor == AccVendor.NVIDIA:
                for i in range(gpu_count):
                    try:
                        metrics = DragonTelemetryCollector._get_nvidia_metrics(i)
                        for metric_name, value in metrics.items():
                            dt.add_data(
                                ts_metric_name=f"{metric_prefix}.gpu_{metric_name}",
                                ts_data=value,
                                timestamp=timestamp,
                                tagk="gpu",
                                tagv=str(i),
                            )
                            gpu_metrics[f"gpu_{i}_{metric_name}"] = value
                    except Exception as e:
                        print(f"Warning: Failed to collect GPU {i} metrics: {e}")

            elif vendor == AccVendor.AMD:
                for i in range(gpu_count):
                    try:
                        metrics = DragonTelemetryCollector._get_amd_metrics(i)
                        for metric_name, value in metrics.items():
                            dt.add_data(
                                ts_metric_name=f"{metric_prefix}.gpu_{metric_name}",
                                ts_data=value,
                                timestamp=timestamp,
                                tagk="gpu",
                                tagv=str(i),
                            )
                            gpu_metrics[f"gpu_{i}_{metric_name}"] = value
                    except Exception as e:
                        print(f"Warning: Failed to collect GPU {i} metrics: {e}")

            elif vendor == AccVendor.INTEL:
                try:
                    all_metrics = DragonTelemetryCollector._get_intel_metrics()
                    for gpu_id, metrics in all_metrics.items():
                        for metric_name, value in metrics.items():
                            dt.add_data(
                                ts_metric_name=f"{metric_prefix}.gpu_{metric_name}",
                                ts_data=value,
                                timestamp=timestamp,
                                tagk="gpu",
                                tagv=str(gpu_id),
                            )
                            gpu_metrics[f"gpu_{gpu_id}_{metric_name}"] = value
                except Exception as e:
                    print(f"Warning: Failed to collect Intel GPU metrics: {e}")

        except Exception as e:
            print(f"Warning: GPU detection failed: {e}")

        return gpu_metrics

    @staticmethod
    def _identify_gpu():
        """Identify GPU vendor and count.

        :return: Tuple of (vendor, count) or (None, 0)
        :rtype: tuple
        """
        try:
            accelerator = find_accelerators()
            if accelerator is None:
                return None, 0

            vendor = accelerator.vendor

            if vendor == AccVendor.NVIDIA:
                import pynvml

                pynvml.nvmlInit()
                count = pynvml.nvmlDeviceGetCount()
                pynvml.nvmlShutdown()
                return vendor, count

            elif vendor == AccVendor.AMD:
                import sys

                rocm_path = os.path.join(
                    os.environ.get("ROCM_PATH", "/opt/rocm/"), "libexec/rocm_smi"
                )
                sys.path.append(rocm_path)
                import rocm_smi

                rocm_smi.initializeRsmi()
                count = len(rocm_smi.listDevices())
                return vendor, count

            elif vendor == AccVendor.INTEL:
                # strip out tiling
                count = len(set(map(int, accelerator.device_list)))
                return vendor, count

            return None, 0

        except Exception:
            return None, 0

    @staticmethod
    def _get_nvidia_metrics(gpu_id):
        """Collect metrics for a single NVIDIA GPU.

        :param gpu_id: GPU device ID
        :type gpu_id: int
        :return: Dictionary of GPU metrics
        :rtype: dict
        """
        import pynvml

        pynvml.nvmlInit()
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(gpu_id)

            # Utilization
            util = pynvml.nvmlDeviceGetUtilizationRates(handle)

            # Memory
            mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
            mem_percent = (mem.used / mem.total) * 100

            # Power (optional)
            try:
                power_mw = pynvml.nvmlDeviceGetPowerUsage(handle)
                power_w = power_mw / 1000.0
            except Exception:
                power_w = 0.0

            return {
                "utilization": util.gpu,
                "memory_percent": mem_percent,
                "memory_used_gb": mem.used / (1024**3),
                "memory_total_gb": mem.total / (1024**3),
                "power_watts": power_w,
            }
        finally:
            pynvml.nvmlShutdown()

    @staticmethod
    def _get_amd_metrics(gpu_id):
        """Collect metrics for a single AMD GPU.

        :param gpu_id: GPU device ID
        :type gpu_id: int
        :return: Dictionary of GPU metrics
        :rtype: dict
        """

        rocm_path = os.path.join(os.environ.get("ROCM_PATH", "/opt/rocm/"), "libexec/rocm_smi")
        sys.path.append(rocm_path)
        import rocm_smi

        rocm_smi.initializeRsmi()

        utilization_gpu = rocm_smi.getGpuUse(gpu_id)
        mem_used, mem_total = rocm_smi.getMemInfo(gpu_id, "vram")
        mem_percent = (float(mem_used) / float(mem_total)) * 100

        power = rocm_smi.getPower(gpu_id).get("power", 0.0)
        if power == "N/A":
            power = 0.0
        else:
            power = float(power)

        return {
            "utilization": utilization_gpu,
            "memory_percent": mem_percent,
            "memory_used_gb": mem_used / (1024**3),
            "memory_total_gb": mem_total / (1024**3),
            "power_watts": power,
        }

    @staticmethod
    def _get_intel_metrics():
        """Collect metrics for all Intel GPUs using xpu-smi.

        :return: Dictionary mapping gpu_id to metrics dict
        :rtype: dict
        """
        import subprocess

        # Run xpu-smi to get metrics for all GPUs
        output = subprocess.run(
            ["xpu-smi", "dump", "-d", "-1", "-m", "0,1,5", "-n", "1"],
            text=True,
            capture_output=True,
        )
        output_string = output.stdout
        output_lines = output_string.splitlines()

        all_gpu_metrics = {}

        # Parse output
        for device_stats in output_lines[1:]:
            device_info = device_stats.split(",")
            device_id = int(device_info[1].strip())

            utilization_gpu = 0 if device_info[2].strip() == "N/A" else float(device_info[2])
            mem_percent = 0 if device_info[4].strip() == "N/A" else float(device_info[4])
            power_w = 0 if device_info[3].strip() == "N/A" else float(device_info[3])

            all_gpu_metrics[device_id] = {
                "utilization": utilization_gpu,
                "memory_percent": mem_percent,
                "power_watts": power_w,
            }

        return all_gpu_metrics

    @staticmethod
    def _checkpoint_worker(
        end_event,
        checkpoint_lock,
        all_metrics_history,
        hostname,
        checkpoint_interval,
        checkpoint_dir,
        checkpoint_count,
        collection_rate,
        metric_prefix,
    ):
        """Background thread that periodically writes checkpoints.

        This runs as a daemon thread in each collector process.
        """
        checkpoint_id = 0
        last_checkpoint = time.time()

        while not end_event.is_set():
            current_time = time.time()

            # Check if it's time to checkpoint
            if current_time - last_checkpoint >= checkpoint_interval:
                # Acquire lock and copy data
                with checkpoint_lock:
                    metrics_to_save = list(all_metrics_history)
                    all_metrics_history.clear()

                # Write checkpoint (without holding lock)
                if metrics_to_save:
                    DragonTelemetryCollector._write_checkpoint(
                        hostname,
                        checkpoint_id,
                        metrics_to_save,
                        checkpoint_dir,
                        checkpoint_count,
                        collection_rate,
                        metric_prefix,
                    )
                    checkpoint_id += 1

                last_checkpoint = current_time

            # Sleep briefly
            time.sleep(1)

        # Final checkpoint on shutdown
        with checkpoint_lock:
            if all_metrics_history:
                DragonTelemetryCollector._write_checkpoint(
                    hostname,
                    checkpoint_id,
                    all_metrics_history,
                    checkpoint_dir,
                    checkpoint_count,
                    collection_rate,
                    metric_prefix,
                )

    @staticmethod
    def _write_checkpoint(
        hostname,
        checkpoint_id,
        metrics_data,
        checkpoint_dir,
        checkpoint_count,
        collection_rate,
        metric_prefix,
    ):
        """Write checkpoint to JSON file with atomic rename pattern.

        :param hostname: Hostname of this node
        :type hostname: str
        :param checkpoint_id: Sequential checkpoint ID
        :type checkpoint_id: int
        :param metrics_data: List of metric dictionaries
        :type metrics_data: list
        :param checkpoint_dir: Directory to write checkpoint
        :type checkpoint_dir: str
        :param checkpoint_count: Max checkpoint files to keep
        :type checkpoint_count: int
        :param collection_rate: Collection rate for metadata
        :type collection_rate: float
        :param metric_prefix: Metric prefix for metadata
        :type metric_prefix: str
        """
        try:
            timestamp = int(time.time())
            filename = f"telemetry_checkpoint_{hostname}_{timestamp}.json"
            filepath = os.path.join(checkpoint_dir, filename)

            # Ensure directory exists
            os.makedirs(checkpoint_dir, exist_ok=True)

            # Prepare checkpoint data
            checkpoint_data = {
                "checkpoint_metadata": {
                    "hostname": hostname,
                    "timestamp": timestamp,
                    "collection_rate": collection_rate,
                    "metric_prefix": metric_prefix,
                    "checkpoint_id": checkpoint_id,
                    "num_samples": len(metrics_data),
                },
                "metrics": metrics_data,
            }

            # Write to temp file first (atomic write pattern)
            temp_fd, temp_path = tempfile.mkstemp(
                dir=checkpoint_dir, prefix=f"tmp_checkpoint_{hostname}_", suffix=".json"
            )

            try:
                with os.fdopen(temp_fd, "w") as f:
                    json.dump(checkpoint_data, f, indent=2)

                # Atomic rename
                os.rename(temp_path, filepath)

                print(
                    f"DragonTelemetryCollector: Wrote checkpoint {filepath} ({len(metrics_data)} samples)"
                )

            except Exception:
                # Clean up temp file on error
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
                raise

            # Rotate old files
            DragonTelemetryCollector._rotate_checkpoints(hostname, checkpoint_dir, checkpoint_count)

        except Exception as e:
            print(f"Warning: Failed to write checkpoint on {hostname}: {e}")

    @staticmethod
    def _rotate_checkpoints(hostname, checkpoint_dir, checkpoint_count):
        """Remove old checkpoint files beyond the count limit.

        :param hostname: Hostname to filter checkpoint files
        :type hostname: str
        :param checkpoint_dir: Directory containing checkpoints
        :type checkpoint_dir: str
        :param checkpoint_count: Max files to keep (0 for unlimited)
        :type checkpoint_count: int
        """
        if checkpoint_count <= 0:
            return

        try:
            # Find all checkpoint files for this hostname
            pattern = os.path.join(checkpoint_dir, f"telemetry_checkpoint_{hostname}_*.json")
            checkpoint_files = sorted(glob.glob(pattern))

            # Remove oldest files if beyond limit
            while len(checkpoint_files) > checkpoint_count:
                oldest = checkpoint_files.pop(0)
                try:
                    os.remove(oldest)
                    print(f"DragonTelemetryCollector: Removed old checkpoint {oldest}")
                except OSError as e:
                    print(f"Warning: Failed to remove old checkpoint {oldest}: {e}")

        except Exception as e:
            print(f"Warning: Failed to rotate checkpoints for {hostname}: {e}")
