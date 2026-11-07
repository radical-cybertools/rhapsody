#!/usr/bin/env python3
"""Run the HPC test groups inside one allocation and write one JUnit file.

Each group gets its own launcher and time budget, so a crash or hang costs that group and
not the whole run:

    python3 run_suite.py --backend dragon --junit junit.xml -- -w slurm -t tcp

Everything after "--" goes to the launcher. The exit code is 0 once the JUnit file is
written, because failures are reported in that file. A launcher that crashes or runs over
budget appears there as a failed test named "<group>.launcher".

This script uses only the standard library and depends only on pytest's JUnit output.
"""

import argparse
import dataclasses
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

HERE = pathlib.Path(__file__).resolve().parent
TAIL_LINES = 100
STOP_GRACE_SECONDS = 10
CLEANUP_TIMEOUT_SECONDS = 120


@dataclasses.dataclass
class Group:
    name: str
    test_file: str
    budget: int  # seconds
    pytest_args: tuple = ()


# Cheapest first, so the riskiest groups can't stop the others from running. Budgets total
# 14 minutes to fit a 15-minute job.
GROUPS = {
    "dragon": [
        Group("tasks", "dragon/test_tasks.py", 180),
        Group("placement", "dragon/test_placement.py", 180),
        Group("scale", "dragon/test_scale.py", 240, ("-m", "scale")),
        # Stop at the first failure: a hung MPI test would otherwise use the whole budget,
        # and pytest writes its results only when it finishes.
        Group("mpi", "dragon/test_mpi.py", 240, ("--maxfail=1",)),
    ],
}


def read_tail(path):
    try:
        lines = pathlib.Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return "(no log)"
    return "\n".join(lines[-TAIL_LINES:])


def read_cases(path):
    """Return a JUnit file's test cases, or None if it's missing or invalid."""
    try:
        return list(ET.parse(path).getroot().iter("testcase"))
    except (OSError, ET.ParseError):
        return None


def launcher_failure(group, reason, log_path):
    case = ET.Element("testcase", classname=group.name, name="launcher", time="0")
    failure = ET.SubElement(case, "failure", message=reason)
    failure.text = f"{reason}\n\nLog (end):\n{read_tail(log_path)}"
    return case


def run_limited(command, budget, log_path, cwd, env):
    """Run command in its own process group.

    Return True if it finished within budget.
    """
    with open(log_path, "w") as log:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=budget)
            return True
        except subprocess.TimeoutExpired:
            pass
        for sig, grace in ((signal.SIGTERM, STOP_GRACE_SECONDS), (signal.SIGKILL, None)):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                break
            try:
                process.wait(timeout=grace)
                break
            except subprocess.TimeoutExpired:
                continue
    return False


def leftover_steps(job_id):
    output = subprocess.run(
        ["squeue", "-h", "-s", "-j", job_id, "-o", "%i"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    return [s for s in output.split() if not s.endswith((".batch", ".extern", ".interactive"))]


def cancel_slurm_steps():
    """Cancel steps a stopped launcher left behind.

    They would keep the nodes' memory.
    """
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id or not shutil.which("squeue"):
        return
    steps = leftover_steps(job_id)
    if steps:
        subprocess.run(["scancel", *steps], check=False)
    deadline = time.monotonic() + 60
    while steps and time.monotonic() < deadline:
        time.sleep(2)
        steps = leftover_steps(job_id)


def clean_up_dragon(log_path, cwd, env):
    """Clear what an abnormal end left, so the next group can start."""
    cancel_slurm_steps()
    # Dragon's default cleanup uses SSH between nodes, which many machines don't allow.
    # srun reaches every node without it.
    command = ["dragon-cleanup", "-s"]
    nodes = os.environ.get("SLURM_NNODES")
    if nodes and shutil.which("srun"):
        command = ["srun", "--overlap", f"-N{nodes}", "--ntasks-per-node=1", *command]
    run_limited(command, CLEANUP_TIMEOUT_SECONDS, log_path, cwd, env)


def run_group(group, launcher_args, budget, work_dir, env):
    """Run one group and return its test cases, including any launcher failure."""
    junit = work_dir / f"{group.name}.xml"
    log = work_dir / f"{group.name}.log"
    junit.unlink(missing_ok=True)
    command = [
        "dragon",
        *launcher_args,
        sys.executable,
        "-m",
        "pytest",
        str(HERE / group.test_file),
        "-v",
        *group.pytest_args,
        f"--junitxml={junit}",
    ]
    print(f"=== group {group.name}: budget {budget} s", flush=True)
    start = time.monotonic()
    finished = run_limited(command, budget, log, work_dir, env)
    cases = read_cases(junit)

    # The launcher's exit code can't be used: dragon returned 0 when the application failed
    # or was killed. Missing results are the reliable sign of a crash.
    problem = None
    if not finished:
        problem = f"group {group.name} exceeded its budget of {budget} s"
    elif cases is None:
        problem = f"launcher ended without results for group {group.name}"
    if problem:
        clean_up_dragon(work_dir / f"{group.name}-cleanup.log", work_dir, env)

    print(read_tail(log), flush=True)
    print(
        f"=== group {group.name}: {time.monotonic() - start:.0f} s, "
        f"{len(cases or [])} tests, {problem or 'launcher ok'}",
        flush=True,
    )
    cases = list(cases or [])
    if problem:
        cases.append(launcher_failure(group, problem, log))
    return cases


def write_junit(path, cases):
    suite = ET.Element(
        "testsuite",
        name="hpc",
        tests=str(len(cases)),
        failures=str(sum(c.find("failure") is not None for c in cases)),
        errors=str(sum(c.find("error") is not None for c in cases)),
        skipped=str(sum(c.find("skipped") is not None for c in cases)),
    )
    suite.extend(cases)
    temporary = pathlib.Path(f"{path}.tmp")
    root = ET.Element("testsuites")
    root.append(suite)
    ET.ElementTree(root).write(temporary, encoding="utf-8", xml_declaration=True)
    temporary.replace(path)


def parse_args(argv):
    launcher_args = []
    if "--" in argv:
        split = argv.index("--")
        argv, launcher_args = argv[:split], argv[split + 1 :]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--backend", required=True, choices=sorted(GROUPS))
    parser.add_argument("--junit", required=True, type=pathlib.Path)
    parser.add_argument("--groups", help="comma-separated groups to run; default all")
    parser.add_argument(
        "--budget",
        action="append",
        default=[],
        metavar="GROUP=SECONDS",
        help="override a group's time budget; can be repeated",
    )
    args = parser.parse_args(argv)
    args.launcher_args = launcher_args
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    groups = GROUPS[args.backend]
    known = {g.name for g in groups}
    wanted = args.groups.split(",") if args.groups else [g.name for g in groups]
    budgets = {g.name: g.budget for g in groups}
    for item in args.budget:
        name, _, seconds = item.partition("=")
        if name not in known or not seconds.isdigit():
            raise SystemExit(f"invalid --budget {item!r}")
        budgets[name] = int(seconds)
    unknown = set(wanted) - known
    if unknown:
        raise SystemExit(f"unknown groups: {sorted(unknown)}; known: {sorted(known)}")

    junit = args.junit.resolve()
    work_dir = junit.parent / "groups"
    work_dir.mkdir(parents=True, exist_ok=True)

    suite_dir = HERE / args.backend
    env = os.environ | {"RHAPSODY_HPC": "1"}
    # Workers import hpc_workers by name, so the suite directory must be on the path.
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{suite_dir}{os.pathsep}{existing}" if existing else str(suite_dir)

    cases = []
    for group in groups:
        if group.name in wanted:
            cases += run_group(group, args.launcher_args, budgets[group.name], work_dir, env)
    write_junit(junit, cases)
    print(f"=== wrote {junit}: {len(cases)} test results", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
