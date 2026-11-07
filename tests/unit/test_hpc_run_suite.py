"""Offline tests for tests/integration/test-hpc/run_suite.py, using fake launcher commands."""

import importlib.util
import os
import pathlib
import stat
import xml.etree.ElementTree as ET

import pytest

RUNNER = pathlib.Path(__file__).parents[1] / "integration" / "test-hpc" / "run_suite.py"

# Each group's behaviour comes from FAKE_<group>, read by the fake dragon below.
FAKE_DRAGON = """#!/usr/bin/env python3
import os, pathlib, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_CALLS"], "a") as calls:
    calls.write(" ".join(args) + "\\n")
target = next(a for a in args if a.endswith(".py") or "test_" in a)
group = pathlib.Path(target).stem.replace("test_", "")
junit = next(a.split("=", 1)[1] for a in args if a.startswith("--junitxml="))
behaviour = os.environ.get(f"FAKE_{group}", "pass")
case = f'<testcase classname="test_{group}" name="test_a" time="1"'
print(f"fake log for {group}", flush=True)
if behaviour == "hang":
    time.sleep(600)
if behaviour == "crash":
    sys.exit(0)  # dragon exits 0 even when the application dies
if behaviour == "corrupt":
    pathlib.Path(junit).write_text("<testsuites><oops")
    sys.exit(0)
body = {
    "pass": case + " />",
    "fail": case + '><failure message="boom">traceback</failure></testcase>',
    "skip": case + '><skipped message="no">why</skipped></testcase>',
}[behaviour]
pathlib.Path(junit).write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
"""

RECORD = """#!/bin/sh
echo "$(basename "$0") $*" >> "$FAKE_CALLS"
"""


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location("run_suite", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fake_bin(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    def write(name, content):
        path = bin_dir / name
        path.write_text(content)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)

    write("dragon", FAKE_DRAGON)
    write("dragon-cleanup", RECORD)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_CALLS", str(tmp_path / "calls"))
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)
    monkeypatch.delenv("SLURM_NNODES", raising=False)
    (tmp_path / "calls").touch()
    return write


def run(runner, tmp_path, *extra):
    junit = tmp_path / "junit.xml"
    code = runner.main(["--backend", "dragon", "--junit", str(junit), *extra])
    assert code == 0
    results = {}
    for case in ET.parse(junit).getroot().iter("testcase"):
        name = f"{case.get('classname')}.{case.get('name')}"
        kinds = [c.tag for c in case if c.tag in ("failure", "error", "skipped")]
        results[name] = kinds[0] if kinds else "passed"
    return results, ET.parse(junit).getroot()


def calls(tmp_path):
    return (tmp_path / "calls").read_text().splitlines()


def test_all_groups_pass_and_merge(runner, fake_bin, tmp_path):
    results, root = run(runner, tmp_path)
    assert results == {
        "test_tasks.test_a": "passed",
        "test_placement.test_a": "passed",
        "test_scale.test_a": "passed",
        "test_mpi.test_a": "passed",
    }
    assert root.find("testsuite").get("tests") == "4"


def test_groups_run_cheapest_first_with_launcher_args(runner, fake_bin, tmp_path):
    run(runner, tmp_path, "--", "-w", "slurm", "--network-prefix", "^(a|b)$")
    lines = calls(tmp_path)
    assert [
        next(p for p in ("tasks", "placement", "scale", "mpi") if f"test_{p}.py" in line)
        for line in lines
    ] == ["tasks", "placement", "scale", "mpi"]
    assert all(line.startswith("-w slurm --network-prefix ^(a|b)$ ") for line in lines)
    assert "-m scale" in lines[2]
    assert "--maxfail=1" in lines[3]


def test_test_failure_is_not_a_launcher_failure(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_placement", "fail")
    results, root = run(runner, tmp_path)
    assert results["test_placement.test_a"] == "failure"
    assert "placement.launcher" not in results
    assert root.find("testsuite").get("failures") == "1"
    assert not [line for line in calls(tmp_path) if line.startswith("dragon-cleanup")]


def test_skips_are_kept(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_mpi", "skip")
    results, _ = run(runner, tmp_path)
    assert results["test_mpi.test_a"] == "skipped"


def test_crash_costs_one_group_and_cleans_up(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_placement", "crash")
    results, root = run(runner, tmp_path)
    assert results["placement.launcher"] == "failure"
    assert "test_placement.test_a" not in results
    assert results["test_scale.test_a"] == "passed"
    assert results["test_mpi.test_a"] == "passed"
    assert "dragon-cleanup -s" in calls(tmp_path)
    failure = next(c for c in root.iter("testcase") if c.get("name") == "launcher").find("failure")
    assert "without results" in failure.get("message")
    assert "fake log for placement" in failure.text


def test_invalid_results_count_as_a_crash(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_tasks", "corrupt")
    results, _ = run(runner, tmp_path)
    assert results["tasks.launcher"] == "failure"
    assert results["test_placement.test_a"] == "passed"


def test_over_budget_is_stopped_and_next_group_runs(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_scale", "hang")
    results, root = run(runner, tmp_path, "--budget", "scale=1")
    assert results["scale.launcher"] == "failure"
    assert results["test_mpi.test_a"] == "passed"
    failure = next(c for c in root.iter("testcase") if c.get("name") == "launcher").find("failure")
    assert "exceeded its budget of 1 s" in failure.get("message")


def test_group_selection(runner, fake_bin, tmp_path):
    results, _ = run(runner, tmp_path, "--groups", "mpi,tasks")
    assert set(results) == {"test_tasks.test_a", "test_mpi.test_a"}


@pytest.mark.parametrize(
    "extra", [("--groups", "nope"), ("--budget", "nope=3"), ("--budget", "mpi=soon")]
)
def test_bad_options_are_rejected(runner, fake_bin, tmp_path, extra):
    with pytest.raises(SystemExit):
        run(runner, tmp_path, *extra)


def test_suite_directory_is_on_pythonpath(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/existing")
    fake_bin(
        "dragon",
        FAKE_DRAGON.replace(
            "args = sys.argv[1:]",
            "args = sys.argv[1:]\n"
            "open(os.environ['FAKE_CALLS'] + '.env', 'a')"
            ".write(os.environ['PYTHONPATH'] + ' ' + os.environ['RHAPSODY_HPC'] + chr(10))",
        ),
    )
    run(runner, tmp_path, "--groups", "tasks")
    line = pathlib.Path(f"{tmp_path}/calls.env").read_text().strip()
    assert line == f"{RUNNER.parent / 'dragon'}{os.pathsep}/existing 1"


def test_slurm_steps_are_cancelled_and_cleanup_uses_srun(runner, fake_bin, tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "77")
    monkeypatch.setenv("SLURM_NNODES", "2")
    monkeypatch.setenv("FAKE_tasks", "crash")
    state = tmp_path / "cancelled"
    fake_bin(
        "squeue",
        f'#!/bin/sh\necho 77.batch\necho 77.extern\n[ -e "{state}" ] || echo 77.5\n',
    )
    fake_bin("scancel", f'#!/bin/sh\necho "scancel $*" >> "$FAKE_CALLS"\ntouch "{state}"\n')
    fake_bin("srun", RECORD)
    run(runner, tmp_path, "--groups", "tasks")
    lines = calls(tmp_path)
    assert "scancel 77.5" in lines
    assert "srun --overlap -N2 --ntasks-per-node=1 dragon-cleanup -s" in lines
    assert lines.index("scancel 77.5") < lines.index(
        "srun --overlap -N2 --ntasks-per-node=1 dragon-cleanup -s"
    )
