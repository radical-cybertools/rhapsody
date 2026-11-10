#!/usr/bin/env python3
"""Report what RHAPSODY can do in the current Python environment.

Prints the RHAPSODY version, the execution backends reported by
``rhapsody.discover_backends()``, telemetry dependencies, launchers on PATH and visible GPUs.
With ``--api`` it also prints the live signatures of the public API, so an agent works from the
installed version rather than from documentation.

Usage::

    python rhapsody_env.py            # human-readable report
    python rhapsody_env.py --api      # also print live API signatures
    python rhapsody_env.py --json     # machine-readable report

Exit status: 0 if ``rhapsody`` imports, 1 otherwise.
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import os
import platform
import shutil
import sys

# pip extra that provides each backend (see pyproject.toml [project.optional-dependencies]).
EXTRAS = {
    "concurrent": None,
    "noop": None,
    "dask": "dask",
    "dragon": "dragon",
    "orbit": "orbit",
}

API = {
    "Session": ("rhapsody.api", "Session", "__init__"),
    "Session.submit_tasks": ("rhapsody.api", "Session", "submit_tasks"),
    "Session.wait_tasks": ("rhapsody.api", "Session", "wait_tasks"),
    "Session.start_telemetry": ("rhapsody.api", "Session", "start_telemetry"),
    "ComputeTask": ("rhapsody.api", "ComputeTask", "__init__"),
    "AITask": ("rhapsody.api", "AITask", "__init__"),
    "ConcurrentExecutionBackend": (
        "rhapsody.backends.execution.concurrent",
        "ConcurrentExecutionBackend",
        "__init__",
    ),
    "DaskExecutionBackend": (
        "rhapsody.backends.execution.dask_parallel",
        "DaskExecutionBackend",
        "__init__",
    ),
    "DragonExecutionBackend": (
        "rhapsody.backends.execution.dragon",
        "DragonExecutionBackend",
        "__init__",
    ),
    "OrbitExecutionBackend": (
        "rhapsody.backends.execution.orbit",
        "OrbitExecutionBackend",
        "__init__",
    ),
}


def _importable(module: str) -> bool:
    try:
        importlib.import_module(module)
    except Exception:  # noqa: BLE001 - any import failure means "not available"
        return False
    return True


def _gpus() -> dict:
    info: dict = {
        "nvidia-smi": bool(shutil.which("nvidia-smi")),
        "rocm-smi": bool(shutil.which("rocm-smi")),
        "nvml_devices": None,
    }
    try:
        import pynvml  # type: ignore[import-not-found]

        pynvml.nvmlInit()
        info["nvml_devices"] = int(pynvml.nvmlDeviceGetCount())
        pynvml.nvmlShutdown()
    except Exception as exc:  # noqa: BLE001 - no NVML is a normal outcome
        info["nvml_devices"] = f"unavailable ({type(exc).__name__})"
    return info


def collect() -> dict:
    report: dict = {
        "python": {"version": platform.python_version(), "executable": sys.executable},
        "rhapsody": {"importable": False},
        "backends": {},
        "telemetry": {},
        "launchers": {},
        "gpu": _gpus(),
        "notes": [],
    }
    try:
        import rhapsody
    except Exception as exc:  # noqa: BLE001
        report["rhapsody"]["error"] = f"{type(exc).__name__}: {exc}"
        report["notes"].append("Install RHAPSODY: pip install rhapsody-py (Python >= 3.11).")
        return report

    report["rhapsody"] = {
        "importable": True,
        "version": rhapsody.__version__,
        "location": rhapsody.__file__,
    }
    if sys.version_info < (3, 11):
        report["notes"].append("RHAPSODY requires Python >= 3.11; this interpreter is older.")

    for name, available in sorted(rhapsody.discover_backends().items()):
        extra = EXTRAS.get(name)
        report["backends"][name] = {
            "available": bool(available),
            "install": f'pip install "rhapsody-py[{extra}]"' if extra else None,
        }

    for module in ("opentelemetry.sdk", "psutil", "pynvml"):
        report["telemetry"][module] = _importable(module)

    # PATH first, then this interpreter's bin/ (a venv's launchers live there even when the
    # venv is not activated).
    search = os.pathsep.join([os.environ.get("PATH", ""), os.path.dirname(sys.executable)])
    for exe in ("dragon", "redis-server", "srun", "mpirun", "flux"):
        report["launchers"][exe] = shutil.which(exe, path=search)

    if report["backends"].get("dragon", {}).get("available"):
        report["notes"].append(
            "Dragon: start scripts with the 'dragon' launcher (dragon my_script.py) and call "
            "multiprocessing.set_start_method('dragon') before creating the backend."
        )
    if not report["telemetry"]["opentelemetry.sdk"]:
        report["notes"].append("Telemetry needs: pip install 'rhapsody-py[telemetry]'.")
    return report


def api_signatures() -> dict:
    out = {}
    for label, (module, cls, attr) in API.items():
        try:
            obj = getattr(getattr(importlib.import_module(module), cls), attr)
            sig = str(inspect.signature(obj))
            out[label] = sig.replace("(self, ", "(").replace("(self)", "()")
        except Exception as exc:  # noqa: BLE001 - report, do not fail
            out[label] = f"<unavailable: {type(exc).__name__}>"
    return out


def print_report(report: dict, api: dict | None) -> None:
    py = report["python"]
    print(f"Python    {py['version']}  ({py['executable']})")
    rh = report["rhapsody"]
    if not rh["importable"]:
        print(f"RHAPSODY  NOT IMPORTABLE: {rh.get('error')}")
    else:
        print(f"RHAPSODY  {rh['version']}  ({rh['location']})")
        print("\nExecution backends:")
        for name, b in report["backends"].items():
            hint = "" if b["available"] or not b["install"] else f"   -> {b['install']}"
            print(f"  {'yes' if b['available'] else 'no ':<4}{name}{hint}")
        tel = ", ".join(f"{k}={'yes' if v else 'no'}" for k, v in report["telemetry"].items())
        print(f"\nTelemetry {tel}")
        launchers = ", ".join(f"{k}={'yes' if v else 'no'}" for k, v in report["launchers"].items())
        print(f"Launchers {launchers}")
    g = report["gpu"]
    print(
        f"GPUs      nvidia-smi={'yes' if g['nvidia-smi'] else 'no'}, "
        f"rocm-smi={'yes' if g['rocm-smi'] else 'no'}, NVML devices={g['nvml_devices']}"
    )
    for note in report["notes"]:
        print(f"NOTE: {note}")
    if api:
        print("\nLive API signatures (installed version):")
        for label, sig in api.items():
            print(f"  {label}{sig}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--api", action="store_true", help="include live API signatures")
    ap.add_argument("--json", action="store_true", help="emit JSON")
    args = ap.parse_args()

    report = collect()
    api = api_signatures() if args.api and report["rhapsody"]["importable"] else None
    if args.json:
        if api is not None:
            report["api"] = api
        print(json.dumps(report, indent=2, default=str))
    else:
        print_report(report, api)
    return 0 if report["rhapsody"]["importable"] else 1


if __name__ == "__main__":
    sys.exit(main())
