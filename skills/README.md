# RHAPSODY Agent Skills

These are [Agent Skills](https://agentskills.io) that teach AI coding agents how to use RHAPSODY. An agent such as Claude Code, Codex, Cursor or Gemini CLI loads a skill when a request matches the skill's description, follows its steps, and uses the bundled scripts and templates.

The skills guide the agent and point it to RHAPSODY's own documentation, examples and source code, which stay the source of truth. Nothing in them is tied to a particular site or machine. Values such as node counts, paths or scheduler settings come from the target environment.

## Skills

| Skill | Use it for | Status |
|---|---|---|
| [`rhapsody-execution`](rhapsody-execution/SKILL.md) | Choose and configure a backend (Concurrent, Dask, Dragon, RADICAL-Pilot, Orbit), define and submit `ComputeTask`s, order stages, handle failures and retries, read results, set per-task resources | Released (RHAPSODY 0.6.0) |
| `rhapsody-telemetry` | Enable, monitor and export telemetry; diagnose runs | Planned |
| `rhapsody-performance` | Throughput and overhead experiments, strong/weak scaling, comparing configurations | Planned |
| `rhapsody-ai-hpc` | `AITask` and inference backends, coupling simulation with inference | Planned |

**AsyncFlow:** workflow graphs (DAGs) belong to AsyncFlow, which will ship its own skills. The RHAPSODY skills cover the Session API only.

```
rhapsody-execution/
├── SKILL.md                      # instructions the agent loads
├── references/backends.md        # per-backend constructor and per-task options
├── references/troubleshooting.md # symptom → cause → fix
├── scripts/rhapsody_env.py       # environment report + live API signatures
└── assets/workflow_template.py   # runnable prepare → fan-out → fan-in program
```

## Install

Each skill is a self-contained folder; install it by putting that folder in a directory your agent scans for skills. Copy the folder, or symlink it to stay in sync with this checkout.

```bash
# Claude Code: personal skills (or <project>/.claude/skills/ for one project)
mkdir -p ~/.claude/skills
ln -s "$(pwd)/skills/rhapsody-execution" ~/.claude/skills/rhapsody-execution

# Other clients: use the skills directory your client documents, e.g.
ln -s "$(pwd)/skills/rhapsody-execution" ~/.agents/skills/rhapsody-execution
```

Skills cite RHAPSODY files as `$RHAPSODY_REPO/<path>`, the root of a RHAPSODY checkout. The agent finds the files in one of three ways:

- **Inside the repository** (or through a symlink back to it): `$RHAPSODY_REPO` is two directories above the skill.
- **From a copy elsewhere:** set `export RHAPSODY_REPO=/path/to/rhapsody`.
- **With no checkout:** the agent reads the same paths on GitHub at the tag that matches the installed version.

Install RHAPSODY itself in the environment where tasks will run:

```bash
pip install rhapsody-py                 # plus extras as needed: [dask], [dragon], [telemetry], ...
```

## Use

Ask in plain language. The agent selects the skill from its description:

- "Run my ensemble of 64 simulations with RHAPSODY on this machine and collect the results."
- "Port this script to run with RHAPSODY's Dask backend on 8 workers."
- "Turn this pipeline into stages: preprocess, then 100 simulations, then one analysis."
- "My RHAPSODY tasks fail with exit code 127. Find out why."

In Claude Code you can also invoke the skill directly with `/rhapsody-execution`.

## Versioning and maintenance

**Versioning**
- Each skill records the RHAPSODY version it was validated against: `metadata.rhapsody-version` in `SKILL.md`.
- Skills tell agents to prefer the installed code over prose, and to check signatures with `scripts/rhapsody_env.py --api`.

**Checks after changing RHAPSODY or a skill**

```bash
python skills/tools/check_skills.py          # Agent Skills spec + links + $RHAPSODY_REPO paths exist
pre-commit run --files $(find skills -type f)
```

**What `check_skills.py` covers**
- **Agent Skills spec:** frontmatter keys, the `name` and `description` rules, size limits.
- **Links:** every link stays inside the skill.
- **Repo paths:** every `$RHAPSODY_REPO/...` path still exists. When docs or examples move, the check fails and names the file to update.

## Validation (RHAPSODY 0.6.0)

**Environment:** Linux, single node. Python 3.12.3 with `rhapsody-py` 0.6.0 installed editable from this checkout, plus `[dask,telemetry,dev,dragon]` (dragonhpc 0.15.0). There were no GPUs.

**Executed and passing**

- **Workflow template** (`assets/workflow_template.py`):
  - Concurrent with threads;
  - Concurrent with processes, plus `--telemetry`, which wrote a JSONL file;
  - Dask LocalCluster with `--fail-member 2`: failure, retry and partial result, with exit code 2 as designed;
  - Dragon single-node runtime under `dragon`, with telemetry.
- **Every Python snippet** in `SKILL.md` and `references/` was run verbatim through a harness on Concurrent and Dask. All 25 checks passed:
  - wait modes and exception types;
  - stage ordering;
  - the retry helper;
  - `capture_stdio` paths;
  - cancellation;
  - Dask `client=` ownership, unsatisfiable resources, and `cwd`/`env`;
  - multi-backend routing;
  - the troubleshooting rows: not awaited, no backend, unknown backend, validation errors, timeout message, missing executable, and the missing `__main__` guard with process-based workers.
- **Dragon snippet and task-mode table** were run under the Dragon runtime. All 4 passed:
  - a native function returns the Python value;
  - a function with `process_template` returns its exit code;
  - per-task `cwd`;
  - a 2-rank `process_templates` job with `pmi="none"`.
- **`scripts/rhapsody_env.py`** in text, `--api` and `--json` modes, on Python 3.12 and 3.9.
- **`tools/check_skills.py`** passes. A negative test confirmed it catches a bad name, a broken link and missing `$RHAPSODY_REPO` paths.
- **Repository pre-commit hooks** pass on `skills/`: ruff, ruff-format, docformatter, typos, detect-secrets.

**Static only** (checked against code and installed signatures, not run)
- Multi-node Dragon, MPI jobs with `pmi="pmix"`/`"cray"`, and GPU placement with `Policy`.
- Dask on a batch-scheduler cluster.
- RADICAL-Pilot and Orbit, which were not installed.

**Valid once the corresponding RHAPSODY fix lands**

The skills describe intended behaviour, so two things don't work as documented yet:

- `from rhapsody.backends import OrbitExecutionBackend` is not exported yet. Until then, import it from `rhapsody.backends.execution`.
- `rhapsody_env.py` relies on `rhapsody.discover_backends()`. Until that function checks the real dependencies, it reports `dragon`, `radical_pilot` and `orbit` as available whenever RHAPSODY is installed.
