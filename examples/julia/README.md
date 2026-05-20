# Julia ↔ Rhapsody — feasibility demos

Three minimal prototypes showing the Rhapsody-Julia integration points. The
goal here is *feasibility*, not a polished integration: each demo is the
simplest thing that proves the model works, and every shortcut is flagged
with a comment pointing at what a real integration would do instead.

| # | Demo | Files |
|---|---|---|
| a | Julia script as Rhapsody task     | `hello.jl`, `01_script_task.py` |
| b | Julia driver → Rhapsody           | `02_driver.jl`, `_rhapsody_helper.py` |
| c | Julia function as Rhapsody task   | `mymod.jl`, `call_fn.jl`, `03_function_task.py` |

All three run on `ConcurrentExecutionBackend` out of the box; (a) and (c)
also run unchanged on `DragonExecutionBackendV3` via the `dragon` launcher.

## One-time setup

```bash
# Python side: activate the venv that has rhapsody installed (it must export
# VIRTUAL_ENV -- the standard `source <venv>/bin/activate` does this).
# Julia side: install the demo deps (JSON, PythonCall) into the project env.
julia --project=examples/julia -e 'using Pkg; Pkg.instantiate()'
```

`02_driver.jl` reads `VIRTUAL_ENV` and points PythonCall at that
interpreter, so no extra env vars are needed at the shell.

## Running

```bash
# (a) script as task
python examples/julia/01_script_task.py
dragon examples/julia/01_script_task.py dragon

# (b) julia driver -> rhapsody (Concurrent only -- see Notes)
julia --project=examples/julia examples/julia/02_driver.jl

# (c) function as task
python examples/julia/03_function_task.py
dragon examples/julia/03_function_task.py dragon
```

Pass `dragon` as a CLI arg to (a) / (c) to switch from the default
`ConcurrentExecutionBackend` to `DragonExecutionBackendV3`.

## Notes / deferred work

- **(c) startup cost.** Each call spawns a fresh `julia` process and pays the
  JIT / package-load tax (~0.3-1 s). The standard fix is to precompile a
  sysimage with `PackageCompiler.jl`; `call_fn.jl` is unchanged, only the
  invocation gains `--sysimage=…`. Out of scope for feasibility.
- **Dragon stdout capture.** With default settings the Dragon backend does
  not populate `task.stdout` for executable tasks — the Julia process output
  appears on the `dragon` launcher's own stdout instead. The tasks
  themselves run correctly and exit cleanly; only the result-readback path
  differs. The real integration will pick a Dragon `process_template` /
  capture configuration that matches the customer's I/O conventions.
- **(b) Dragon.** `02_driver.jl` launches `julia`, which embeds Python via
  juliacall. The Dragon backend wants to be the process entry point
  (`dragon driver.py`), which does not fit a Julia-launched process cleanly.
  Sticking to `ConcurrentExecutionBackend` here keeps the demo honest.
  A real integration would either drive Rhapsody from a Python entry point
  that hosts Julia, or expose a Rhapsody-side service the Julia driver
  talks to over the network.
- **Arg marshalling in (c).** JSON via argv works for small scalar payloads;
  large/binary data should move through files (Rhapsody's `input_files` /
  `output_files`) instead.
- **PyCall.jl.** Skipped on purpose. `PythonCall.jl` / `juliacall` is the
  modern path and avoids the Julia↔Python version coupling that PyCall
  imposes. Easy to add a PyCall variant later if the customer needs it.
