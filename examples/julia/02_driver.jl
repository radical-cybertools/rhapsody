# Demo (b): a Julia program that drives Rhapsody.
#
# Rhapsody is a Python library, so the driver uses juliacall (PythonCall.jl)
# to import a thin sync wrapper (_rhapsody_helper.py) that hides Rhapsody's
# async API. Julia -> Python -> Rhapsody is enough for feasibility; a real
# integration would expose a richer, non-blocking surface.
#
# Run (with the rhapsody venv activated):
#   julia --project=examples/julia examples/julia/02_driver.jl

# Point PythonCall at the activated venv's interpreter (where rhapsody lives)
# instead of letting it bootstrap its own Conda env. Must be set BEFORE
# `using PythonCall`.
haskey(ENV, "VIRTUAL_ENV") || error(
    "VIRTUAL_ENV is not set -- activate the venv that has rhapsody installed " *
    "before running this demo."
)
ENV["JULIA_CONDAPKG_BACKEND"] = "Null"
ENV["JULIA_PYTHONCALL_EXE"]   = joinpath(ENV["VIRTUAL_ENV"], "bin", "python")

using PythonCall

# Make the helper next to this file importable.
pyimport("sys").path.insert(0, @__DIR__)

helper = pyimport("_rhapsody_helper")
result = pyconvert(String, helper.run_echo("hello from Julia driver"))

println("Rhapsody task stdout: ", result)
