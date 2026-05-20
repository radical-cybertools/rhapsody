# Dispatch wrapper for demo (c).
#
# Reads a single JSON argument {"fn": "...", "args": [...]}, looks up the
# named function in MyMod, calls it, writes {"result": ...} to stdout.
#
# Rhapsody dispatches *processes*, not Julia functions. A real integration
# would replace this wrapper with a fast in-process dispatch (sysimage +
# per-node worker). One-shot julia per call is the simplest thing that
# proves the model works.

using JSON

include(joinpath(@__DIR__, "mymod.jl"))
using .MyMod

payload = JSON.parse(ARGS[1])
fn      = getfield(MyMod, Symbol(payload["fn"]))
result  = fn(payload["args"]...)

JSON.print(stdout, Dict("result" => result))
