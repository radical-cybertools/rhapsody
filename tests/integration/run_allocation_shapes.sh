#!/usr/bin/env bash
# Run test_allocation_shapes.py on all, half and one of the CPUs this process may use. Each shape
# gets its own Dragon runtime, and CPU ids come from the process's affinity, not 0..nproc-1, so
# this also works inside a Slurm or container cpuset such as 8-15.
#
# Environment:
#   PYTHON                   Python to run pytest with (default python3).
#   RHAPSODY_SHAPES_TIMEOUT  seconds each task, cancel or close may take (default 60 here).
#   SHAPES_TIME_LIMIT        seconds for one shape. The default covers the worst case, so a hang
#                            can't kill pytest before its report: seven tests each time out on
#                            a task and on its cancel, and two sessions time out closing.

PYTHON=${PYTHON:-python3}
export RHAPSODY_ALLOCATION_SHAPES=1
export RHAPSODY_SHAPES_TIMEOUT=${RHAPSODY_SHAPES_TIMEOUT:-60}
LIMIT=${SHAPES_TIME_LIMIT:-$((16 * RHAPSODY_SHAPES_TIMEOUT + 60))}

if ! command -v dragon >/dev/null 2>&1; then
    echo "Dragon launcher not available, skipping the allocation-shape tests."
    exit 0
fi

read -r -a cpus <<<"$("$PYTHON" -c 'import os; print(*sorted(os.sched_getaffinity(0)))')"
n=${#cpus[@]}
join() { local IFS=,; echo "$*"; }

shapes=("$(join "${cpus[@]}")")
[ "$n" -ge 4 ] && shapes+=("$(join "${cpus[@]:0:n/2}")")
[ "$n" -ge 2 ] && shapes+=("${cpus[0]}")

rc=0
for shape in "${shapes[@]}"; do
    echo "== allocation shape: CPUs $shape of $n"
    timeout --signal=INT --kill-after=30 "$LIMIT" taskset -c "$shape" \
        dragon -s "$PYTHON" -m pytest tests/integration/test_allocation_shapes.py -v -s || rc=1
done
exit $rc
