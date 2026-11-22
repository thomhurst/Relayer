#!/usr/bin/env bash
set -euo pipefail

results="$PWD/results"
profile_args=()
if [[ "$CAPTURE_MODE" == profiled ]]; then
  profiler="$PWD/.artifacts/profiler/libFrozenAllocationProfiler.so"
  profile_args=(
    --environment "CORECLR_ENABLE_PROFILING=1"
    --environment "CORECLR_PROFILER={32B7EF07-25E8-4AC7-B011-9A4321D702C8}"
    --environment "CORECLR_PROFILER_PATH=$profiler"
    --environment "RELAYER_ALLOCATION_TARGET=Relayer.Tests"
    --environment "RELAYER_ALLOCATION_PROFILER=$profiler"
    --environment "RELAYER_ALLOCATION_OUTPUT=$results/objects.jsonl"
    --environment "RELAYER_ALLOCATION_STACKS=$results/stacks.jsonl"
  )
fi

cd .artifacts/ci-source
status=0
timeout --kill-after=10s 8m dotnet test --no-build -c Release -f "$FRAMEWORK" "${profile_args[@]}" \
  > "$results/tests.log" 2>&1 || status=$?
printf '%s\n' "$status" > "$results/exit-code"
grep -aE 'Warmed_batches_add_no_allocations|Batch [0-9]+: native DI|Test run summary|Test run failed' \
  "$results/tests.log" || true
exit "$status"
