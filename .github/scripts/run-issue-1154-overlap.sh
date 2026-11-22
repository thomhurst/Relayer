#!/usr/bin/env bash
set -euo pipefail

workload=$1
mode=$2
results="$PWD/results"
core_filter='/*/*/(BatchPipelineTests|ConcurrentReceiveAllocationTests|InMemoryTransportTests|JsonSerializerTests|RabbitMqSendTests|SendRetentionTests)/*'
tracing_filter='/*/*/(BatchTracingTests|ConsumeTracingTests|SendTracingPoolTests|TracingTests)/*'
postgres_filter='/*/*/(PostgreSqlFixtureTests|PostgreSqlOutboxStoreTests|ChangeCaptureTests)/*'
profiler="$PWD/.artifacts/profiler/libFrozenAllocationProfiler.so"

run_project() {
  local project=$1 filter=$2 output=$3
  shift 3
  mkdir -p "$output"
  date -u --iso-8601=ns > "$output/started"
  local status=0
  timeout --kill-after=5s 75s dotnet test --project "tests/$project" --no-build -c Release -f net11.0 \
    --results-directory "$output" --treenode-filter "$filter" --output Detailed "$@" \
    > "$output/tests.log" 2>&1 || status=$?
  date -u --iso-8601=ns > "$output/finished"
  printf '%s\n' "$status" > "$output/exit-code"
  return "$status"
}

status=0
for attempt in 1 2 3; do
  sample="$results/sample-$attempt"
  profile_args=()
  if [[ "$mode" == profiled ]]; then
    profile_args=(
      --environment "CORECLR_ENABLE_PROFILING=1"
      --environment "CORECLR_PROFILER={32B7EF07-25E8-4AC7-B011-9A4321D702C8}"
      --environment "CORECLR_PROFILER_PATH=$profiler"
      --environment "RELAYER_ALLOCATION_PROFILER=$profiler"
      --environment "RELAYER_ALLOCATION_OUTPUT=$sample/core/objects.jsonl"
      --environment "RELAYER_ALLOCATION_STACKS=$sample/core/stacks.jsonl"
    )
  fi

  siblings=()
  if [[ "$workload" == overlap ]]; then
    run_project Relayer.Outbox.PostgreSql.Tests "$postgres_filter" "$sample/postgres" &
    siblings+=("$!")
  fi
  run_project Relayer.Tests "$core_filter" "$sample/core" "${profile_args[@]}" &
  core=$!
  if [[ "$workload" == overlap ]]; then
    run_project Relayer.Tracing.Tests "$tracing_filter" "$sample/tracing" &
    siblings+=("$!")
  fi

  for process in "$core" "${siblings[@]}"; do
    wait "$process" || status=$?
  done
  for module in "$sample"/*; do
    printf 'Sample %s %s\n' "$attempt" "${module##*/}"
    cat "$module/started" "$module/finished" "$module/exit-code"
    grep -E 'Running tests from|Warmed_batches_add_no_allocations|Batch [0-9]+: native DI' "$module/tests.log" || true
  done
done
exit "$status"
