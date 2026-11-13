"""Fixed-count Linux diagnostic for #571; never retries or replaces a CI gate."""
import json
import os
from pathlib import Path
import subprocess
import sys

from read_native_allocations import read

image, profiler, destination = map(lambda value: Path(value).resolve(), sys.argv[1:])
destination.mkdir(parents=True, exist_ok=True)
results = []
# Every outcome is retained; a passing trial never discards an earlier failure.
for trial in range(8):
    for instrumented in (False, True):
        name = f'{trial:02d}-' + ('callbacks' if instrumented else 'original')
        report = destination / f'{name}.jsonl'
        environment = dict(os.environ)
        for key in ("CORECLR_ENABLE_PROFILING", "CORECLR_PROFILER", "CORECLR_PROFILER_PATH",
                    "RELAYER_ALLOCATION_PROFILER", "RELAYER_ALLOCATION_OUTPUT"):
            environment.pop(key, None)
        if instrumented:
            environment.update(CORECLR_ENABLE_PROFILING='1',
                               CORECLR_PROFILER='{32B7EF07-25E8-4AC7-B011-9A4321D702C8}',
                               CORECLR_PROFILER_PATH=str(profiler),
                               RELAYER_ALLOCATION_PROFILER=str(profiler),
                               RELAYER_ALLOCATION_OUTPUT=str(report))
        outcome = subprocess.run(['dotnet', str(image), '--treenode-filter',
            '/*/*/BatchPipelineTests/Warmed_batches_add_no_allocations_beyond_identical_fresh_DI_scope_and_sender'],
            env=environment, capture_output=True, text=True, timeout=120)
        (destination / f'{name}.log').write_text(outcome.stdout + outcome.stderr)
        row = {'trial': trial, 'instrumented': instrumented, 'exit_code': outcome.returncode}
        if instrumented:
            row['allocation_report'] = read(report)
        results.append(row)
        (destination / 'outcomes.json').write_text(json.dumps(results, indent=2) + '\n')
        print(name, 'test exit', outcome.returncode, flush=True)
# Test failures are diagnostic observations, not suppressed CI failures. The original
# ready-PR CI check remains failed. Invalid/missing callback reports fail this job.
