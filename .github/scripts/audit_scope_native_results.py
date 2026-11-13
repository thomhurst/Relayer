#!/usr/bin/env python3
"""Audit completed original-image scoped native-parent ABBA without executing it."""
import argparse
import json
from pathlib import Path
import re

from frozen_bdn_control import LAUNCHES, executable
from frozen_concurrent_control import verify
from frozen_scope_control import result


def audit(root):
    root = Path(root).resolve()
    output = root / 'results/native-scope'
    protocol = json.loads((output / 'protocol.json').read_text())
    if (protocol['case'] != 'scope-only' or protocol['launches'] != list(LAUNCHES)
            or len(protocol['commands']) != 4
            or protocol['benchmark'] != 'Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly'
            or protocol['sameImage'] is not None or protocol['instrumented']
            or protocol['operationsPerInvoke'] != 1 or protocol['requestedRuntime'] != '10.0.15'
            or protocol['parameter'] != 'instance.ScopedHandler = true;'
            or protocol['host'] != 'BenchmarkDotNet 0.15.8 DotNetCliExecutor: native anonymous pipes/Broker/process priority'):
        raise ValueError('Unexpected scoped native-parent protocol')
    builds = root / 'inputs/performance-builds'
    for side in ('baseline', 'candidate', 'baseline-after'):
        if verify(builds / side) != protocol['commits'][side]:
            raise ValueError('Input commit differs from protocol')
    recorded = json.loads((output / 'outcomes.json').read_text())
    if len(recorded) != 4:
        raise ValueError('Expected every ABBA outcome')
    launches = []
    for index, side in enumerate(LAUNCHES, 1):
        image, runner = executable(builds / side, 'scope-only')
        name = f'{index}-{side}-native'
        command = protocol['commands'][index - 1]
        suffix = (Path('inputs/performance-builds') / side / image.relative_to(builds / side)).as_posix()
        if (len(command) != 8 or command[0] != 'dotnet'
                or not command[1].endswith('/FrozenNativeHost.dll')
                or not command[2].endswith('/' + suffix) or command[3] != runner
                or not command[4].endswith('/native-dotnet.sh')
                or not command[5].endswith('/' + name + '.log')
                or not command[6].endswith('/' + name + '.json') or command[7] != 'scope-only'):
            raise ValueError('Native command does not bind selected image/runner/receipt')
        receipt = json.loads((output / f'{name}.json').read_text())
        expected = ('--benchmarkName "Relayer.Benchmarks.ConsumePipelineBenchmarks.'
                    f'ScopeOnly(ScopedHandler: True)" --job Default --benchmarkId {runner}')
        if (receipt['FoundExecutable'] is not True or receipt['IsSuccess'] is not True
                or receipt['ExitCode'] != 0 or receipt['Errors']
                or receipt['selectedCase'] != 'scope-only' or receipt['operationsPerInvoke'] != 1
                or receipt['benchmarkArguments'] != expected
                or receipt['parentAssembly'] != 'BenchmarkDotNet, Version=0.15.8.0, Culture=neutral, PublicKeyToken=aa0ca2f9092cefc4'):
            raise ValueError('Native receipt identity/completion mismatch')
        text = (output / f'{name}.log').read_text()
        if re.findall(r'^// Runtime=\.NET ([0-9.]+) ', text, re.M) != ['10.0.15']:
            raise ValueError('Loaded runtime mismatch')
        parsed = result(text)
        original = recorded[index - 1]
        expected_outcome = dict(launch=index, side=side, nativeReceipt=f'{name}.json', runtime='10.0.15', **parsed)
        if original != expected_outcome:
            raise ValueError('Recorded outcome differs from native log')
        # Native receipt and broker log independently retain the result stream.
        if result('\n'.join(receipt['Results'] + receipt['PrefixedLines']) + '\n// AfterAll\n') != parsed:
            raise ValueError('Receipt results differ from broker log')
        launches.append(expected_outcome)
    bracket = (launches[0]['meanNs'] + launches[3]['meanNs']) / 2
    return {'launches': launches, 'baselineBracketMeanNs': bracket,
            'candidateTimeIncreasePercent': [100 * (item['meanNs'] / bracket - 1) for item in launches[1:3]],
            'limits': 'Descriptive process means, not independent iteration replicates, a gate verdict, throughput equivalence, isolated pipe cost or historical causal attribution. All original unfavorable signals remain unresolved.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact', type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.artifact), indent=2))
