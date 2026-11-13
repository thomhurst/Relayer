#!/usr/bin/env python3
"""Exact heap owners under reciprocal retained core-DLL substitution; never a gate."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from frozen_core_crossover import inventory, prepare
from frozen_concurrent_control import verify
from read_native_allocations import read


def run(artifact, probe, profiler, output, runtime):
    output.mkdir(parents=True, exist_ok=False)
    commits, images = prepare(artifact / 'performance-builds', output / 'images')
    launches = []
    for engine in ('baseline', 'candidate'):
        other = 'candidate' if engine == 'baseline' else 'baseline'
        launches.extend(f'{engine}-engine-{core}-core' for core in (engine, other, other, engine))
    protocol = {'commits': commits, 'launches': launches, 'runtime': runtime,
                'images': {name: image['inventory'] for name, image in images.items()},
                'limits': 'Separate exact-callback execution with original BDN waiter. Only Relayer.dll changes within each fixed engine bracket. '
                          'Callbacks alter CLR fast paths and scheduling; object sizes exclude some allocation-context overhead. '
                          'No stacks, historical counts, identity-only intervention, loaded-byte proof, throughput evidence or cross-control subtraction.'}
    (output / 'protocol.json').write_text(json.dumps(protocol, indent=2) + '\n')
    outcomes = []
    for index, name in enumerate(launches, 1):
        image = images[name]
        if inventory(image['directory']) != image['inventory']:
            raise ValueError('Prepared image changed before execution')
        report = (output / f'{index}-{name}-objects.jsonl').resolve()
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING='1',
                           CORECLR_PROFILER='{32B7EF07-25E8-4AC7-B011-9A4321D702C8}',
                           CORECLR_PROFILER_PATH=str(profiler.resolve()),
                           RELAYER_ALLOCATION_PROFILER=str(profiler.resolve()),
                           RELAYER_ALLOCATION_OUTPUT=str(report))
        outcome = {'launch': index, 'image': name}
        log_path = output / f'{index}-{name}.log'
        with log_path.open('w') as log:
            try:
                subprocess.run(['dotnet', str(probe.resolve()), str(image['directory'].resolve()), '--bdn-await'],
                               stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600, env=environment)
                read(report)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, ValueError) as error:
                outcome['error'] = str(error)
                log.write(f'\nLaunch failed: {error}\n')
        if 'error' not in outcome:
            try:
                records = [json.loads(line) for line in log_path.read_text().splitlines() if line.startswith('{')]
                if len(records) != 1 or records[0]['runtime'] != '.NET ' + runtime:
                    raise ValueError('Expected one outcome on requested runtime')
                outcome['measurement'] = records[0]
            except (ValueError, KeyError) as error:
                outcome['error'] = str(error)
        if inventory(image['directory']) != image['inventory']:
            outcome['error'] = 'Prepared image changed during execution'
        outcomes.append(outcome)
        (output / 'outcomes.json').write_text(json.dumps(outcomes, indent=2) + '\n')
    for side in ('baseline', 'candidate', 'baseline-after'):
        verify(artifact / 'performance-builds' / side)
    if any('error' in outcome for outcome in outcomes):
        raise SystemExit('Exact crossover failed; every outcome retained')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('artifact', 'probe', 'profiler', 'output'):
        parser.add_argument(name, type=Path)
    parser.add_argument('--runtime', choices=('10.0.14', '10.0.15'), default='10.0.14')
    args = parser.parse_args()
    run(args.artifact, args.probe, args.profiler, args.output, args.runtime)
