#!/usr/bin/env python3
"""Audit retained native contention exports without inferring historical causality."""
import argparse
import json
from pathlib import Path

BENCHMARK = 'Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly'
PROVIDER = 'BenchmarkDotNet.EngineEventSource'


def audit(events):
    coverage = [e for e in events if e.get('kind') == 'coverage']
    if len(coverage) != 1 or coverage[0].get('eventsLost') != 0 or coverage[0].get('samples') != 0 or coverage[0].get('contentionOnly') is not True:
        raise ValueError('Expected one loss-free contention-only coverage record')
    markers = [e for e in events if e.get('ProviderName') == PROVIDER and e['id'] in (1, 2, 15, 16)]
    bounds = [e for e in markers if e['id'] in (1, 2)]
    if len(bounds) != 2 or [e['id'] for e in bounds] != [1, 2] or any(e['payload'].get('benchmarkName') != BENCHMARK for e in bounds):
        raise ValueError('Missing or ambiguous benchmark identity/bounds')
    identity = (bounds[0]['processId'], bounds[0]['threadId'])
    if (bounds[1]['processId'], bounds[1]['threadId']) != identity:
        raise ValueError('Benchmark process/thread changed')
    intervals, pending = [], None
    previous = bounds[0]['timeMilliseconds']
    for event in (e for e in markers if e['id'] in (15, 16)):
        time = event['timeMilliseconds']
        if (event['processId'], event['threadId']) != identity or not previous <= time <= bounds[1]['timeMilliseconds']:
            raise ValueError('Invalid workload identity/order/bounds')
        previous = time
        if event['id'] == 15:
            if pending is not None:
                raise ValueError('Nested workload start')
            pending = event
        else:
            if pending is None or pending['payload'] != event['payload']:
                raise ValueError('Unmatched workload stop or operation count')
            intervals.append((pending['timeMilliseconds'], time))
            pending = None
    if pending is not None or not intervals:
        raise ValueError('Incomplete workload coverage')
    if coverage[0].get('workloadStarts') != len(intervals) or coverage[0].get('workloadStops') != len(intervals):
        raise ValueError('Coverage disagrees with workload records')
    contention = [e for e in events if e.get('ProviderName') == 'Microsoft-Windows-DotNETRuntime' and e.get('EventName', '').startswith('Contention/')]
    if coverage[0].get('contentionRecords') != len(contention):
        raise ValueError('Coverage disagrees with contention records')
    # Preserve every event, including lock-created records, other threads, boundary
    # crossings and unmatched pairs. No pairing inference is needed for zero events.
    return {'processId': identity[0], 'threadId': identity[1], 'intervals': intervals,
            'contentionRecords': len(contention), 'events': contention,
            'interpretation': 'No exported contention events in any phase' if not contention else 'Contention present: Start/Stop pairing and boundary analysis remain unresolved',
            'limits': 'No events does not prove no contention or measure uncontended monitor cost. Console replay differs from the historical native parent. No timing, equivalence, historical attribution or gate claim.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('exports', nargs='+', type=Path)
    args = parser.parse_args()
    print(json.dumps({str(path): audit([json.loads(line) for line in path.open()]) for path in args.exports}, indent=2))
