import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from read_native_tail_allocations import read


def report():
    events = [(5, 8), (6, 8), (1, 0), (15, 8), (16, 8), (15, 16), (16, 16), (2, 0)]
    return ([{'kind': 'engine-event', 'id': identity, 'thread': 42, 'operations': operations}
             for identity, operations in events]
            + [{'kind': 'native-engine-tail', 'nativeOperations': 16, 'iterations': 2, 'stopped': True},
               {'kind': 'type', 'classId': 1, 'type': 'Message', 'objects': 3, 'bytes': 72},
               {'kind': 'coverage', 'objects': 3, 'bytes': 72, 'failures': 0}])


class NativeTailReportTests(unittest.TestCase):
    def check(self, rows, operations=16):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'objects.jsonl'
            path.write_text('\n'.join(map(json.dumps, rows)))
            return read(path, operations)

    def test_preserves_every_event_and_uses_final_iteration_denominator(self):
        actual = self.check(report())
        self.assertEqual(len(actual['events']), 8)
        self.assertEqual(actual['tail']['nativeOperations'], 16)
        self.assertEqual(actual['coverage']['objects'], 3)

    def test_rejects_missing_ambiguous_and_unmatched_boundaries_or_counts(self):
        mutations = [lambda r: r.pop(7), lambda r: r.pop(2), lambda r: r.pop(5),
                     lambda r: r[6].update(operations=8), lambda r: r[6].update(thread=99),
                     lambda r: r.insert(8, copy.copy(r[3])), lambda r: r.insert(8, copy.copy(r[8])),
                     lambda r: r[8].update(stopped=False), lambda r: r[8].update(nativeOperations=8),
                     lambda r: r[8].update(iterations=3), lambda r: r[-1].update(failures=1),
                     lambda r: r[-1].update(bytes=73), lambda r: r[4].update(operations=-8),
                     lambda r: r[3].update(id=17), lambda r: r.reverse()]
        for mutate in mutations:
            rows = report()
            mutate(rows)
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check(rows)
        with self.assertRaisesRegex(ValueError, 'denominator'):
            self.check(report(), 24)


@unittest.skipUnless(os.environ.get('TEST_NATIVE_PROFILER'), 'Native profiler fixture paths not supplied')
class NativeTailFixtureTests(unittest.TestCase):
    def execute(self, count, mode):
        evidence = Path(os.environ['TEST_NATIVE_OUTPUT'])
        evidence.mkdir(parents=True, exist_ok=True)
        path = (evidence / f'tail-{mode}-{count}.jsonl').resolve()
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING='1',
                           CORECLR_PROFILER='{32B7EF07-25E8-4AC7-B011-9A4321D702C8}',
                           CORECLR_PROFILER_PATH=str(Path(os.environ['TEST_NATIVE_PROFILER']).resolve()),
                           RELAYER_ALLOCATION_BDN_TAIL='1', RELAYER_ALLOCATION_OUTPUT=str(path))
        execution = subprocess.run(['dotnet', '--roll-forward', 'Major', os.environ['TEST_NATIVE_FIXTURE'], str(count), mode],
                                   capture_output=True, text=True, env=environment, timeout=60)
        (evidence / f'tail-{mode}-{count}.log').write_text(execution.stdout + execution.stderr)
        self.assertEqual(execution.returncode, 0, execution.stderr)
        return path

    def test_exact_final_tail_counts_exclude_timed_work_earlier_tails_and_post_stop(self):
        for count in (1, 1024, 16384):
            actual = read(self.execute(count, 'tail'), 8)
            self.assertEqual(actual['tail']['iterations'], 2)
            for name in ('FixtureMessage', 'FixtureTypes+Box`1[FixtureMessage]'):
                rows = [row for row in actual['types'] if row['type'] == name]
                self.assertEqual(len(rows), 1, actual['types'])
                self.assertEqual(rows[0]['objects'], count)
                self.assertEqual(rows[0]['bytes'], count * 24)

    def test_missing_boundaries_and_events_after_stop_fail_closed(self):
        for mode in ('missing-stop', 'missing-benchmark', 'extra-event'):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                read(self.execute(1024, mode), 8)


if __name__ == '__main__':
    unittest.main()
