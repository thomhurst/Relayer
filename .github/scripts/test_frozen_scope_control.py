import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_scope_control import executable, main, result
import test_frozen_bdn_control as original_tests

COMPLETE = 'WorkloadResult   1: 16 op, 432 ns, 27 ns/op\n// GC:  0 0 0 2048 16\n// AfterAll\n'


class ScopeControlTests(unittest.TestCase):
    def test_native_results_keep_timing_and_single_operation_allocations(self):
        actual = result(COMPLETE)
        self.assertEqual(actual['meanNs'], 27)
        self.assertEqual(actual['bytesPerOperation'], 128)
        for invalid in ('', COMPLETE * 2, COMPLETE.replace('// AfterAll', ''),
                        COMPLETE.replace('2048 16', '2048 0')):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                result(invalid)

    def test_selects_parameter_and_rejects_wrong_job_and_missing_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, image = original_tests.OriginalRunnerTests().fixture(root, '4')
            original = source.read_text().replace('EndToEndBenchmarks', 'ConsumePipelineBenchmarks').replace('ConcurrentRoundTrip()', 'ScopeOnly()').replace('OperationsPerInvoke = 8', 'OperationsPerInvoke = 1').replace('{ Operations', '{ instance.ScopedHandler = true; Operations')
            source.write_text(original)
            self.assertEqual(executable(root, True), (image.resolve(), '4'))
            with self.assertRaises(ValueError):
                executable(root, False)
            source.write_text(original.replace('OperationsPerInvoke = 1', 'OperationsPerInvoke = 8'))
            with self.assertRaises(ValueError):
                executable(root, True)
            source.write_text(original)
            (root / 'inventory-sha256.json').write_text('{"files": []}')
            with self.assertRaises(ValueError):
                executable(root, True)

    def test_all_launches_attempted_and_unfavorable_results_retained_after_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = []

            def execute(command, **kwargs):
                calls.append(command)
                if len(calls) == 2:
                    raise subprocess.CalledProcessError(1, command)
                kwargs['stdout'].write(COMPLETE)

            with patch('sys.argv', ['control', directory, str(root / 'output')]), \
                 patch('frozen_scope_control.verify', return_value='commit') as verify, \
                 patch('frozen_scope_control.executable', side_effect=lambda path, scoped: (path / 'runner.dll', '4' if scoped else '0')), \
                 patch('frozen_scope_control.subprocess.run', side_effect=execute):
                with self.assertRaises(SystemExit):
                    main()
            self.assertEqual(len(calls), 8)
            self.assertEqual(calls[0], calls[3])
            self.assertEqual(calls[1], calls[2])
            self.assertEqual(verify.call_count, 6)
            outcomes = json.loads((root / 'output/outcomes.json').read_text())
            self.assertIn('error', outcomes[1])
            self.assertNotIn('meanNs', outcomes[1])
            self.assertEqual(outcomes[-1]['meanNs'], 27)
            self.assertEqual([item['scoped'] for item in outcomes], [True]*4 + [False]*4)

    def test_invalid_archive_never_launches(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch('sys.argv', ['control', directory, str(Path(directory) / 'output')]), \
             patch('frozen_scope_control.verify', side_effect=ValueError('changed')), \
             patch('frozen_scope_control.subprocess.run') as execute:
            with self.assertRaises(ValueError):
                main()
            execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
