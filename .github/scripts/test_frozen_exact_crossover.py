import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_exact_crossover import run
import test_frozen_core_crossover as fixtures


class ExactCrossoverTests(unittest.TestCase):
    def execute(self, root, failure=None):
        fixtures.CrossoverTests().fixture(root)
        calls = []
        def launch(command, **kwargs):
            calls.append((command, kwargs['env']))
            if failure == 'process' and len(calls) == 2:
                raise subprocess.CalledProcessError(1, command)
            runtime = '.NET 10.0.15' if failure == 'runtime' and len(calls) == 2 else '.NET 10.0.14'
            kwargs['stdout'].write(json.dumps({'runtime': runtime}) + '\n')
        with patch('frozen_core_crossover.verify', return_value='commit'), \
             patch('frozen_exact_crossover.verify', return_value='commit'), \
             patch('frozen_exact_crossover.read', side_effect=ValueError('bad coverage') if failure == 'report' else None) as reader, \
             patch('frozen_exact_crossover.subprocess.run', side_effect=launch):
            if failure:
                with self.assertRaisesRegex(SystemExit, 'every outcome retained'):
                    run(root, root/'probe.dll', root/'profiler.so', root/'out', '10.0.14')
            else:
                run(root, root/'probe.dll', root/'profiler.so', root/'out', '10.0.14')
        return calls, reader.call_count, json.loads((root/'out/outcomes.json').read_text())

    def test_original_waiter_reciprocal_brackets_and_unique_callback_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls, reads, outcomes = self.execute(root)
            self.assertEqual(reads, 8)
            self.assertEqual(len(outcomes), 8)
            self.assertEqual(calls[0][0], calls[3][0])
            self.assertEqual(calls[1][0], calls[2][0])
            self.assertEqual(calls[4][0], calls[7][0])
            self.assertEqual(calls[5][0], calls[6][0])
            self.assertTrue(all(command[-1] == '--bdn-await' for command, _ in calls))
            self.assertEqual(len({env['RELAYER_ALLOCATION_OUTPUT'] for _, env in calls}), 8)
            self.assertTrue(all(env['CORECLR_ENABLE_PROFILING'] == '1' for _, env in calls))
            self.assertTrue(all('measurement' in outcome for outcome in outcomes))

    def test_failures_preserved_and_remaining_launches_attempted(self):
        for failure in ('process', 'runtime', 'report'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                calls, _, outcomes = self.execute(Path(directory), failure)
                self.assertEqual(len(calls), 8)
                self.assertEqual(len(outcomes), 8)
                self.assertIn('error', outcomes[1])
                self.assertNotIn('measurement', outcomes[1])

    def test_bad_input_prevents_execution(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch('frozen_core_crossover.verify', side_effect=ValueError('bad inputs')), \
             patch('frozen_exact_crossover.subprocess.run') as execute:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, 'bad inputs'):
                run(root, root/'probe', root/'profiler', root/'output', '10.0.14')
            execute.assert_not_called()
