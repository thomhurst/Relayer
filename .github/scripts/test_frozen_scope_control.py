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

    def test_resolution_selection_uses_its_original_runner_and_rejects_scope_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, image = original_tests.OriginalRunnerTests().fixture(root, '7')
            original = source.read_text().replace('EndToEndBenchmarks', 'ConsumePipelineBenchmarks').replace('ConcurrentRoundTrip()', 'ScopeAndHandlerResolution()').replace('OperationsPerInvoke = 8', 'OperationsPerInvoke = 1').replace('{ Operations', '{ instance.ScopedHandler = true; Operations')
            source.write_text(original)
            self.assertEqual(executable(root, True, 'ScopeAndHandlerResolution'), (image.resolve(), '7'))
            with self.assertRaises(ValueError):
                executable(root, True)
            with self.assertRaises(ValueError):
                executable(root, False, 'ScopeAndHandlerResolution')
            with self.assertRaises(ValueError):
                executable(root, True, 'Other')

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
                 patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method="ScopeOnly": (path / 'runner.dll', '4' if scoped else '0')), \
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

    def test_jit_capture_retains_code_and_excludes_timing_and_missing_code_fails(self):
        for emit_code in (True, False):
            with self.subTest(emit_code=emit_code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                environments = []

                def execute(command, **kwargs):
                    environment = kwargs['env']
                    environments.append(environment)
                    kwargs['stdout'].write(COMPLETE)
                    if emit_code:
                        Path(environment['DOTNET_JitStdOutFile']).write_text(
                            '; Assembly listing for method Example:ScopeOnly()\n')

                with patch('sys.argv', ['control', directory, str(root / 'output'), '--jit-disassembly', '--method', 'ScopeAndHandlerResolution']), \
                     patch('frozen_scope_control.verify', return_value='commit') as verify, \
                     patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method="ScopeOnly": (path / 'runner.dll', '4' if scoped else '0')), \
                     patch('frozen_scope_control.subprocess.run', side_effect=execute):
                    if emit_code:
                        main()
                    else:
                        with self.assertRaises(SystemExit):
                            main()
                self.assertEqual(len(environments), 8)
                protocol = json.loads((root / 'output/protocol.json').read_text())
                self.assertTrue(protocol['benchmark'].endswith('.ScopeAndHandlerResolution'))
                self.assertTrue(all(launch['command'][3] == protocol['benchmark'] for launch in protocol['launches']))
                self.assertEqual(verify.call_count, 6)
                self.assertEqual(len({env['DOTNET_JitStdOutFile'] for env in environments}), 8)
                outcomes = json.loads((root / 'output/outcomes.json').read_text())
                self.assertTrue(all('meanNs' not in item for item in outcomes))
                self.assertTrue(all('bytesPerOperation' not in item for item in outcomes))
                if emit_code:
                    self.assertTrue(all('disassembly' in item for item in outcomes))
                else:
                    self.assertTrue(all('error' in item for item in outcomes))

    def test_tiering_control_is_explicit_recorded_and_does_not_mutate_parent(self):
        for setting in ("default", "off"):
            with self.subTest(setting=setting), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                environments = []

                def execute(command, **kwargs):
                    environments.append(kwargs['env'])
                    kwargs['stdout'].write(COMPLETE)

                parent = {'DOTNET_TieredCompilation': '1', 'DOTNET_PRIVATE_FEED_TOKEN': 'secret'}
                with patch('sys.argv', ['control', directory, str(root / 'output'), '--tiered-compilation', setting]), \
                     patch('frozen_scope_control.os.environ', parent), \
                     patch('frozen_scope_control.verify', return_value='commit'), \
                     patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method="ScopeOnly": (path / 'runner.dll', '4')), \
                     patch('frozen_scope_control.subprocess.run', side_effect=execute):
                    main()
                expected = '0' if setting == 'off' else '1'
                self.assertEqual(len(environments), 8)
                self.assertTrue(all(env['DOTNET_TieredCompilation'] == expected for env in environments))
                self.assertEqual(parent['DOTNET_TieredCompilation'], '1')
                outcomes = json.loads((root / 'output/outcomes.json').read_text())
                self.assertTrue(all(item['runtimeEnvironment'] == {'DOTNET_TieredCompilation': expected} for item in outcomes))
                self.assertTrue(all(item['meanNs'] == 27 for item in outcomes))
                protocol = json.loads((root / 'output/protocol.json').read_text())
                self.assertEqual(protocol['tieredCompilation'], setting)

    def test_tiering_off_code_capture_keeps_both_controls_and_excludes_timings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environments = []

            def execute(command, **kwargs):
                environment = kwargs['env']
                environments.append(environment)
                kwargs['stdout'].write(COMPLETE)
                Path(environment['DOTNET_JitStdOutFile']).write_text(
                    '; Assembly listing for method Example:ScopeOnly()\n')

            parent = {'DOTNET_TieredCompilation': '1'}
            with patch('sys.argv', ['control', directory, str(root / 'output'),
                                    '--tiered-compilation', 'off', '--jit-disassembly']), \
                 patch('frozen_scope_control.os.environ', parent), \
                 patch('frozen_scope_control.verify', return_value='commit') as verify, \
                 patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method="ScopeOnly": (path / 'runner.dll', '4')), \
                 patch('frozen_scope_control.subprocess.run', side_effect=execute):
                main()
            self.assertEqual(verify.call_count, 6)
            self.assertEqual(len(environments), 8)
            self.assertTrue(all(env['DOTNET_TieredCompilation'] == '0' for env in environments))
            self.assertTrue(all(env['DOTNET_JitDisasm'] == '*' for env in environments))
            self.assertEqual(parent, {'DOTNET_TieredCompilation': '1'})
            outcomes = json.loads((root / 'output/outcomes.json').read_text())
            self.assertEqual([item['side'] for item in outcomes], ['baseline', 'candidate', 'candidate', 'baseline'] * 2)
            self.assertTrue(all(item['runtimeEnvironment'] == {'DOTNET_TieredCompilation': '0'} for item in outcomes))
            self.assertTrue(all('disassembly' in item and 'error' not in item for item in outcomes))
            self.assertTrue(all('meanNs' not in item and 'bytesPerOperation' not in item for item in outcomes))
            self.assertTrue(all('WorkloadResult' in path.read_text() for path in (root / 'output').glob('*.log')))
            protocol = json.loads((root / 'output/protocol.json').read_text())
            self.assertEqual(protocol['tieredCompilation'], 'off')
            self.assertTrue(protocol['jitDisassembly'])

    def test_method_filter_reaches_every_child_without_changing_parent_or_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environments = []
            listing = ('; Assembly listing for method Dispatcher1:GetService()\n'
                       '; Total bytes of code 7\n')

            def execute(command, **kwargs):
                environment = kwargs['env']
                environments.append(environment)
                kwargs['stdout'].write(COMPLETE)
                # Repeated compilations are raw evidence; never deduplicate them.
                Path(environment['DOTNET_JitStdOutFile']).write_text(listing * 2)

            parent = {'DOTNET_JitDisasm': '*'}
            with patch('sys.argv', ['control', directory, str(root / 'output'),
                                    '--jit-disassembly', '--jit-method', 'VisitScopeCache GetService']), \
                 patch('frozen_scope_control.os.environ', parent), \
                 patch('frozen_scope_control.verify', return_value='commit') as verify, \
                 patch('frozen_scope_control.executable', return_value=(root / 'runner.dll', '4')), \
                 patch('frozen_scope_control.subprocess.run', side_effect=execute):
                main()
            self.assertEqual(verify.call_count, 6)
            self.assertEqual(len(environments), 8)
            self.assertTrue(all(env['DOTNET_JitDisasm'] == 'VisitScopeCache GetService' for env in environments))
            self.assertEqual(parent, {'DOTNET_JitDisasm': '*'})
            protocol = json.loads((root / 'output/protocol.json').read_text())
            self.assertEqual(protocol['jitMethod'], 'VisitScopeCache GetService')
            outcomes = json.loads((root / 'output/outcomes.json').read_text())
            self.assertTrue(all(item['diagnosticEnvironment']['DOTNET_JitDisasm'] == 'VisitScopeCache GetService'
                                for item in outcomes))
            self.assertTrue(all('error' not in item and 'meanNs' not in item and
                                'bytesPerOperation' not in item for item in outcomes))
            self.assertEqual(len(list((root / 'output').glob('*.asm'))), 8)
            self.assertTrue(all(path.read_text() == listing * 2
                                for path in (root / 'output').glob('*.asm')))

    def test_invalid_filter_never_launches_or_verifies_inputs(self):
        for options in (['--jit-method', 'Resolve'],
                        ['--jit-disassembly', '--jit-method', '  ']):
            with self.subTest(options=options), \
                 patch('sys.argv', ['control', 'inputs', 'output'] + options), \
                 patch('frozen_scope_control.verify') as verify, \
                 patch('frozen_scope_control.subprocess.run') as execute:
                with self.assertRaises(SystemExit):
                    main()
                verify.assert_not_called()
                execute.assert_not_called()

    def test_readytorun_control_is_recorded_and_preserves_other_controls(self):
        for setting, inherited, expected in (("default", "1", "1"), ("on", "0", "1"),
                                             ("off", "1", "0"), ("default", None, None)):
            with self.subTest(setting=setting), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                environments = []

                def execute(command, **kwargs):
                    environments.append(kwargs['env'])
                    kwargs['stdout'].write(COMPLETE)

                parent = {'DOTNET_PRIVATE_FEED_TOKEN': 'secret'}
                if inherited is not None:
                    parent['DOTNET_ReadyToRun'] = inherited
                original_parent = parent.copy()
                with patch('sys.argv', ['control', directory, str(root / 'output'),
                                       '--ready-to-run', setting, '--tiered-compilation', 'off']), \
                     patch('frozen_scope_control.os.environ', parent), \
                     patch('frozen_scope_control.verify', return_value='commit') as verify, \
                     patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method: (path / 'runner.dll', '4')), \
                     patch('frozen_scope_control.subprocess.run', side_effect=execute):
                    main()
                self.assertEqual(len(environments), 8)
                self.assertEqual(verify.call_count, 6)
                self.assertTrue(all(env.get('DOTNET_ReadyToRun') == expected for env in environments))
                self.assertEqual(parent, original_parent)
                outcomes = json.loads((root / 'output/outcomes.json').read_text())
                runtime_environment = {'DOTNET_TieredCompilation': '0'}
                if expected is not None:
                    runtime_environment['DOTNET_ReadyToRun'] = expected
                self.assertTrue(all(item['runtimeEnvironment'] == runtime_environment for item in outcomes))
                self.assertTrue(all(item['meanNs'] == 27 for item in outcomes))
                self.assertEqual([item['side'] for item in outcomes], ['baseline', 'candidate', 'candidate', 'baseline'] * 2)
                protocol = json.loads((root / 'output/protocol.json').read_text())
                self.assertEqual(protocol['readyToRun'], setting)
                self.assertEqual(protocol['tieredCompilation'], 'off')

    def test_single_image_controls_preserve_slot_labels_and_actual_provenance(self):
        for selected in (None, "baseline", "candidate"):
            with self.subTest(selected=selected), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                commands = []

                def execute(command, **kwargs):
                    commands.append(command)
                    kwargs['stdout'].write(COMPLETE)

                options = ['--single-image', selected] if selected else []
                with patch('sys.argv', ['control', directory, str(root / 'output')] + options), \
                     patch('frozen_scope_control.verify', return_value='commit') as verify, \
                     patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method: (path / 'runner.dll', '4' if scoped else '0')), \
                     patch('frozen_scope_control.subprocess.run', side_effect=execute):
                    main()
                self.assertEqual(verify.call_count, 6)
                protocol = json.loads((root / 'output/protocol.json').read_text())
                outcomes = json.loads((root / 'output/outcomes.json').read_text())
                slots = ['baseline', 'candidate', 'candidate', 'baseline'] * 2
                actual = [selected or side for side in slots]
                self.assertEqual(protocol['singleImage'], selected)
                self.assertEqual([item['side'] for item in outcomes], slots)
                self.assertEqual([item['imageSide'] for item in outcomes], actual)
                self.assertEqual([item['imageSide'] for item in protocol['launches']], actual)
                self.assertEqual([Path(command[1]).parent.name for command in commands], actual)
                self.assertEqual([command[-1] for command in commands], ['4'] * 4 + ['0'] * 4)
                self.assertTrue(all(item['meanNs'] == 27 for item in outcomes))

    def test_counterbalanced_blocks_keep_every_process_and_failure_with_image_provenance(self):
        for selected, method in ((None, "ScopeOnly"), ("candidate", "ScopeOnly"),
                                 (None, "ScopeAndHandlerResolution")):
            with self.subTest(selected=selected, method=method), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    if len(calls) == 2:
                        raise subprocess.CalledProcessError(1, command)
                    # Keep the unfavorable observation; never stop or filter on timing.
                    kwargs['stdout'].write(COMPLETE.replace('27 ns/op', '40 ns/op')
                                           if len(calls) == 3 else COMPLETE)
                    self.assertEqual(kwargs['env'], {})

                options = ['--single-image', selected] if selected else []
                with patch('sys.argv', ['control', directory, str(root / 'output'), '--counterbalanced', '--method', method] + options), \
                     patch('frozen_scope_control.os.environ', {}), \
                     patch('frozen_scope_control.verify', return_value='commit') as verify, \
                     patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method: (path / 'runner.dll', '4' if scoped else '0')), \
                     patch('frozen_scope_control.subprocess.run', side_effect=execute):
                    with self.assertRaises(SystemExit):
                        main()
                self.assertEqual(verify.call_count, 6)
                protocol = json.loads((root / 'output/protocol.json').read_text())
                outcomes = json.loads((root / 'output/outcomes.json').read_text())
                abba = ['baseline', 'candidate', 'candidate', 'baseline']
                baab = ['candidate', 'baseline', 'baseline', 'candidate']
                expected = abba + baab + baab + abba
                self.assertEqual([item['side'] for item in outcomes], expected)
                self.assertEqual([item['block'] for item in outcomes], [1]*4 + [2]*4 + [3]*4 + [4]*4)
                self.assertEqual([item['position'] for item in outcomes], [1, 2, 3, 4]*4)
                self.assertEqual([item['imageSide'] for item in outcomes], [selected or side for side in expected])
                self.assertEqual([Path(command[1]).parent.name for command in calls], [selected or side for side in expected])
                self.assertEqual([command[-1] for command in calls], ['4']*16)
                self.assertTrue(all(command[3].endswith('.' + method) for command in calls))
                self.assertTrue(protocol['benchmark'].endswith('.' + method))
                self.assertTrue(all(item['scoped'] for item in outcomes))
                self.assertIn('error', outcomes[1])
                self.assertEqual(outcomes[2]['meanNs'], 40)
                self.assertEqual(outcomes[-1]['meanNs'], 27)
                self.assertEqual(len(list((root / 'output').glob('*.log'))), 16)
                self.assertEqual([item['block'] for item in protocol['launches']], [item['block'] for item in outcomes])
                self.assertIn('no adaptive stopping', protocol['plan'])

    def test_affinity_control_balances_images_and_retains_failed_pinned_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = []

            def execute(command, **kwargs):
                calls.append(command)
                self.assertEqual(kwargs['cwd'], Path(command[4] if command[0] == '/usr/bin/taskset' else command[1]).parent)
                self.assertEqual(kwargs['env'], {})
                if len(calls) == 5:
                    raise subprocess.CalledProcessError(1, command)
                kwargs['stdout'].write(COMPLETE.replace('27 ns/op', '40 ns/op')
                                       if len(calls) == 6 else COMPLETE)

            with patch('sys.argv', ['control', directory, str(root / 'output'), '--affinity-control']), \
                 patch('frozen_scope_control.os.environ', {}), \
                 patch('frozen_scope_control.os.sched_getaffinity', return_value={7, 3}, create=True), \
                 patch('frozen_scope_control.shutil.which', return_value='/usr/bin/taskset'), \
                 patch('frozen_scope_control.verify', return_value='commit') as verify, \
                 patch('frozen_scope_control.executable', side_effect=lambda path, scoped, method: (path / 'runner.dll', '4' if scoped else '0')), \
                 patch('frozen_scope_control.subprocess.run', side_effect=execute):
                with self.assertRaises(SystemExit):
                    main()
            self.assertEqual(verify.call_count, 6)
            self.assertEqual(len(calls), 16)
            protocol = json.loads((root / 'output/protocol.json').read_text())
            outcomes = json.loads((root / 'output/outcomes.json').read_text())
            self.assertEqual(protocol['allowedCpus'], [3, 7])
            self.assertEqual(protocol['pinnedCpu'], 3)
            self.assertEqual([item['affinity'] for item in outcomes], ['default']*4 + ['pinned']*8 + ['default']*4)
            abba = ['baseline', 'candidate', 'candidate', 'baseline']
            baab = ['candidate', 'baseline', 'baseline', 'candidate']
            expected = abba + baab + abba + baab
            self.assertEqual([item['imageSide'] for item in outcomes], expected)
            self.assertTrue(all(item['scoped'] for item in outcomes))
            for index, command in enumerate(calls):
                if 4 <= index < 12:
                    self.assertEqual(command[:4], ['/usr/bin/taskset', '--cpu-list', '3', 'dotnet'])
                else:
                    self.assertEqual(command[0], 'dotnet')
                self.assertEqual(command[-1], '4')
            self.assertIn('error', outcomes[4])
            self.assertEqual(outcomes[5]['meanNs'], 40)
            self.assertEqual(outcomes[-1]['meanNs'], 27)
            self.assertEqual(len(list((root / 'output').glob('*.log'))), 16)

    def test_invalid_affinity_control_never_verifies_or_launches(self):
        for cpus, taskset, options in (({3}, '/usr/bin/taskset', []),
                                      ({3, 7}, None, []),
                                      ({3, 7}, '/usr/bin/taskset', ['--counterbalanced']),
                                      ({3, 7}, '/usr/bin/taskset', ['--jit-disassembly']),
                                      ({3, 7}, '/usr/bin/taskset', ['--tiered-compilation', 'off']),
                                      ({3, 7}, '/usr/bin/taskset', ['--ready-to-run', 'off'])):
            with self.subTest(cpus=cpus, taskset=taskset, options=options), \
                 patch('sys.argv', ['control', 'inputs', 'output', '--affinity-control'] + options), \
                 patch('frozen_scope_control.os.sched_getaffinity', return_value=cpus, create=True), \
                 patch('frozen_scope_control.shutil.which', return_value=taskset), \
                 patch('frozen_scope_control.verify') as verify, \
                 patch('frozen_scope_control.subprocess.run') as execute:
                with self.assertRaises(SystemExit):
                    main()
                verify.assert_not_called()
                execute.assert_not_called()

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
