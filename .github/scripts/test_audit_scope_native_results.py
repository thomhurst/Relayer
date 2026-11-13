import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from audit_scope_native_results import audit
from frozen_bdn_control import LAUNCHES
from frozen_scope_control import result


class ScopeNativeAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / 'results/native-scope'
        self.output.mkdir(parents=True)
        self.protocol = dict(case='scope-only', launches=list(LAUNCHES), sameImage=None,
                             instrumented=False, operationsPerInvoke=1, requestedRuntime='10.0.15',
                             parameter='instance.ScopedHandler = true;', commits=dict.fromkeys(('baseline', 'candidate', 'baseline-after'), 'commit'),
                             host='BenchmarkDotNet 0.15.8 DotNetCliExecutor: native anonymous pipes/Broker/process priority')
        self.protocol['benchmark'] = 'Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly'
        self.protocol['commands'] = [
            ['dotnet', '/host/FrozenNativeHost.dll', f'/host/inputs/performance-builds/{side}/image.dll', '4',
             '/host/native-dotnet.sh', f'/host/{i}-{side}-native.log', f'/host/{i}-{side}-native.json', 'scope-only']
            for i, side in enumerate(LAUNCHES, 1)]
        self.write('protocol.json', self.protocol)
        outcomes = []
        for index, side in enumerate(LAUNCHES, 1):
            name = f'{index}-{side}-native'
            lines = ['// Runtime=.NET 10.0.15 X64', '// GC: 1 0 0 1280 10',
                     f'WorkloadResult 1: 10 op, {index * 10}.0 ns, {index}.0 ns/op']
            (self.output / f'{name}.log').write_text('\n'.join(lines) + '\n// AfterAll\n')
            receipt = dict(FoundExecutable=True, IsSuccess=True, ExitCode=0, Errors=[], selectedCase='scope-only', operationsPerInvoke=1,
                           benchmarkArguments='--benchmarkName "Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly(ScopedHandler: True)" --job Default --benchmarkId 4',
                           parentAssembly='BenchmarkDotNet, Version=0.15.8.0, Culture=neutral, PublicKeyToken=aa0ca2f9092cefc4', Results=lines, PrefixedLines=[])
            self.write(f'{name}.json', receipt)
            outcomes.append(dict(launch=index, side=side, nativeReceipt=f'{name}.json', runtime='10.0.15', **result('\n'.join(lines) + '\n// AfterAll\n')))
        self.write('outcomes.json', outcomes)
        self.addCleanup(patch.stopall)
        patch('audit_scope_native_results.verify', return_value='commit').start()
        patch('audit_scope_native_results.executable', side_effect=lambda root, case: (root / 'image.dll', '4')).start()

    def write(self, name, value):
        (self.output / name).write_text(json.dumps(value))

    def test_all_outcomes_and_bracket_are_retained(self):
        actual = audit(self.root)
        self.assertEqual([x['meanNs'] for x in actual['launches']], [1, 2, 3, 4])
        self.assertEqual(actual['candidateTimeIncreasePercent'], [-19.999999999999996, 19.999999999999996])

    def test_instrumented_or_wrong_parameter_protocol_rejected(self):
        for key, value in [('instrumented', True), ('parameter', 'instance.ScopedHandler = false;'), ('commands', [])]:
            with self.subTest(key=key):
                self.write('protocol.json', dict(self.protocol, **{key: value}))
                with self.assertRaises(ValueError):
                    audit(self.root)

    def test_wrong_runner_or_failed_native_receipt_rejected(self):
        path = self.output / '2-candidate-native.json'
        original = json.loads(path.read_text())
        for key, value in [('ExitCode', 1), ('benchmarkArguments', 'wrong runner'), ('operationsPerInvoke', 8)]:
            with self.subTest(key=key):
                self.write(path.name, dict(original, **{key: value}))
                with self.assertRaises(ValueError):
                    audit(self.root)

    def test_missing_or_modified_outcome_and_receipt_result_rejected(self):
        outcomes = json.loads((self.output / 'outcomes.json').read_text())
        self.write('outcomes.json', outcomes[:3])
        with self.assertRaises(ValueError):
            audit(self.root)
        outcomes[0]['meanNs'] = 99
        self.write('outcomes.json', outcomes)
        with self.assertRaises(ValueError):
            audit(self.root)
        outcomes[0]['meanNs'] = 1
        self.write('outcomes.json', outcomes)
        receipt = json.loads((self.output / '1-baseline-native.json').read_text())
        receipt['Results'][-1] = 'WorkloadResult 1: 10 op, 90.0 ns, 9.0 ns/op'
        self.write('1-baseline-native.json', receipt)
        with self.assertRaises(ValueError):
            audit(self.root)
