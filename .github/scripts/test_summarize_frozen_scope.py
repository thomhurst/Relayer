import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_frozen_scope import listings, summarize


def listing(name='Example:Scope()', operand='0x1234'):
    return f'; Assembly listing for method {name}\n       mov rax, {operand}\n; Total bytes of code 10\n\n'.encode()


class ScopeListingTests(unittest.TestCase):
    def fixture(self, directory):
        launches = [{'side': side, 'scoped': scoped} for scoped in (True, False)
                    for side in ('baseline', 'candidate', 'candidate', 'baseline')]
        protocol = {'benchmark': 'Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly',
                    'jitDisassembly': True, 'tieredCompilation': 'off', 'launches': launches}
        outcomes = []
        for index, launch in enumerate(launches, 1):
            name = f"{index}-{launch['side']}-{launch['scoped']}.asm"
            outcomes.append(dict(launch, launch=index, disassembly=name,
                                 runtimeEnvironment={'DOTNET_TieredCompilation': '0'},
                                 diagnosticEnvironment={'DOTNET_JitDisasm': '*', 'DOTNET_JitDisasmDiffable': '1'}))
            (directory / name).write_bytes(listing() + listing('Repeated()', '1') + listing('Repeated()', '2'))
        (directory / 'protocol.json').write_text(json.dumps(protocol))
        (directory / 'outcomes.json').write_text(json.dumps(outcomes))
        return protocol, outcomes

    def test_exact_comparison_retains_immediates_recompilations_and_missing_methods(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            self.fixture(directory)
            original = summarize(directory)
            self.assertEqual([group['identicalAcrossAllFour'] for group in original['groups']], [2, 2])
            path = directory / '2-candidate-True.asm'
            path.write_bytes(listing(operand='0x5678') + listing('Repeated()', '2') + listing('Repeated()', '1') + listing('OnlyCandidate()'))
            actual = summarize(directory)
            differences = actual['groups'][0]['differences']
            self.assertEqual(len(differences), 3)
            self.assertEqual(actual['groups'][1]['identicalAcrossAllFour'], 2)
            missing = next(item for item in differences if item['method'] == 'OnlyCandidate()')
            self.assertIsNone(missing['listingSha256ByLaunch'][0])
            self.assertEqual(len(actual['captures'][1]['methods']['Repeated()']), 2)
            self.assertNotEqual(original['captures'][1]['sha256'], actual['captures'][1]['sha256'])

    def test_resolution_capture_keeps_benchmark_identity_and_rejects_other_methods(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            protocol, _ = self.fixture(directory)
            for method in ('ScopeOnly', 'ScopeAndHandlerResolution'):
                protocol['benchmark'] = 'Relayer.Benchmarks.ConsumePipelineBenchmarks.' + method
                (directory / 'protocol.json').write_text(json.dumps(protocol))
                actual = summarize(directory)
                self.assertEqual(actual['benchmark'], protocol['benchmark'])
                self.assertEqual([group['identicalAcrossAllFour'] for group in actual['groups']], [2, 2])
            protocol['benchmark'] = 'Relayer.Benchmarks.ConsumePipelineBenchmarks.DirectDispatch'
            (directory / 'protocol.json').write_text(json.dumps(protocol))
            with self.assertRaises(ValueError):
                summarize(directory)

    def test_audit_reports_invalid_capture_without_comparing_partial_group(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            self.fixture(directory)
            path = directory / '2-candidate-True.asm'
            path.write_bytes(path.read_bytes() + b'; Assembly listing for method Partial()\n')
            with self.assertRaises(ValueError):
                summarize(directory)
            actual = summarize(directory, audit=True)
            self.assertIn('listingError', actual['captures'][1])
            self.assertNotIn('methods', actual['captures'][1])
            self.assertFalse(actual['groups'][0]['comparable'])
            self.assertNotIn('differences', actual['groups'][0])
            self.assertEqual(actual['groups'][1]['identicalAcrossAllFour'], 2)
            # Audit never relaxes launch/runtime validation.
            outcomes = json.loads((directory / 'outcomes.json').read_text())
            outcomes[0]['error'] = 'failed launch'
            (directory / 'outcomes.json').write_text(json.dumps(outcomes))
            with self.assertRaises(ValueError):
                summarize(directory, audit=True)

    def test_rejects_partial_or_unexpected_listings(self):
        for invalid in (b'', b'warning\n' + listing(), listing().replace(b'; Total bytes of code 10', b''),
                        listing() + b'; Assembly listing for method Partial()\n'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                listings(invalid)

    def test_rejects_failed_missing_reordered_or_wrong_runtime_launches(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            protocol, outcomes = self.fixture(directory)
            variants = []
            failed = copy.deepcopy(outcomes)
            failed[1]['error'] = 'launch failed'
            variants.append(failed)
            variants.extend((outcomes[:-1], list(reversed(outcomes))))
            wrong = copy.deepcopy(outcomes)
            wrong[0]['runtimeEnvironment']['DOTNET_TieredCompilation'] = '1'
            variants.append(wrong)
            for invalid in variants:
                (directory / 'outcomes.json').write_text(json.dumps(invalid))
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    summarize(directory)
            (directory / 'outcomes.json').write_text(json.dumps(outcomes))
            protocol['jitDisassembly'] = False
            (directory / 'protocol.json').write_text(json.dumps(protocol))
            with self.assertRaises(ValueError):
                summarize(directory)


if __name__ == '__main__':
    unittest.main()
