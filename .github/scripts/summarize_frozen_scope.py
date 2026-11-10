#!/usr/bin/env python3
"""Compare retained tiering-off JIT listings exactly; never infer timing or causality."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

from frozen_scope_control import BENCHMARK, LAUNCHES

BENCHMARKS = (BENCHMARK, BENCHMARK.rsplit('.', 1)[0] + '.ScopeAndHandlerResolution')
HEADER = b'; Assembly listing for method '


def listings(raw):
    """Keep repeated compilations and every byte, including immediates and metadata."""
    blocks = re.split(rb'(?=^; Assembly listing for method )', raw, flags=re.M)
    if blocks[0].strip():
        raise ValueError('Unexpected text before first listing')
    methods = defaultdict(list)
    for block in blocks[1:]:
        identity = block.splitlines()[0][len(HEADER):].decode('utf-8')
        if not identity or len(re.findall(rb'^; Total bytes of code \d+\r?$', block, re.M)) != 1:
            raise ValueError('Incomplete or ambiguous method listing')
        methods[identity].append(hashlib.sha256(block).hexdigest())
    if not methods:
        raise ValueError('No complete method listings')
    return dict(methods)


def summarize(directory, audit=False):
    directory = Path(directory)
    protocol = json.loads((directory / 'protocol.json').read_text())
    outcomes = json.loads((directory / 'outcomes.json').read_text())
    expected = [(side, scoped) for scoped in (True, False) for side in LAUNCHES]
    if (protocol.get('benchmark') not in BENCHMARKS or protocol.get('jitDisassembly') is not True
            or protocol.get('tieredCompilation') != 'off'
            or len(protocol.get('launches', [])) != 8 or len(outcomes) != 8):
        raise ValueError('Expected complete tiering-off scope or handler-resolution code capture')
    # Older wildcard captures predate the explicit native filter field.
    jit_method = protocol.get('jitMethod', '*')
    if not isinstance(jit_method, str) or not jit_method.strip():
        raise ValueError('Expected a nonempty native JIT method filter')
    captures = []
    for index, ((side, scoped), launch, outcome) in enumerate(zip(expected, protocol['launches'], outcomes), 1):
        name = f'{index}-{side}-{scoped}.asm'
        diagnostic = outcome.get('diagnosticEnvironment', {})
        if (launch.get('side') != side or launch.get('scoped') is not scoped
                or outcome.get('launch') != index or outcome.get('side') != side
                or outcome.get('scoped') is not scoped or 'error' in outcome
                or outcome.get('disassembly') != name
                or outcome.get('runtimeEnvironment', {}).get('DOTNET_TieredCompilation') != '0'
                or diagnostic.get('DOTNET_JitDisasm') != jit_method
                or diagnostic.get('DOTNET_JitDisasmDiffable') != '1'):
            raise ValueError('Missing, failed or unexpected capture launch')
        raw = (directory / name).read_bytes()
        capture = {'launch': index, 'side': side, 'scoped': scoped, 'file': name,
                   'sha256': hashlib.sha256(raw).hexdigest()}
        try:
            capture['methods'] = listings(raw)
        except ValueError as error:
            if not audit:
                raise
            capture['listingError'] = str(error)
        captures.append(capture)
    groups = []
    for offset in (0, 4):
        group = captures[offset:offset + 4]
        if any('listingError' in capture for capture in group):
            groups.append({'scoped': offset == 0, 'comparable': False,
                           'reason': 'At least one capture has invalid listings; no method comparison performed.'})
            continue
        names = sorted(set().union(*(capture['methods'] for capture in group)))
        differences = []
        for name in names:
            hashes = [capture['methods'].get(name) for capture in group]
            if any(value != hashes[0] for value in hashes[1:]):
                differences.append({'method': name, 'listingSha256ByLaunch': hashes})
        groups.append({'scoped': offset == 0, 'comparable': True, 'methodIdentities': len(names),
                       'identicalAcrossAllFour': len(names) - len(differences),
                       'differences': differences})
    return {'benchmark': protocol['benchmark'], 'jitMethod': jit_method, 'captures': captures, 'groups': groups,
            'comparison': 'Exact bytes per method identity, preserving repeated-listing order; no normalization or filtering.',
            'limits': 'Diffable native JIT text omits addresses/relocations and does not cover all precompiled code, layout, host state or execution. Matching listings do not prove timing equivalence or historical causality. Different listings do not prove a production regression. Diagnostic timings are excluded; no gate pass or accepted trade-off.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--audit', action='store_true', help='Report invalid listing files and skip their entire group; never compare partial listings.')
    args = parser.parse_args()
    print(json.dumps(summarize(args.directory, audit=args.audit), indent=2))
