"""Summarize contiguous thread-local allocation counter reads."""
import argparse
import csv
from collections import Counter
from itertools import pairwise
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("capture", type=Path)
parser.add_argument("--pairs", type=int, default=1_000)
args = parser.parse_args()

fields = ("nativeStart", "nativeEnd", "pipelineStart", "rented", "processed", "pipelineEnd")
phases = ("gap before native", "native DI", "gap before pipeline", "rent", "process", "dispose")
distributions = {phase: Counter() for phase in phases}
mismatches = 0
regressions = 0
previous_end = None

with args.capture.open(encoding="utf-8", newline="") as source:
    samples = list(csv.DictReader(source))

if [int(sample["batch"]) for sample in samples] != list(range(1, args.pairs + 1)):
    raise SystemExit(f"Expected exactly {args.pairs:,} consecutive batch samples")

for sample in samples:
    values = [int(sample[field]) for field in fields]
    if previous_end is None:
        previous_end = values[0]
    deltas = [end - start for start, end in pairwise([previous_end, *values])]
    previous_end = values[-1]
    native = deltas[1]
    pipeline = sum(deltas[3:])
    collections = f"{sample['collectionsBefore']} to {sample['collectionsAfter']}"

    if native != pipeline:
        mismatches += 1
        print(f"Batch {sample['batch']}: native {native} B, pipeline {pipeline} B, GC count {collections}, deltas {deltas}")

    for phase, delta in zip(phases, deltas, strict=True):
        distributions[phase][delta] += 1
        if delta < 0:
            regressions += 1
            print(f"Batch {sample['batch']}: counter fell by {-delta} B in {phase}, GC count {collections}")

print(f"Measured {len(samples)} batches, {mismatches} unequal pairs, {regressions} counter decreases.")
for phase, distribution in distributions.items():
    print(f"{phase}: {dict(sorted(distribution.items()))}")
