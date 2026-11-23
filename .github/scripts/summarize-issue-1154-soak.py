"""Report allocation differences and garbage collections around the measured pairs."""
import csv
import sys
from collections import Counter
from pathlib import Path

FIELD_NAMES = ("batch", "native", "rent", "process", "dispose", "gen0", "gen1", "gen2")
CONTEXT_FIELDS = (
    "limit_changed", "pointer_delta", "remaining_before", "remaining_after",
    "soh", "uoh", "before_drift", "after_drift",
)
EXPECTED_PAIRS = 65_536

histogram = Counter()
byte_pairs = Counter()
failures = Counter()
context_changes = Counter()

with Path(sys.argv[1]).open(encoding="utf-8", newline="") as capture:
    rows = csv.DictReader(capture)
    if tuple(rows.fieldnames or ()) != FIELD_NAMES + CONTEXT_FIELDS:
        raise SystemExit("Unexpected allocation capture columns.")

    pairs = 0
    for pairs, row in enumerate(rows, start=1):
        batch, native, rent, process, dispose, gen0, gen1, gen2 = (
            int(row[field]) for field in FIELD_NAMES
        )
        if batch != pairs:
            raise SystemExit(f"Expected batch {pairs}, received {batch}.")

        pipeline = rent + process + dispose
        byte_pairs[native, pipeline] += 1
        highest_collection = 2 if gen2 else 1 if gen1 else 0 if gen0 else None
        histogram[highest_collection] += 1
        limit_changed, pointer_delta, remaining_before, remaining_after, soh, uoh, before_drift, after_drift = (
            int(row[field]) for field in CONTEXT_FIELDS
        )
        if limit_changed not in (0, 1) or min(remaining_before, remaining_after) < 0:
            raise SystemExit(f"Invalid allocation context in batch {batch}.")
        reconstructed = soh + uoh + remaining_before - remaining_after
        if reconstructed - before_drift - after_drift != process:
            raise SystemExit(f"Allocation context delta does not reconcile in batch {batch}.")
        if not limit_changed and pointer_delta != remaining_before - remaining_after:
            raise SystemExit(f"Unchanged limit does not reconcile with pointer movement in batch {batch}.")
        context_changes[limit_changed, bool(before_drift or after_drift)] += 1
        if native == pipeline:
            continue

        key = (native, rent, process, dispose, highest_collection)
        if failures[key] == 0:
            print(
                f"First batch with this difference: {batch}. {native=} {pipeline=} "
                f"{rent=} {process=} {dispose=} {gen0=} {gen1=} {gen2=}"
            )
        print(
            f"Allocation context for unequal batch {batch}: {limit_changed=} {pointer_delta=} "
            f"{remaining_before=} {remaining_after=} {soh=} {uoh=} {before_drift=} {after_drift=}"
        )
        failures[key] += 1

if pairs != EXPECTED_PAIRS:
    raise SystemExit(f"Expected {EXPECTED_PAIRS} pairs, received {pairs}.")

print(f"Measured {pairs} pairs. Unequal pairs: {sum(failures.values())}.")
for (native, pipeline), count in byte_pairs.most_common():
    print(f"{count} pairs: native={native} B, pipeline={pipeline} B")
for generation in (None, 0, 1, 2):
    label = "no collection" if generation is None else f"through generation {generation}"
    print(f"{label}: {histogram[generation]} pairs")

for (limit_changed, boundary_drift), count in sorted(context_changes.items()):
    print(f"{count} pairs: {limit_changed=} {boundary_drift=}")

if failures:
    for (native, rent, process, dispose, generation), count in failures.most_common():
        print(f"{count} pairs: {native=} {rent=} {process=} {dispose=} {generation=}")
