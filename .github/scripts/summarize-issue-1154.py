from collections import Counter, defaultdict
import argparse
import csv
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("root", type=Path)
parser.add_argument("--caller-only", action="store_true")
parser.add_argument("--pairs", type=int, default=1_000)
args = parser.parse_args()
root = args.root
checkpoint_count = args.pairs * 3
windows = {}
types = {}
with (root / "objects.jsonl").open() as source:
    for row in map(json.loads, source):
        if row["kind"] == "window":
            windows[row["index"]] = row["measured"], row["callerThread"]
        elif row["kind"] == "window-type":
            types[row["index"], row["classId"]] = row["type"]
assert set(windows) == set(range(checkpoint_count)), f"Expected {args.pairs:,} native, pipeline and gap checkpoint groups"
for start in range(0, checkpoint_count, 3):
    assert windows[start][1] == windows[start + 1][1] == windows[start + 2][1]

caller_bytes = Counter()
caller_stacks = defaultdict(Counter)
snapshot_failures = Counter()
frames = {}
coverage = None
with (root / "stacks.jsonl").open() as source:
    for row in map(json.loads, source):
        if row["kind"] == "frame":
            frames[row["functionId"]] = row["name"]
        elif row["kind"] == "stack-coverage":
            coverage = row
        if row["kind"] != "allocation-stack":
            continue
        index = row["index"]
        is_caller = row["thread"] == windows[index][1]
        if args.caller_only and not is_caller:
            raise ValueError(f"Callback on non-caller thread in window {index}")
        if row["snapshotResult"] != 0 or not row["frames"] or not row["thread"]:
            if is_caller:
                owner = "caller"
            elif row["thread"]:
                owner = "other"
            else:
                owner = "unknown"
            snapshot_failures[owner] += row["objects"]
        if is_caller:
            caller_bytes[index] += row["bytes"]
            allocation_type = types[index, row["classId"]]
            caller_stacks[index][allocation_type, tuple(row["frames"])] += row["bytes"]

assert coverage is not None and coverage["failures"] == sum(snapshot_failures.values())
print("Failed stack snapshots (caller / other / unknown):",
      tuple(snapshot_failures[owner] for owner in ("caller", "other", "unknown")))
with (root / "boundaries.csv").open(encoding="utf-8", newline="") as source:
    phases = list(csv.DictReader(source))
assert [int(row["batch"]) for row in phases] == list(range(1, args.pairs + 1))
for index, row in enumerate(phases):
    native = int(row["nativeEnd"]) - int(row["nativeStart"])
    pipeline = int(row["pipelineEnd"]) - int(row["pipelineStart"])
    assert (native, pipeline) == (windows[index * 3][0], windows[index * 3 + 1][0]), index + 1
print("Phase counters agree with caller checkpoints for every batch.")
print("Counter pairs (native / pipeline):", dict(Counter(
    (windows[i][0], windows[i + 1][0]) for i in range(0, checkpoint_count, 3)
)))
print("Caller callback bytes (native / pipeline / gap):", dict(Counter(
    tuple(caller_bytes[index] for index in range(i, i + 3)) for i in range(0, checkpoint_count, 3)
)))
for i in range(0, checkpoint_count, 3):
    if windows[i][0] != windows[i + 1][0]:
        print(f"Batch {i // 3 + 1}, caller {windows[i][1]}")
        for index, name in ((i, "native"), (i + 1, "pipeline")):
            print(f"  {name}: counter={windows[index][0]}, callbacks={caller_bytes[index]}")
            for (allocation_type, stack), size in caller_stacks[index].most_common():
                stack_name = " > ".join(frames.get(frame, f"<unresolved frame {frame}>") for frame in stack) or "<stack unavailable>"
                print(f"    {size} B {allocation_type}: {stack_name}")
