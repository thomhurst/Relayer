from collections import Counter, defaultdict
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
windows = {}
types = {}
with (root / "objects.jsonl").open() as source:
    for row in map(json.loads, source):
        if row["kind"] == "window":
            windows[row["index"]] = row["measured"], row["callerThread"]
        elif row["kind"] == "window-type":
            types[row["index"], row["classId"]] = row["type"]
assert set(windows) == set(range(3000)), "Expected 1,000 native, pipeline and gap checkpoints"
for start in range(0, 3000, 3):
    assert windows[start][1] == windows[start + 1][1] == windows[start + 2][1]

caller_bytes = Counter()
caller_stacks = defaultdict(Counter)
frames = {}
with (root / "stacks.jsonl").open() as source:
    for row in map(json.loads, source):
        if row["kind"] == "frame":
            frames[row["functionId"]] = row["name"]
        if row["kind"] != "allocation-stack":
            continue
        index = row["index"]
        if row["thread"] == windows[index][1]:
            caller_bytes[index] += row["bytes"]
            allocation_type = types[index, row["classId"]]
            caller_stacks[index][allocation_type, tuple(row["frames"])] += row["bytes"]

print("Counter pairs (native / pipeline):", dict(Counter(
    (windows[i][0], windows[i + 1][0]) for i in range(0, 3000, 3)
)))
print("Caller callback bytes (native / pipeline / gap):", dict(Counter(
    tuple(caller_bytes[index] for index in range(i, i + 3)) for i in range(0, 3000, 3)
)))
for i in range(0, 3000, 3):
    if windows[i][0] != windows[i + 1][0]:
        print(f"Batch {i // 3 + 1}, caller {windows[i][1]}")
        for index, name in ((i, "native"), (i + 1, "pipeline")):
            print(f"  {name}: counter={windows[index][0]}, callbacks={caller_bytes[index]}")
            for (allocation_type, stack), size in caller_stacks[index].most_common():
                print(f"    {size} B {allocation_type}: {' > '.join(frames[frame] for frame in stack)}")
