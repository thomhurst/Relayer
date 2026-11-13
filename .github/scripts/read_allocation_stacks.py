"""Validate CLR snapshot coverage against the same execution's exact callbacks."""
import json
from collections import Counter
from pathlib import Path

from read_native_allocations import read


def validate(rows, report):
    if not rows or rows[-1].get("kind") != "stack-coverage":
        raise ValueError("Incomplete stack coverage")
    coverage = rows[-1]
    if coverage.get("failures") != 0:
        raise ValueError("CLR snapshot failures")
    frames = {}
    actual = Counter()
    totals = Counter()
    for row in rows[:-1]:
        if row.get("kind") == "frame":
            key = row["functionId"]
            if type(key) is not int or key < 0 or key in frames or not row.get("name"):
                raise ValueError("Invalid or duplicate frame")
            frames[key] = row["name"]
        elif row.get("kind") == "allocation-stack":
            if row.get("snapshotResult") != 0 or not row.get("frames"):
                raise ValueError("Incomplete CLR snapshot")
            if any(type(row.get(key)) is not int or row[key] <= 0 for key in ("thread", "classId", "objects", "bytes")):
                raise ValueError("Invalid stack allocation counts")
            if type(row.get("index")) is not int or row["index"] < 0:
                raise ValueError("Invalid stack window")
            if any(type(frame) is not int or frame not in frames for frame in row["frames"]):
                raise ValueError("Unknown stack frame")
            for key in ("objects", "bytes"):
                actual[row["index"], row["classId"], key] += row[key]
                totals[key] += row[key]
        else:
            raise ValueError("Unknown stack record")
    expected = Counter()
    for window in report["windows"]:
        for entry in window["types"]:
            for key in ("objects", "bytes"):
                expected[window["index"], entry["classId"], key] = entry[key]
    if not expected or actual != expected:
        raise ValueError("Stack allocations do not match checkpoint callbacks")
    for key in ("objects", "bytes"):
        if totals[key] != coverage.get(key) or totals[key] != report["coverage"][key]:
            raise ValueError("Stack coverage total mismatch")
    return {"coverage": coverage, "unresolved_frames": [key for key, name in frames.items() if name.startswith("<unresolved")],
            "frames": frames, "allocations": [row for row in rows if row["kind"] == "allocation-stack"]}


if __name__ == "__main__":
    import sys
    rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
    print(json.dumps(validate(rows, read(sys.argv[2])), indent=2))
