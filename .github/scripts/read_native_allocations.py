"""Validate exact callback reports; never equate these counts to historical execution."""
import json
from pathlib import Path


def read(path):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    coverage = [row for row in rows if row.get("kind") == "coverage"]
    types = [row for row in rows if row.get("kind") == "type"]
    if len(coverage) != 1 or not types or len(rows) != len(types) + 1 or rows[-1] != coverage[0]:
        raise ValueError("Incomplete or unknown allocation report records")
    if coverage[0]["failures"] != 0:
        raise ValueError("Profiler reported unresolved metadata or callback failures")
    if len({row["classId"] for row in types}) != len(types):
        raise ValueError("Duplicate ClassID records")
    for row in types:
        if not row["type"] or "<unresolved" in row["type"] or "<type-depth" in row["type"]:
            raise ValueError("Unresolved type")
        if any(type(row[key]) is not int or row[key] <= 0 for key in ("classId", "objects", "bytes")):
            raise ValueError("Invalid allocation counts")
    for key in ("objects", "bytes"):
        if sum(row[key] for row in types) != coverage[0][key]:
            raise ValueError("Coverage total does not match type counts")
    return {"coverage": coverage[0], "types": types}


if __name__ == "__main__":
    import sys
    print(json.dumps({str(path): read(path) for path in sys.argv[1:]}, indent=2))
