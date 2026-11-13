"""Validate exact callback reports; never equate these counts to historical execution."""
import json
from pathlib import Path


def read_rows(rows):
    if not rows or rows[-1].get("kind") != "coverage":
        raise ValueError("Incomplete allocation report coverage")
    window_rows = [row for row in rows if row.get("kind") in ("window", "window-type")]
    rows = [row for row in rows if row.get("kind") not in ("window", "window-type")]
    coverage = [row for row in rows if row.get("kind") == "coverage"]
    types = [row for row in rows if row.get("kind") == "type"]
    if len(coverage) != 1 or not types or len(rows) != len(types) + 1 or rows[-1] != coverage[0]:
        raise ValueError("Incomplete or unknown allocation report records")
    if coverage[0]["failures"] != 0:
        raise ValueError("Profiler reported unresolved metadata or callback failures")
    if coverage[0].get("boundary") == "method-enter-leave":
        if coverage[0].get("methodEntries") != 1 or coverage[0].get("methodLeaves") != 1:
            raise ValueError("Incomplete or repeated method boundary")
    if len({row["classId"] for row in types}) != len(types):
        raise ValueError("Duplicate ClassID records")
    unavailable = 0
    for row in types:
        if "<metadata-unavailable" in row["type"].replace("<metadata-unavailable:0x80131351>", ""):
            raise ValueError("Unknown metadata-unavailable HRESULT")
        marked = row.get("metadataUnavailable", False)
        if type(marked) is not bool or marked != ("<metadata-unavailable:0x80131351>" in row["type"]):
            raise ValueError("Invalid metadata-unavailable type status")
        unavailable += marked
        if not row["type"] or "<unresolved" in row["type"] or "<type-depth" in row["type"]:
            raise ValueError("Unresolved type")
        if any(type(row[key]) is not int or row[key] <= 0 for key in ("classId", "objects", "bytes")):
            raise ValueError("Invalid allocation counts")
    if type(coverage[0].get("metadataUnavailableTypes", 0)) is not int or coverage[0].get("metadataUnavailableTypes", 0) != unavailable:
        raise ValueError("Metadata-unavailable coverage does not match types")
    for key in ("objects", "bytes"):
        if sum(row[key] for row in types) != coverage[0][key]:
            raise ValueError("Coverage total does not match type counts")
    windows = validate_windows(window_rows, coverage[0], types)
    return {"coverage": coverage[0], "types": types, "windows": windows}


def validate_windows(rows, coverage, types):
    expected = coverage.get("windows", 0)
    windows = [row for row in rows if row["kind"] == "window"]
    if type(expected) is not int or expected < 0 or expected % 3 or len(windows) != expected:
        raise ValueError("Incomplete window coverage")
    if [row["index"] for row in windows] != list(range(expected)):
        raise ValueError("Unordered or duplicate window boundaries")
    totals = {row["classId"]: [0, 0] for row in types}
    names = {row["classId"]: row["type"] for row in types}
    unavailable = {row["classId"]: row.get("metadataUnavailable", False) for row in types}
    result = []
    cursor = 0
    for index, window in enumerate(windows):
        if window["mode"] != index % 3 or type(window["measured"]) is not int or window["measured"] < 0:
            raise ValueError("Invalid window mode or measured counter")
        if "callerThread" in window and (type(window["callerThread"]) is not int or window["callerThread"] <= 0):
            raise ValueError("Invalid checkpoint caller thread")
        entries = []
        while cursor < len(rows) and rows[cursor]["kind"] == "window-type":
            entry = rows[cursor]
            if entry["index"] != index or entry["classId"] not in totals or entry["type"] != names[entry["classId"]]:
                raise ValueError("Invalid window type association")
            if entry.get("metadataUnavailable", False) != unavailable[entry["classId"]]:
                raise ValueError("Invalid window metadata-unavailable status")
            if any(type(entry[key]) is not int or entry[key] <= 0 for key in ("objects", "bytes")):
                raise ValueError("Invalid window allocation counts")
            entries.append(entry)
            for slot, key in enumerate(("objects", "bytes")):
                totals[entry["classId"]][slot] += entry[key]
            cursor += 1
        if len({entry["classId"] for entry in entries}) != len(entries):
            raise ValueError("Duplicate window ClassID")
        if cursor >= len(rows) or rows[cursor] != window:
            raise ValueError("Unordered window records")
        cursor += 1
        for key in ("objects", "bytes"):
            if type(window[key]) is not int or sum(entry[key] for entry in entries) != window[key]:
                raise ValueError("Window total does not match type counts")
        result.append({**window, "types": entries})
    if cursor != len(rows):
        raise ValueError("Unexpected window records")
    if expected:
        for row in types:
            if totals[row["classId"]] != [row["objects"], row["bytes"]]:
                raise ValueError("Window totals do not match aggregate coverage")
    return result


def read(path):
    return read_rows([json.loads(line) for line in Path(path).read_text().splitlines()])


if __name__ == "__main__":
    import sys
    print(json.dumps({str(path): read(path) for path in sys.argv[1:]}, indent=2))
