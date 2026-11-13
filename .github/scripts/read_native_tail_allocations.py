"""Validate synchronous CLR callback tail counts, not historical allocation ownership."""
import json
from pathlib import Path

from read_native_allocations import read_rows


def read(path, expected_operations):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    events = [r for r in rows if r.get("kind") == "engine-event"]
    tails = [r for r in rows if r.get("kind") == "native-engine-tail"]
    allocations = [r for r in rows if r.get("kind") in ("type", "coverage")]
    if len(tails) != 1 or rows != events + tails + allocations:
        raise ValueError("Missing, unordered or unknown tail records")
    started = stopped = opened = False
    iterations = operations = pending = 0
    thread = None
    for event in events:
        identity, current, count = event["id"], event["thread"], event["operations"]
        if type(current) is not int or current <= 0 or (thread is not None and thread != current) or stopped:
            raise ValueError("Invalid engine thread or event after benchmark stop")
        thread = current
        if identity in (1, 2):
            if count != 0:
                raise ValueError("Unexpected benchmark payload")
        elif type(count) is not int or count <= 0 or count % 8:
            raise ValueError("Invalid native operation count")
        if not started and 3 <= identity <= 6:
            continue
        if identity == 1 and not started:
            started = True
        elif not started:
            raise ValueError("Missing benchmark start")
        elif identity == 15 and not opened:
            opened, pending = True, count
        elif identity == 16 and opened and pending == count:
            opened, operations = False, count
            iterations += 1
        elif identity == 2 and not opened and iterations:
            stopped = True
        elif not 3 <= identity <= 14 or opened or iterations:
            raise ValueError("Unexpected engine stage or unmatched iteration")
    tail = tails[0]
    if (not stopped or tail.get("stopped") is not True or tail.get("iterations") != iterations
            or tail.get("nativeOperations") != operations or operations != expected_operations):
        raise ValueError("Incomplete tail or native // GC denominator mismatch")
    return {"tail": tail, "events": events, **read_rows(allocations)}
