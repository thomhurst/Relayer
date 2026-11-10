#!/usr/bin/env python3
"""Inspect measured allocation samples; never infer exact byte ownership or stable thread roles."""
import argparse
import json
import math
from pathlib import Path


def integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("Expected an integer allocation count or thread ID")
    return value


def summarize(path):
    records = [json.loads(line) for line in Path(path).read_text().splitlines()]
    if not records or records[0].get("kind") != "measurement":
        raise ValueError("Expected measurement metadata before allocation records")
    measurement = records[0]
    integer(measurement["processId"], 1)
    for field in ("startMilliseconds", "stopMilliseconds"):
        value = measurement[field]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("Invalid measurement boundary")
    if measurement["startMilliseconds"] >= measurement["stopMilliseconds"]:
        raise ValueError("Measurement boundaries must be ordered")
    markers = {integer(measurement[name], 1) for name in ("startThreadId", "stopThreadId")}
    integer(measurement["excludedSamples"])
    if integer(measurement["samplesWithoutStacks"]) != 0:
        raise ValueError("Missing stacks prevent a complete owner summary")
    threads = {}
    owners = {}
    seen = set()
    for record in records[1:]:
        if record.get("kind") != "allocation":
            raise ValueError("Unexpected record in allocation profile")
        thread = integer(record["ThreadId"], 1)
        samples = integer(record["Samples"], 1)
        weight = integer(record["SampledBytes"], 1)
        owner = (record["Type"], record["Stack"])
        if any(not isinstance(value, str) or not value.strip() for value in owner):
            raise ValueError("Allocation type and complete stack are required")
        key = (thread, *owner)
        if key in seen:
            raise ValueError("Duplicate thread/type/stack record")
        seen.add(key)
        totals = threads.setdefault(thread, {"samples": 0, "sampledBytes": 0})
        totals["samples"] += samples
        totals["sampledBytes"] += weight
        group = owners.setdefault(owner, {"samples": 0, "sampledBytes": 0, "threadIds": set()})
        group["samples"] += samples
        group["sampledBytes"] += weight
        group["threadIds"].add(thread)
    if not threads:
        raise ValueError("No measured allocations")
    return {
        "limits": __doc__,
        "measurement": measurement,
        "samples": sum(group["samples"] for group in threads.values()),
        "sampledBytes": sum(group["sampledBytes"] for group in threads.values()),
        "threads": [{"threadId": thread, "emittedBoundaryMarker": thread in markers, **totals}
                    for thread, totals in sorted(threads.items())],
        "owners": [{"type": kind, "stack": stack, "samples": totals["samples"],
                    "sampledBytes": totals["sampledBytes"], "threadIds": sorted(totals["threadIds"])}
                   for (kind, stack), totals in sorted(owners.items(), key=lambda item:
                       (-item[1]["sampledBytes"], item[0]))],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.profile), indent=2))
