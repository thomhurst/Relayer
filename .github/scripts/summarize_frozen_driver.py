#!/usr/bin/env python3
"""Validate exact-driver ABBA records; report regions without attributing historical totals."""
import argparse
import json
import math
from pathlib import Path


LAUNCHES = ["baseline", "candidate", "candidate", "baseline"]


def integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"Invalid nonnegative integer: {name}")
    return value


def region(record, batches):
    count = integer(record["Invocations"], "Invocations")
    total = integer(record["Bytes"], "Bytes")
    bins = record["Histogram"]
    values = [integer(item["Bytes"], "histogram bytes") for item in bins]
    counts = [integer(item["Invocations"], "histogram count") for item in bins]
    if count != batches or sum(counts) != count or any(value == 0 for value in counts):
        raise ValueError("Incomplete histogram invocation counts")
    if values != sorted(set(values)) or sum(value * count for value, count in zip(values, counts)) != total:
        raise ValueError("Invalid histogram byte sum or bins")
    return total


def summarize(directory):
    directory = Path(directory)
    protocol = json.loads((directory / "protocol.json").read_text())
    for field in ("warmup_batches", "measured_batches", "messages_per_batch"):
        integer(protocol[field], field)
    if (protocol["launches"] != LAUNCHES or protocol.get("exact_driver") is not True
            or protocol.get("instrumented") is not True
            or any(protocol.get(key, False) for key in ("exact_invocations", "exact_releases", "exact_roundtrips"))
            or (protocol["warmup_batches"], protocol["measured_batches"], protocol["messages_per_batch"]) != (4096, 32768, 8)):
        raise ValueError("Unexpected exact-driver protocol")
    records = []
    identities = set()
    for index, side in enumerate(LAUNCHES, 1):
        # Exactly one result is required; never skip a failed or partial launch.
        lines = (directory / f"{index}-{side}.log").read_text().splitlines()
        if len(lines) != 1:
            raise ValueError("Expected exactly one complete JSON result per launch")
        result = json.loads(lines[0])
        for field in ("warmupBatches", "measuredBatches", "messages"):
            integer(result[field], field)
        if (result["instrumentation"] != "exact-driver-calling-thread"
                or (result["warmupBatches"], result["measuredBatches"], result["messages"]) != (4096, 32768, 262144)):
            raise ValueError("Unexpected driver result identity or counts")
        if Path(result["image"]).parts[-7:-5] != (side, "builds"):
            raise ValueError("Launch loaded the wrong image side")
        identities.add((result["runtime"], result["arch"]))
        total = integer(result["bytes"], "inclusive bytes")
        reported = result["bytesPerMessage"]
        if type(reported) not in (int, float) or not math.isfinite(reported) or reported != total / 262144:
            raise ValueError("Inclusive bytes/message disagrees with byte total")
        regions = {name: region(result[name], 32768) for name in ("invocation", "completion")}
        if sum(regions.values()) > total:
            raise ValueError("Disjoint calling-thread regions exceed inclusive total")
        records.append({"launch": index, "side": side, "bytes": total, "bytes_per_message": reported,
                        "regions": {name: {"bytes": value, "bytes_per_message": value / 262144}
                                    for name, value in regions.items()}})
    if len(identities) != 1:
        raise ValueError("Mixed runtime or architecture")
    return {"runtime": next(iter(identities))[0], "arch": next(iter(identities))[1],
            "launches": records,
            "limits": "Separate instrumented experiment. Exact regional byte counts exclude other threads and do not identify object types or the historical allocation owner. Inclusive totals are not pipeline-only costs; do not subtract results from separate experiments."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.directory), indent=2))
