"""Correlate exact checkpoints with the real parallel TUnit test's retained output."""
import json
import re
from pathlib import Path

from read_native_allocations import read
from read_receive_allocations import correlate


# TUnit can repeat captured output in its detailed summary. Accept identical
# echoes only; never choose a favorable copy of a conflicting measurement.
WINDOW = re.compile(
    r"^\s*window=(\d+); scoped=Window \{ Bytes = (\d+),[^\r\n]*?\}; "
    r"consumer=Window \{ Bytes = (\d+),[^\r\n]*?\}; "
    r"receive=Window \{ Bytes = (\d+),[^\r\n]*?\}\s*$", re.MULTILINE)


def correlate_parallel(report, log):
    log = re.sub(r"\x1b\[[0-9;]*m", "", log)
    counters = {}
    for match in WINDOW.finditer(log):
        index, *values = map(int, match.groups())
        if index in counters and counters[index] != values:
            raise ValueError("Conflicting repeated test window")
        counters[index] = values
    if set(counters) != set(range(1000)):
        raise ValueError("Expected every one of the 1000 measured test triples")
    normalized = "\n".join(
        f"window={index}; scoped={values[0]}; consumer={values[1]}; receive={values[2]}"
        for index, values in sorted(counters.items()))
    result = correlate(report, normalized)
    result["test_output"] = {
        "unique_triples": len(counters),
        "rows_including_echoes": len(WINDOW.findall(log)),
        "limits": "Callbacks cover all process threads between test checkpoints, including other parallel tests. "
                  "Their object bytes are not the fixture's thread counters. No stacks or historical attribution."}
    return result


if __name__ == "__main__":
    import sys
    print(json.dumps(correlate_parallel(read(sys.argv[1]), Path(sys.argv[2]).read_text()), indent=2))
