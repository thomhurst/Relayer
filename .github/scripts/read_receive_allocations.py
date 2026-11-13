"""Correlate retained receive counters with callbacks from the same execution only."""
import json
import re
from pathlib import Path

from read_native_allocations import read


def correlate(report, log):
    counters = re.findall(r"^window=(\d+); scoped=(\d+); consumer=(\d+); receive=(\d+)$", log, re.MULTILINE)
    if len(counters) != 1000 or [int(row[0]) for row in counters] != list(range(1000)):
        raise ValueError("Expected every one of the 1000 measured receive triples")
    windows = report["windows"]
    if len(windows) != 3000:
        raise ValueError("Expected 3000 allocation checkpoints")
    for index, row in enumerate(counters):
        for mode in range(3):
            if windows[index * 3 + mode]["measured"] != int(row[mode + 1]):
                raise ValueError("Checkpoint counter differs from retained probe log")
    return report


if __name__ == "__main__":
    import sys
    print(json.dumps(correlate(read(sys.argv[1]), Path(sys.argv[2]).read_text()), indent=2))
