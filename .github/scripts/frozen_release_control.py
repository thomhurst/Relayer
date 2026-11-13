#!/usr/bin/env python3
"""Run an existing frozen-image ABBA with one explicit native scheduling intervention."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", choices=("async", "sync"))
    parser.add_argument("artifact", type=Path)
    parser.add_argument("probe", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--trace-tool", type=Path, required=True)
    parser.add_argument("--profile-reader", type=Path, required=True)
    parser.add_argument("--keep-traces", action="store_true", required=True)
    args = parser.parse_args()
    # Keep orchestration, verification, profiling, extraction and failures in their
    # existing owner. The child-only variable cannot affect another experiment.
    environment = dict(os.environ, RELAYER_FROZEN_RELEASE_CHANNEL=args.arm)
    metadata = {
        "arm": args.arm, "channel": "fixture Release only", "capacity": 8,
        "fullMode": "Wait", "singleReader": False, "singleWriter": False,
        "allowSynchronousContinuations": args.arm == "sync",
        "completionBoundary": "original-BenchmarkDotNet-AwaitHelper.GetResult",
        "limits": "Both arms replace the channel before setup and differ only in its native scheduling option. Scheduling and downstream execution change. New instrumented fixed-count executions are not historical allocation attribution, exact object counts, or permission for a production trade-off. Never subtract sampled weights or independent totals."
    }
    # Refuse overwrite before executing; retain the selected arm even on failure.
    metadata_path = args.output.with_name(args.output.name + "-intervention.json")
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with metadata_path.open("x") as output:
        json.dump(metadata, output, indent=2)
        output.write("\n")
    command = [sys.executable, str(Path(__file__).with_name("frozen_concurrent_control.py")),
               str(args.artifact), str(args.probe), str(args.output), "--bdn-await",
               "--trace-tool", str(args.trace_tool), "--profile-reader", str(args.profile_reader),
               "--keep-traces"]
    subprocess.run(command, env=environment, check=True)


if __name__ == "__main__":
    main()
