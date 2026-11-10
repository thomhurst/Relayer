#!/usr/bin/env python3
"""Fixed-count diagnostic, not a regression gate or an attribution from inclusive totals."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def verify(root):
    root = Path(root).resolve()
    manifest = json.loads((root / "inventory-sha256.json").read_text())
    for entry in manifest["files"]:
        path = (root / entry["path"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"Inventory path escapes archive: {entry['path']}")
        if path.stat().st_size != entry["length"] or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ValueError(f"Retained input changed: {path}")
    return manifest["commit"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("probe", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--trace-tool", type=Path)
    parser.add_argument("--profile-reader", type=Path)
    args = parser.parse_args()
    if bool(args.trace_tool) != bool(args.profile_reader):
        parser.error("--trace-tool and --profile-reader must be supplied together")
    args.output.mkdir(parents=True, exist_ok=False)
    builds = args.artifact / "performance-builds"
    commits = {side: verify(builds / side) for side in ("baseline", "candidate", "baseline-after")}
    protocol = {"commits": commits, "launches": ["baseline", "candidate", "candidate", "baseline"],
                "warmup_batches": 4096, "measured_batches": 32768, "messages_per_batch": 8,
                "counter": "GC.GetTotalAllocatedBytes(true)",
                "limits": "Inclusive fixed-count control; not original BDN replay, allocation-owner attribution, throughput equivalence or native-client budget evidence."}
    protocol["instrumented"] = args.trace_tool is not None
    if args.trace_tool:
        protocol["profile_limits"] = "Separate instrumented ABBA. Phase-filtered weighted samples can suggest owners, never exact object counts or subtraction from inclusive totals."
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    # All images verify before the first launch. Preserve every process output, including failures.
    failed = False
    for index, side in enumerate(protocol["launches"]):
        images = list((builds / side / "builds").glob("*/Relayer.Benchmarks-DefaultJob-*/bin/Release/net10.0/Relayer.Benchmarks.dll"))
        if len(images) != 1:
            raise ValueError(f"Expected one frozen image for {side}, found {len(images)}")
        command = ["dotnet", str(args.probe.resolve()), str(images[0].parent.resolve())]
        trace = args.output / f"{index + 1}-{side}.nettrace"
        if args.trace_tool:
            command = [str(args.trace_tool.resolve()), "collect", "--profile", "gc-verbose",
                       "--providers", "Relayer-FrozenMeasurement", "--output", str(trace.resolve()),
                       "--show-child-io", "--", *command]
        with (args.output / f"{index + 1}-{side}.log").open("w") as log:
            try:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                log.write(f"\nLaunch failed: {error}\n")
                failed = True
        if args.profile_reader and not trace.exists():
            with (args.output / f"{index + 1}-{side}.log").open("a") as log:
                log.write("\nProfile capture missing; allocation ownership cannot be inspected.\n")
            failed = True
        if args.profile_reader and trace.exists():
            with (args.output / f"{index + 1}-{side}-allocations.jsonl").open("w") as output:
                try:
                    subprocess.run(["dotnet", str(args.profile_reader.resolve()), str(trace.resolve())],
                                   stdout=output, stderr=subprocess.STDOUT, check=True, timeout=600)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                    output.write(f"\nProfile extraction failed: {error}\n")
                    failed = True
                else:
                    # Full measured stacks and sample weights are retained; raw captures are disposable.
                    trace.unlink()
                    trace.with_suffix(trace.suffix + ".etlx").unlink(missing_ok=True)
    if failed:
        raise SystemExit("One or more launches failed; inspect all retained outputs.")


if __name__ == "__main__":
    main()
