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
    parser.add_argument("--exact-invocations", action="store_true",
                        help="Separate instrumented fixture control using synchronous thread-local counters.")
    parser.add_argument("--exact-releases", action="store_true",
                        help="Separate exact calling-thread counts around each eight-write release batch.")
    parser.add_argument("--exact-roundtrips", action="store_true",
                        help="Separate exact synchronous full-delivery delegate-call counts, eight messages per record.")
    parser.add_argument("--exact-driver", action="store_true",
                        help="Separate full-delivery calling-thread invocation and original BDN completion counts.")
    parser.add_argument("--bdn-await", action="store_true",
                        help="Use original BDN synchronous ValueTask waiter in the full-delivery probe.")
    args = parser.parse_args()
    if args.bdn_await and (args.exact_invocations or args.exact_releases or args.exact_roundtrips or args.exact_driver):
        parser.error("BDN full-delivery waiter and exact counters are distinct controls")
    if sum((args.exact_invocations, args.exact_releases, args.exact_roundtrips, args.exact_driver)) > 1:
        parser.error("Exact regions require separate ABBAs")
    if bool(args.trace_tool) != bool(args.profile_reader):
        parser.error("--trace-tool and --profile-reader must be supplied together")
    if (args.exact_invocations or args.exact_releases or args.exact_roundtrips or args.exact_driver) and args.trace_tool:
        parser.error("Exact invocation counters and sampled profiles require separate ABBAs")
    args.output.mkdir(parents=True, exist_ok=False)
    builds = args.artifact / "performance-builds"
    commits = {side: verify(builds / side) for side in ("baseline", "candidate", "baseline-after")}
    protocol = {"commits": commits, "launches": ["baseline", "candidate", "candidate", "baseline"],
                "warmup_batches": 4096, "measured_batches": 32768, "messages_per_batch": 8,
                "counter": "GC.GetTotalAllocatedBytes(true)",
                "limits": "Inclusive fixed-count control; not original BDN replay, allocation-owner attribution, throughput equivalence or native-client budget evidence."}
    protocol["completion_boundary"] = "original-BenchmarkDotNet-AwaitHelper.GetResult" if args.bdn_await or args.exact_driver else "asynchronous-await"
    protocol["instrumented"] = args.trace_tool is not None or args.exact_invocations or args.exact_releases or args.exact_roundtrips or args.exact_driver
    protocol["exact_invocations"] = args.exact_invocations
    protocol["exact_releases"] = args.exact_releases
    protocol["exact_roundtrips"] = args.exact_roundtrips
    protocol["exact_driver"] = args.exact_driver
    if args.exact_roundtrips:
        protocol["roundtrip_boundary"] = "One synchronous original ConcurrentRoundTrip call (eight messages); no await. Calling-thread work only, excluding later continuations and other threads. No historical allocation attribution."
    if args.exact_driver:
        protocol["driver_boundary"] = "Separate synchronous workload invocation and original BDN GetResult calls; exact calling-thread bytes only. Includes inline continuations, excludes other threads. No await between counters. Not exact object types or historical attribution."
    if args.exact_releases:
        protocol["release_boundary"] = "Eight synchronous Release.Writer.TryWrite calls per record; no await. Calling-thread work only, not historical allocation attribution."
    if args.exact_invocations:
        protocol["invocation_counter"] = "GC.GetAllocatedBytesForCurrentThread()"
        protocol["invocation_boundary"] = "Synchronous cached handler call only; no await between counter reads. Excludes other threads and subsequent continuations; does not identify object types or the historical delta."
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
        if args.bdn_await:
            command.append("--bdn-await")
        if args.exact_driver:
            command.append("--exact-driver")
        if args.exact_invocations:
            command.append("--exact-invocations")
        if args.exact_releases:
            command.append("--exact-releases")
        if args.exact_roundtrips:
            command.append("--exact-roundtrips")
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
