#!/usr/bin/env python3
"""Replay a retained BenchmarkDotNet engine, without rebuilding or changing its job."""
import argparse
import json
from pathlib import Path
import re
import subprocess

from frozen_concurrent_control import verify


BENCHMARK = "Relayer.Benchmarks.EndToEndBenchmarks.ConcurrentRoundTrip"
LAUNCHES = ("baseline", "candidate", "candidate", "baseline")


def executable(root):
    """Require the original generated runner and its eight-operation GC-stat job."""
    root = Path(root).resolve()
    files = {entry["path"] for entry in json.loads((root / "inventory-sha256.json").read_text())["files"]}
    sources = list(root.glob("builds/*/Relayer.Benchmarks-DefaultJob-*/*.notcs"))
    if len(sources) != 1:
        raise ValueError("Expected one retained generated BenchmarkDotNet source")
    source = sources[0]
    text = source.read_text()
    runners = re.findall(r"public unsafe class Runnable_(\d+) : global::Relayer\.Benchmarks\.EndToEndBenchmarks\s*\{(.*?)(?=public unsafe class Runnable_|\Z)", text, re.S)
    matches = [(identity, body) for identity, body in runners
               if "AwaitHelper.GetResult(ConcurrentRoundTrip())" in body]
    if len(matches) != 1:
        raise ValueError("Expected one original ConcurrentRoundTrip runner")
    identity, body = matches[0]
    if "OperationsPerInvoke = 8," not in body or "MeasureExtraStats = true," not in body:
        raise ValueError("Original runner must measure eight operations and GC statistics")
    images = list(source.parent.glob("bin/Release/net10.0/Relayer.Benchmarks-DefaultJob-*.dll"))
    if len(images) != 1:
        raise ValueError("Expected one retained generated BenchmarkDotNet executable")
    for path in (source, images[0]):
        if path.relative_to(root).as_posix() not in files:
            raise ValueError(f"Selected input is not inventoried: {path}")
    return images[0], identity


def result(text):
    # BDN 0.15.8 GcStats.ToOutputLine: gen0 gen1 gen2 allocatedBytes totalOperations.
    # TotalOperations already includes OperationsPerInvoke; do not divide by eight again.
    lines = re.findall(r"^// GC: +(\d+) +(\d+) +(\d+) +(\d+) +(\d+)\s*$", text, re.M)
    if len(lines) != 1 or len(re.findall(r"^// GC:", text, re.M)) != 1:
        raise ValueError("Expected exactly one complete native GC-stat result")
    if len(re.findall(r"^// AfterAll\s*$", text, re.M)) != 1 or not re.search(r"^WorkloadResult +\d+:", text, re.M):
        raise ValueError("Original engine did not report completed workload results")
    gen0, gen1, gen2, allocated, operations = map(int, lines[0])
    if operations <= 0 or operations % 8:
        raise ValueError("Native operation count must be positive and divisible by eight")
    return {"collections": [gen0, gen1, gen2], "allocatedBytes": allocated,
            "operations": operations, "bytesPerMessage": allocated / operations,
            "bdnRoundedBytesPerMessage": round(allocated / operations)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--trace-tool", type=Path,
                        help="Separate instrumented replay; retain whole-process raw allocation profiles.")
    args = parser.parse_args()
    builds = args.artifact.resolve() / "performance-builds"
    # Verify all three original inventories before starting any process. A2 deliberately
    # repeats the first baseline executable, as in the external-probe ABBA.
    commits = {side: verify(builds / side) for side in (*dict.fromkeys(LAUNCHES), "baseline-after")}
    runners = {side: executable(builds / side) for side in dict.fromkeys(LAUNCHES)}
    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {"commits": commits, "launches": LAUNCHES, "benchmark": BENCHMARK,
                "engine": "Original retained BenchmarkDotNet generated executable and Default job",
                "host": "Native NoAcknowledgementConsoleHost; original anonymous-pipe parent absent",
                "operationsPerInvoke": 8, "instrumented": args.trace_tool is not None,
                "limits": "New original-engine executions, not historical reconstruction, allocation-type attribution, throughput equivalence or native-client budget evidence. No external fixed-count driver, sampled profile, job override or threshold change.",
                "commands": []}
    for side in LAUNCHES:
        image, identity = runners[side]
        protocol["commands"].append(["dotnet", str(image), "--benchmarkName", BENCHMARK,
                                     "--job", "Default", "--benchmarkId", identity])
    if args.trace_tool:
        protocol["profile_limits"] = (
            "Separate instrumented native-engine ABBA with gc-verbose allocation sampling. "
            "Whole-process traces include startup, pilot, warmup, overhead and GC-stat work; "
            "there are no Relayer measurement markers. Native engine events permit a separate final-iteration-stop to benchmark-stop envelope, which includes cleanup/logging and GC-stat setup/work/cleanup, not exact allocation-measurement boundaries. Retain all raw captures and native logs. "
            "Weighted samples are not exact object counts or attribution of a historical delta. "
            "Profiler attachment changes scheduling; do not compare these totals as a gate, "
            "subtract independent controls, or apply the marker-filtered reader to these captures.")
        protocol["limits"] = protocol["profile_limits"]
        protocol["commands"] = [
            [str(args.trace_tool.resolve()), "collect", "--profile", "gc-verbose",
             "--providers", "BenchmarkDotNet.EngineEventSource:0xffffffffffffffff:4",
             "--output", str((args.output / f"{index}-{side}.nettrace").resolve()),
             "--show-child-io", "--", *command]
            for index, (side, command) in enumerate(zip(LAUNCHES, protocol["commands"]), 1)]
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    outcomes = []
    failed = False
    for index, (side, command) in enumerate(zip(LAUNCHES, protocol["commands"]), 1):
        log_path = args.output / f"{index}-{side}.log"
        outcome = {"launch": index, "side": side}
        with log_path.open("w") as log:
            try:
                subprocess.run(command, cwd=runners[side][0].parent, stdout=log,
                               stderr=subprocess.STDOUT, check=True, timeout=900)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                log.write(f"\nLaunch failed: {error}\n")
                outcome["error"] = str(error)
                failed = True
        if "error" not in outcome:
            try:
                outcome.update(result(log_path.read_text()))
            except ValueError as error:
                outcome["error"] = str(error)
                failed = True
        if args.trace_tool:
            trace = args.output / f"{index}-{side}.nettrace"
            if not trace.is_file() or trace.stat().st_size == 0:
                outcome["profileError"] = "Missing or empty raw allocation capture"
                failed = True
            else:
                outcome["rawTrace"] = trace.name
        outcomes.append(outcome)
        (args.output / "outcomes.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    # Preserve failure logs and attempt every launch, then fail closed on mutated inputs.
    for side in (*dict.fromkeys(LAUNCHES), "baseline-after"):
        verify(builds / side)
    if failed:
        raise SystemExit("One or more original-engine launches failed; inspect retained outcomes.")


if __name__ == "__main__":
    main()
