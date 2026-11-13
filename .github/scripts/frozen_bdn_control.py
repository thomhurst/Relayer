#!/usr/bin/env python3
"""Replay a retained BenchmarkDotNet engine, without rebuilding or changing its job."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from frozen_concurrent_control import verify


BENCHMARK = "Relayer.Benchmarks.EndToEndBenchmarks.ConcurrentRoundTrip"
LAUNCHES = ("baseline", "candidate", "candidate", "baseline")
# Explicit original generated runner selectors; ConsumptionModel.Queue=0, Log=1.
CASES = {
    "concurrent": ("EndToEndBenchmarks", "ConcurrentRoundTrip", None, 8),
    "scope-only": ("ConsumePipelineBenchmarks", "ScopeOnly", "instance.ScopedHandler = true;", 1),
    "scoped-dispatch": ("ConsumePipelineBenchmarks", "DirectScopedDispatch", "instance.ScopedHandler = true;", 1),
    "log-composed": ("InMemoryTransportBenchmarks", "ComposedRoundTrip", "instance.Mode = (Relayer.Transports.ConsumptionModel)(1);", 1),
    "queue-composed": ("InMemoryTransportBenchmarks", "ComposedRoundTrip", "instance.Mode = (Relayer.Transports.ConsumptionModel)(0);", 1),
    "queue-native": ("InMemoryTransportBenchmarks", "NativeRoundTrip", "instance.Mode = (Relayer.Transports.ConsumptionModel)(0);", 1),
}


def executable(root, case="concurrent"):
    """Require the selected original generated runner, parameter and GC-stat job."""
    owner, method, parameter, operations = CASES[case]
    root = Path(root).resolve()
    files = {entry["path"] for entry in json.loads((root / "inventory-sha256.json").read_text())["files"]}
    sources = list(root.glob("builds/*/Relayer.Benchmarks-DefaultJob-*/*.notcs"))
    if len(sources) != 1:
        raise ValueError("Expected one retained generated BenchmarkDotNet source")
    source = sources[0]
    text = source.read_text()
    runners = re.findall(rf"public unsafe class Runnable_(\d+) : global::Relayer\.Benchmarks\.{owner}\s*\{{(.*?)(?=public unsafe class Runnable_|\Z)", text, re.S)
    matches = [(identity, body) for identity, body in runners
               if f"AwaitHelper.GetResult({method}())" in body
               and (parameter is None or parameter in body)]
    if len(matches) != 1:
        raise ValueError(f"Expected one original {case} runner for selected parameter")
    identity, body = matches[0]
    if f"OperationsPerInvoke = {operations}," not in body or "MeasureExtraStats = true," not in body:
        raise ValueError(f"Original runner must measure {operations} operations and GC statistics")
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


def runtime_command(image, version):
    if version is None:
        return ["dotnet", str(image)]
    if not re.fullmatch(r"10\.0\.[0-9]+", version):
        raise ValueError("Expected an exact net10 runtime patch")
    options = json.loads(image.with_suffix(".runtimeconfig.json").read_text())["runtimeOptions"]
    framework = options.get("framework", {})
    if (options.get("frameworks") or framework.get("name") != "Microsoft.NETCore.App"
            or not framework.get("version", "").startswith("10.0.")):
        raise ValueError("Exact patch selection requires one Microsoft.NETCore.App net10 framework")
    # Native host options preserve every retained byte; Disable prevents patch roll-forward.
    return ["dotnet", "--fx-version", version, "--roll-forward", "Disable", str(image)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--case", choices=tuple(CASES), default="concurrent",
                        help="Select an original generated runner and parameter, preserving its native job.")
    parser.add_argument("--native-executor", type=Path,
                        help="Opt-in concurrent, scoped-dispatch or scoped ScopeOnly replay with BenchmarkDotNet native anonymous-pipe parent (FrozenNativeHost.dll).")
    parser.add_argument("--trace-tool", type=Path,
                        help="Separate instrumented replay; retain whole-process raw allocation profiles.")
    parser.add_argument("--runtime-version", choices=("10.0.14", "10.0.15"),
                        help="Bind the original executable to an installed exact patch without editing retained inputs.")
    parser.add_argument("--same-image", choices=("baseline", "candidate"),
                        help="Negative control: launch the selected original image four times, without an image comparison.")
    parser.add_argument("--allocation-boundary", choices=("tail", "method"), default="tail",
                        help="Choose native final-stop tail or exact MeasureWithGc entry/leave.")
    parser.add_argument("--allocation-profiler", type=Path,
                        help="Separate exact CLR callback counts in the selected native boundary.")
    args = parser.parse_args()
    from frozen_scope_control import result as timing_result
    if args.native_executor and (args.case not in ("concurrent", "scoped-dispatch", "scope-only") or args.trace_tool):
        parser.error("Native parent supports concurrent, scoped-dispatch or scoped ScopeOnly replay without an external trace launcher")
    if args.allocation_profiler and args.case != "concurrent":
        parser.error("Exact allocation callbacks support only the concurrent benchmark")
    owner, method, parameter, operations = CASES[args.case]
    benchmark = f"Relayer.Benchmarks.{owner}.{method}"
    if args.trace_tool and args.allocation_profiler:
        parser.error("Choose sampled traces or synchronous CLR allocation callbacks, not both")
    builds = args.artifact.resolve() / "performance-builds"
    # Verify all three original inventories before starting any process. A2 deliberately
    # repeats the first baseline executable, as in the external-probe ABBA.
    commits = {side: verify(builds / side) for side in (*dict.fromkeys(LAUNCHES), "baseline-after")}
    runners = {side: executable(builds / side, args.case) for side in dict.fromkeys(LAUNCHES)}
    launches = (args.same_image,) * 4 if args.same_image else LAUNCHES
    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {"commits": commits, "launches": launches, "sameImage": args.same_image, "benchmark": benchmark,
                "case": args.case, "parameter": parameter,
                "engine": "Original retained BenchmarkDotNet generated executable and Default job",
                "host": "Native NoAcknowledgementConsoleHost; original anonymous-pipe parent absent",
                "operationsPerInvoke": operations, "instrumented": args.trace_tool is not None or args.allocation_profiler is not None,
                "limits": "New original-engine executions, not historical reconstruction, allocation-type attribution, throughput equivalence or native-client budget evidence. No external fixed-count driver, sampled profile, job override or threshold change.",
                "requestedRuntime": args.runtime_version, "commands": []}
    for side in launches:
        image, identity = runners[side]
        protocol["commands"].append([*runtime_command(image, args.runtime_version), "--benchmarkName", benchmark,
                                     "--job", "Default", "--benchmarkId", identity])
    if args.native_executor:
        # The native executor receives a CLI wrapper only to select the exact
        # runtime. It owns the pipes, Broker, priority and process lifetime.
        wrapper = args.output.resolve() / "native-dotnet.sh"
        runtime = ["--fx-version", args.runtime_version, "--roll-forward", "Disable"] if args.runtime_version else []
        wrapper.write_text("#!/bin/sh\nexec dotnet " + shlex.join(runtime) + ' "$@"\n')
        wrapper.chmod(0o700)
        protocol["host"] = "BenchmarkDotNet 0.15.8 DotNetCliExecutor: native anonymous pipes/Broker/process priority"
        protocol["limits"] += " Native-parent replay is a bundled protocol intervention, not isolation of pipe effects or reconstruction of the original process scheduling."
        protocol["commands"] = [
            ["dotnet", str(args.native_executor.resolve()), str(runners[side][0]), runners[side][1], str(wrapper),
             str((args.output / f"{index}-{side}-native.log").resolve()),
             str((args.output / f"{index}-{side}-native.json").resolve())]
            for index, side in enumerate(launches, 1)]
        if args.case != "concurrent":
            for command in protocol["commands"]:
                command.append(args.case)
    if args.trace_tool:
        protocol["profile_limits"] = (
            "Separate instrumented native-engine four-launch bracket with gc-verbose allocation sampling. "
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
            for index, (side, command) in enumerate(zip(launches, protocol["commands"]), 1)]
    if args.allocation_profiler:
        from read_native_tail_allocations import read as read_tail
        from read_native_allocations import read as read_allocations

        protocol["allocationProfiler"] = str(args.allocation_profiler.resolve())
        protocol["limits"] = (
            "Separate instrumented native-engine ABBA; exact CLR ObjectAllocated notifications and GetObjectSize2 sizes, not all allocation-context overhead. "
            "Synchronous native EventPipe callbacks reset counts at each WorkloadActualStop, retaining only the final stop to BenchmarkStop envelope. "
            "Includes final timed cleanup/GC/logging and extra-stat setup/work/cleanup, not exact MeasureWithGc. "
            "Callbacks, metadata resolution and locking change allocation paths and scheduling. No stacks or exact call-site attribution. "
            "New executions do not establish historical ownership, production regression, throughput equivalence or a performance trade-off. "
            "No independent-total subtraction or threshold relaxation.")
    if args.allocation_profiler and args.allocation_boundary == "method":
        protocol["allocationMethod"] = "BenchmarkDotNet.Engines.Engine.MeasureWithGc"
        protocol["allocationProfiler"] = str(args.allocation_profiler.resolve())
        protocol["limits"] = (
            "Exact CLR ObjectAllocated callbacks between entry/leave of original non-inlined "
            "Engine.MeasureWithGc, including its ReadInitial/ReadFinal work and concurrent background allocations. "
            "This is a separate instrumented adaptive-engine execution: callbacks change allocation fastpaths, "
            "ELT changes JIT code and native locking/metadata change scheduling. Object sizes exclude some "
            "allocation-context/alignment overhead; they need not sum to the native GC counter. "
            "No stacks, historical reconstruction, cross-control subtraction, gate rerun or throughput claim.")
    if args.native_executor and args.allocation_profiler:
        protocol["limits"] += " Native-parent child-only profiling; managed parent is unprofiled. Bundled protocol/instrumentation intervention, not isolated pipe effects or historical attribution."
    if args.same_image:
        protocol["limits"] += " Same-image negative control: no candidate-versus-baseline comparison; process order, adaptive operation counts and host drift remain uncontrolled."
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    outcomes = []
    failed = False
    for index, (side, command) in enumerate(zip(launches, protocol["commands"]), 1):
        log_path = args.output / f"{index}-{side}.log"
        outcome = {"launch": index, "side": side}
        with log_path.open("w") as log:
            try:
                environment = None
                if args.allocation_profiler:
                    environment = dict(os.environ, CORECLR_ENABLE_PROFILING="1",
                                       CORECLR_PROFILER="{32B7EF07-25E8-4AC7-B011-9A4321D702C8}",
                                       CORECLR_PROFILER_PATH=str(args.allocation_profiler.resolve()),
                                       RELAYER_ALLOCATION_BDN_TAIL="1",
                                       RELAYER_ALLOCATION_OUTPUT=str((args.output / f"{index}-{side}-objects.jsonl").resolve()))
                if args.allocation_profiler and args.allocation_boundary == "method":
                    environment.pop("RELAYER_ALLOCATION_BDN_TAIL", None)
                    environment.update(RELAYER_ALLOCATION_TYPE="BenchmarkDotNet.Engines.Engine",
                                       RELAYER_ALLOCATION_METHOD="MeasureWithGc")
                if args.native_executor and environment is not None:
                    # Profile only the benchmark child. Profiling the managed parent
                    # too would share/truncate its report and contaminate callbacks.
                    child_variables = {key: value for key, value in environment.items()
                                       if key in ("CORECLR_ENABLE_PROFILING", "CORECLR_PROFILER", "CORECLR_PROFILER_PATH")
                                       or key.startswith("RELAYER_ALLOCATION_")}
                    child_wrapper = args.output.resolve() / f"{index}-{side}-native-dotnet.sh"
                    child_wrapper.write_text(wrapper.read_text().replace(
                        "exec dotnet ", "exec env " + shlex.join(
                            [f"{key}={value}" for key, value in child_variables.items()]) + " dotnet "))
                    child_wrapper.chmod(0o700)
                    command[4] = str(child_wrapper)
                    protocol["commands"][index - 1] = command
                    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
                    environment = {key: value for key, value in environment.items()
                                   if key not in child_variables}
                subprocess.run(command, cwd=runners[side][0].parent, stdout=log,
                               stderr=subprocess.STDOUT, check=True, timeout=900, env=environment)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                log.write(f"\nLaunch failed: {error}\n")
                outcome["error"] = str(error)
                failed = True
        if "error" not in outcome:
            try:
                text = log_path.read_text()
                if args.native_executor:
                    receipt = json.loads((args.output / f"{index}-{side}-native.json").read_text())
                    if (receipt["ExitCode"] != 0 or not receipt["FoundExecutable"]
                            or not receipt["IsSuccess"] or receipt["Errors"]):
                        raise ValueError("Native executor did not report successful completion")
                    # The native logger includes Broker signal lines (AfterAll),
                    # which ExecuteResult intentionally omits from PrefixedLines.
                    text = (args.output / f"{index}-{side}-native.log").read_text()
                    if args.case == "scoped-dispatch":
                        expected = f"--benchmarkName {benchmark} --job Default --benchmarkId {runners[side][1]}"
                        if (receipt.get("benchmarkArguments") != expected
                                or receipt.get("selectedCase") != args.case
                                or receipt.get("operationsPerInvoke") != operations):
                            raise ValueError("Native executor benchmark identity differs from selected original runner")
                    outcome["nativeReceipt"] = f"{index}-{side}-native.json"
                if args.runtime_version:
                    versions = re.findall(r"^// Runtime=\.NET ([0-9.]+) ", text, re.M)
                    if versions != [args.runtime_version]:
                        raise ValueError(f"Unexpected loaded runtime: {versions}")
                    outcome["runtime"] = versions[0]
                # Reuse the existing native WorkloadResult parser; these are new
                # process samples, never a gate verdict or historical reconstruction.
                outcome.update(timing_result(text))
                if args.case == "concurrent":
                    outcome.update(result(text))
            except (ValueError, OSError, KeyError, TypeError) as error:
                outcome["error"] = str(error)
                failed = True
        if args.allocation_profiler:
            report = args.output / f"{index}-{side}-objects.jsonl"
            try:
                if "operations" not in outcome:
                    raise ValueError("No native GC-stat denominator for callback report")
                if args.allocation_boundary == "method":
                    validated = read_allocations(report)
                    if validated["coverage"].get("boundary") != "method-enter-leave":
                        raise ValueError("Missing exact native-engine method boundary")
                    outcome["objectReport"] = report.name
                    outcome["callbackCoverage"] = validated["coverage"]
                else:
                    validated = read_tail(report, outcome["operations"])
                outcome["allocationReport"] = report.name
                outcome["callbackObjects"] = validated["coverage"]["objects"]
                outcome["callbackBytes"] = validated["coverage"]["bytes"]
            except (ValueError, OSError, KeyError, TypeError) as error:
                outcome["allocationError"] = str(error)
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
