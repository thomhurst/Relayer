#!/usr/bin/env python3
"""Fixed ABBA replay of retained scope benchmark images, never a gate retry."""
import argparse
import json
import os
from pathlib import Path
import re
import statistics
import subprocess

from frozen_concurrent_control import verify

SIDES = ("baseline", "candidate", "baseline-after")
LAUNCHES = ("baseline", "candidate", "candidate", "baseline")
BENCHMARK = "Relayer.Benchmarks.ConsumePipelineBenchmarks.ScopeOnly"


def executable(root, scoped, method="ScopeOnly"):
    if method not in ("ScopeOnly", "ScopeAndHandlerResolution"):
        raise ValueError("Unsupported retained scope benchmark")
    root = Path(root).resolve()
    files = {item["path"] for item in json.loads((root / "inventory-sha256.json").read_text())["files"]}
    sources = list(root.glob("builds/*/Relayer.Benchmarks-DefaultJob-*/*.notcs"))
    if len(sources) != 1:
        raise ValueError("Expected one original generated source")
    source = sources[0]
    runners = re.findall(r"public unsafe class Runnable_(\d+) : global::Relayer\.Benchmarks\.ConsumePipelineBenchmarks\s*\{(.*?)(?=public unsafe class Runnable_|\Z)", source.read_text(), re.S)
    parameter = "instance.ScopedHandler = " + str(scoped).lower() + ";"
    matches = [(identity, body) for identity, body in runners
               if parameter in body and f"AwaitHelper.GetResult({method}())" in body]
    if len(matches) != 1:
        raise ValueError("Expected one original selected scope runner for selected parameter")
    identity, body = matches[0]
    if "OperationsPerInvoke = 1," not in body or "MeasureExtraStats = true," not in body:
        raise ValueError("Expected original one-operation GC-stat job")
    images = list(source.parent.glob("bin/Release/net10.0/Relayer.Benchmarks-DefaultJob-*.dll"))
    if len(images) != 1:
        raise ValueError("Expected one original executable")
    for path in (source, images[0]):
        if path.relative_to(root).as_posix() not in files:
            raise ValueError("Selected input is not inventoried")
    return images[0], identity


def result(text):
    gc = re.findall(r"^// GC: +(\d+) +(\d+) +(\d+) +(\d+) +(\d+)\s*$", text, re.M)
    samples = re.findall(r"^WorkloadResult +\d+: +\d+ op, +[0-9.]+ ns, +([0-9.]+) ns/op\s*$", text, re.M)
    if len(gc) != 1 or len(re.findall(r"^// GC:", text, re.M)) != 1 or not samples or len(re.findall(r"^// AfterAll\s*$", text, re.M)) != 1:
        raise ValueError("Incomplete or ambiguous native results")
    gen0, gen1, gen2, allocated, operations = map(int, gc[0])
    if operations <= 0:
        raise ValueError("Expected positive native operation count")
    values = list(map(float, samples))
    return {"collections": [gen0, gen1, gen2], "allocatedBytes": allocated,
            "operations": operations, "bytesPerOperation": allocated / operations,
            "workloadResultNs": values, "meanNs": statistics.mean(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--jit-disassembly", action="store_true",
                        help="Separate code-capture experiment; its timings are not performance evidence")
    parser.add_argument("--tiered-compilation", choices=("default", "off"), default="default",
                        help="Diagnostic runtime control; off uses native DOTNET_TieredCompilation=0")
    parser.add_argument("--method", choices=("ScopeOnly", "ScopeAndHandlerResolution"),
                        default="ScopeOnly", help="Select the original generated benchmark runner")
    parser.add_argument("--jit-method", default="*",
                        help="Native DOTNET_JitDisasm method filter; requires --jit-disassembly")
    parser.add_argument("--ready-to-run", choices=("default", "on", "off"), default="default",
                        help="Diagnostic runtime control; uses native DOTNET_ReadyToRun=1/0")
    args = parser.parse_args()
    if args.jit_method != "*" and not args.jit_disassembly:
        parser.error("--jit-method requires --jit-disassembly")
    if not args.jit_method.strip():
        parser.error("--jit-method must not be empty")
    benchmark = BENCHMARK.rsplit(".", 1)[0] + "." + args.method
    builds = args.artifact.resolve() / "performance-builds"
    commits = {side: verify(builds / side) for side in SIDES}
    runners = {(side, scoped): executable(builds / side, scoped, args.method)
               for side in SIDES for scoped in (True, False)}
    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {"commits": commits, "benchmark": benchmark,
                "plan": "Scoped ABBA, then unscoped ABBA; A2 reuses first baseline image",
                "engine": "Original Default job, native NoAcknowledgementConsoleHost without anonymous-pipe parent",
                "limits": "New host/process executions, not historical reconstruction. Image-associated differences do not identify a production defect; absent differences do not prove equivalence. No threshold changes or gate pass claim.",
                "jitDisassembly": args.jit_disassembly,
                "jitMethod": args.jit_method if args.jit_disassembly else None,
                "captureLimits": "Method filtering reduces concurrent listing output; it does not serialize JIT compilation. Missing, interleaved or precompiled methods remain limitations, not evidence of equality.",
                "tieredCompilation": args.tiered_compilation,
                "readyToRun": args.ready_to_run,
                "runtimeControlLimits": "Changing tiering or ReadyToRun changes execution, not production configuration. Cross-condition timing is diagnostic only; host/process order remains a confound.",
                "timingUse": "Excluded: JIT diagnostics perturb execution" if args.jit_disassembly else "New ABBA experiment only",
                "launches": []}
    for scoped in (True, False):
        for side in LAUNCHES:
            image, identity = runners[side, scoped]
            protocol["launches"].append({"side": side, "scoped": scoped,
                "command": ["dotnet", str(image), "--benchmarkName", benchmark,
                            "--job", "Default", "--benchmarkId", identity]})
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    outcomes = []
    for index, launch in enumerate(protocol["launches"], 1):
        outcome = {"launch": index, "side": launch["side"], "scoped": launch["scoped"]}
        log_path = args.output / f"{index}-{launch['side']}-{launch['scoped']}.log"
        environment = os.environ.copy()
        if args.tiered_compilation == "off":
            environment["DOTNET_TieredCompilation"] = "0"
        if args.ready_to_run != "default":
            environment["DOTNET_ReadyToRun"] = "0" if args.ready_to_run == "off" else "1"
        outcome["runtimeEnvironment"] = {key: environment[key] for key in
            ("DOTNET_TieredCompilation", "COMPlus_TieredCompilation",
             "DOTNET_TieredPGO", "COMPlus_TieredPGO", "DOTNET_ReadyToRun", "COMPlus_ReadyToRun")
            if key in environment}
        disassembly = (args.output / f"{index}-{launch['side']}-{launch['scoped']}.asm").resolve()
        if args.jit_disassembly:
            environment.update(DOTNET_JitDisasm=args.jit_method, DOTNET_JitDisasmDiffable="1",
                               DOTNET_JitStdOutFile=str(disassembly))
            outcome["diagnosticEnvironment"] = {key: environment[key] for key in
                ("DOTNET_JitDisasm", "DOTNET_JitDisasmDiffable", "DOTNET_JitStdOutFile")}
        with log_path.open("w") as log:
            try:
                subprocess.run(launch["command"], cwd=Path(launch["command"][1]).parent,
                               stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600,
                               env=environment)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                outcome["error"] = str(error)
                log.write(f"\nLaunch failed: {error}\n")
        if "error" not in outcome:
            try:
                native = result(log_path.read_text())
                if args.jit_disassembly:
                    # Retain raw timing in process logs but never mix diagnostic timings
                    # into outcomes consumed as throughput evidence.
                    if not disassembly.exists() or "; Assembly listing for method " not in disassembly.read_text():
                        raise ValueError("Missing native JIT disassembly")
                    outcome["disassembly"] = disassembly.name
                    outcome["timingUse"] = "Excluded: JIT diagnostics perturb execution"
                else:
                    outcome.update(native)
            except ValueError as error:
                outcome["error"] = str(error)
        outcomes.append(outcome)
        (args.output / "outcomes.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    for side in SIDES:
        verify(builds / side)
    if any("error" in item for item in outcomes):
        raise SystemExit("One or more launches failed; every outcome retained")


if __name__ == "__main__":
    main()
