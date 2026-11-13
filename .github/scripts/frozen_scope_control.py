#!/usr/bin/env python3
"""Predeclared replay of retained scope benchmark images, never a gate retry."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
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
    samples = re.findall(r"^WorkloadResult +\d+: +\d+ op, +[0-9.]+ ns, +([0-9.]+) (ns|us|µs|μs|ms|s)/op\s*$", text, re.M)
    if len(gc) != 1 or len(re.findall(r"^// GC:", text, re.M)) != 1 or not samples or len(re.findall(r"^// AfterAll\s*$", text, re.M)) != 1:
        raise ValueError("Incomplete or ambiguous native results")
    gen0, gen1, gen2, allocated, operations = map(int, gc[0])
    if operations <= 0:
        raise ValueError("Expected positive native operation count")
    scales = {"ns": 1, "us": 1_000, "µs": 1_000, "μs": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
    values = [float(value) * scales[unit] for value, unit in samples]
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
    parser.add_argument("--single-image", choices=("baseline", "candidate"),
                        help="Negative control: execute one original image in every A/B slot")
    plans = parser.add_mutually_exclusive_group()
    plans.add_argument("--affinity-control", action="store_true",
                       help="Scoped default/pinned/pinned/default blocks, balanced image positions")
    plans.add_argument("--counterbalanced", action="store_true",
                        help="Scoped-only fixed ABBA/BAAB/BAAB/ABBA blocks (16 processes)")
    args = parser.parse_args()
    if args.jit_method != "*" and not args.jit_disassembly:
        parser.error("--jit-method requires --jit-disassembly")
    if not args.jit_method.strip():
        parser.error("--jit-method must not be empty")
    allowed_cpus = None
    taskset = None
    if args.affinity_control:
        if args.jit_disassembly or args.tiered_compilation != "default" or args.ready_to_run != "default":
            parser.error("--affinity-control requires uninstrumented default runtime settings")
        taskset = shutil.which("taskset")
        if not hasattr(os, "sched_getaffinity") or taskset is None:
            parser.error("--affinity-control requires Linux sched_getaffinity and native taskset")
        allowed_cpus = sorted(os.sched_getaffinity(0))
        if len(allowed_cpus) < 2:
            parser.error("--affinity-control requires at least two allowed CPUs")
    benchmark = BENCHMARK.rsplit(".", 1)[0] + "." + args.method
    builds = args.artifact.resolve() / "performance-builds"
    commits = {side: verify(builds / side) for side in SIDES}
    runners = {(side, scoped): executable(builds / side, scoped, args.method)
               for side in SIDES for scoped in (True, False)}
    args.output.mkdir(parents=True, exist_ok=False)
    protocol = {"commits": commits, "benchmark": benchmark,
                "plan": ("Scoped default ABBA / pinned BAAB / pinned ABBA / default BAAB; no adaptive stopping"
                         if args.affinity_control else "Scoped-only ABBA/BAAB/BAAB/ABBA; 4 fixed blocks, no adaptive stopping"
                         if args.counterbalanced else "Scoped ABBA, then unscoped ABBA; A2 reuses first baseline image"),
                "analysisPlan": "Compare process means within each block by actual image; retain every block difference and process. Counterbalancing addresses fixed launch-position effects, not arbitrary host drift or historical causality. No gate decision or equivalence claim.",
                "engine": "Original Default job, native NoAcknowledgementConsoleHost without anonymous-pipe parent",
                "limits": "New host/process executions, not historical reconstruction. Image-associated differences do not identify a production defect; absent differences do not prove equivalence. No threshold changes or gate pass claim.",
                "affinityControl": args.affinity_control,
                "allowedCpus": allowed_cpus,
                "pinnedCpu": allowed_cpus[0] if allowed_cpus else None,
                "affinityLimits": "Compare image differences within each block and retain both blocks per condition. Pinning changes placement and available parallelism for the whole process, including runtime threads; it does not isolate migration, frequency, contention or historical cause. Default blocks bracket pinned blocks, but nonlinear host drift remains a confound. No cross-run causal comparison or gate decision.",
                "jitDisassembly": args.jit_disassembly,
                "jitMethod": args.jit_method if args.jit_disassembly else None,
                "captureLimits": "Method filtering reduces concurrent listing output; it does not serialize JIT compilation. Missing, interleaved or precompiled methods remain limitations, not evidence of equality.",
                "tieredCompilation": args.tiered_compilation,
                "singleImage": args.single_image,
                "slotMeaning": "side is the planned A/B slot label; imageSide identifies the executed original image; all A slots reuse the first baseline image",
                "readyToRun": args.ready_to_run,
                "runtimeControlLimits": "Changing tiering or ReadyToRun changes execution, not production configuration. Cross-condition timing is diagnostic only; host/process order remains a confound.",
                "timingUse": "Excluded: JIT diagnostics perturb execution" if args.jit_disassembly else "New fixed-plan experiment only",
                "launches": []}
    # ABBA is a palindrome: swap A/B, rather than reversing it, for BAAB.
    if args.counterbalanced or args.affinity_control:
        baab = tuple("candidate" if side == "baseline" else "baseline" for side in LAUNCHES)
        blocks = ((LAUNCHES, baab, LAUNCHES, baab) if args.affinity_control
                  else (LAUNCHES, baab, baab, LAUNCHES))
    else:
        blocks = (LAUNCHES,)
    for scoped in ((True,) if args.counterbalanced or args.affinity_control else (True, False)):
        for block, sides in enumerate(blocks, 1):
            for position, side in enumerate(sides, 1):
                image_side = args.single_image or side
                image, identity = runners[image_side, scoped]
                affinity = "pinned" if args.affinity_control and block in (2, 3) else "default"
                command = ["dotnet", str(image), "--benchmarkName", benchmark,
                           "--job", "Default", "--benchmarkId", identity]
                if affinity == "pinned":
                    command = [taskset, "--cpu-list", str(allowed_cpus[0]), *command]
                protocol["launches"].append({"affinity": affinity, "cwd": str(image.parent), "side": side, "imageSide": image_side, "scoped": scoped,
                    "block": block, "position": position,
                    "command": command})
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    outcomes = []
    for index, launch in enumerate(protocol["launches"], 1):
        outcome = {"affinity": launch["affinity"], "launch": index, "side": launch["side"], "imageSide": launch["imageSide"], "scoped": launch["scoped"],
                   "block": launch["block"], "position": launch["position"]}
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
                subprocess.run(launch["command"], cwd=Path(launch["cwd"]),
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
