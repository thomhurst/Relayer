#!/usr/bin/env python3
"""Compare BenchmarkDotNet full JSON reports from identical benchmark harnesses."""

import argparse
from dataclasses import dataclass, replace
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys


MAX_THROUGHPUT_LOSS = 0.05
# BDN amortizes allocations across operations; tolerate rounding below one byte.
ALLOCATION_TOLERANCE_BYTES = 0.5
STARTUP_LIMIT_NS = 50_000_000
REQUIRED_METHODS = {
    ("StartupBenchmarks", "Build100Handlers"),
    ("EndToEndBenchmarks", "DirectSend"),
    ("EndToEndBenchmarks", "SynchronousRoundTrip"),
    ("EndToEndBenchmarks", "ConcurrentRoundTrip"),
    ("ConsumePipelineBenchmarks", "ScopeOnly"),
    ("ConsumePipelineBenchmarks", "ScopeAndHandlerResolution"),
    ("ConsumePipelineBenchmarks", "DirectScopedDispatch"),
    ("ConsumePipelineBenchmarks", "ComposedConsumer"),
    ("InMemoryTransportBenchmarks", "NativeOpaqueRoundTrip"),
    ("InMemoryTransportBenchmarks", "NativeRoundTrip"),
    ("InMemoryTransportBenchmarks", "ComposedRoundTrip"),
}

# These files do not enter either measured build. The candidate CI driver and
# detector control both executions. Keep every other file, including unknown
# root inputs, tests, scripts, generators, dependencies and the overlaid harness.
# Unbuilt test C# files have a separate, opt-in MSBuild verification below.
NON_BUILD_FILES = {
    # Agent instruction aliases are symlinks on Linux, placeholders on Windows.
    ".claude/skills/issue-pr-loop/SKILL.md",
    ".claude/skills/issue-pr-loop/references",
    ".agents/skills/issue-pr-loop/references/repository.md",
    "scripts/Assert-PrGreen.ps1",
    "scripts/AssertPrGreenChecks.ps1",
    "scripts/Enable-AutoMerge.ps1",
    "scripts/Test-AssertPrGreenChecks.ps1",
    "scripts/DockerCleanup.ps1",
    "scripts/Remove-MergedWorktrees.ps1",
    "scripts/Test-DockerCleanup.ps1",
    "scripts/Test-RemoveMergedWorktrees.ps1",
    "scripts/WorktreeLifecycle.md",
    ".github/scripts/benchmark_gate.py",
    ".github/scripts/test_benchmark_gate.py",
    ".github/scripts/pin_benchmark_version.py",
    ".github/workflows/ci.yml",
    ".github/workflows/performance.yml",
}
REQUIRED_INPUTS = {
    "global.json", "Directory.Build.props", "Directory.Packages.props",
    "src/Relayer/Relayer.csproj",
    "src/Relayer.SourceGenerators/Relayer.SourceGenerators.csproj",
    "benchmarks/Relayer.Benchmarks/Relayer.Benchmarks.csproj",
}
BENCHMARK_PROJECT = "benchmarks/Relayer.Benchmarks/Relayer.Benchmarks.csproj"
BUILD_ITEMS = ("Compile", "AdditionalFiles", "Analyzer", "EmbeddedResource", "Content", "None",
               "ProjectReference", "ReferencePath", "EditorConfigFiles", "GlobalAnalyzerConfigFiles")


class GateError(ValueError):
    """Missing or unusable input; never turn an incomplete run into a pass."""


def test_source_inputs(checkout):
    """Retain test sources referenced by the Release build, including transitive projects.

    Query after Build so target-added items are visible, not only evaluation-time globs.
    Other test files remain conservatively hashed by input_manifest.
    """
    checkout = Path(checkout).resolve()
    pending = [checkout / BENCHMARK_PROJECT]
    projects = {}
    inputs = set()
    try:
        while pending:
            project = pending.pop().resolve()
            name = project.relative_to(checkout).as_posix()
            if name in projects:
                continue
            result = subprocess.run(
                ["dotnet", "msbuild", str(project), "-target:Build", "-property:Configuration=Release",
                 "-verbosity:quiet", "-nologo", "-getProperty:MSBuildAllProjects",
                 "-getItem:" + ",".join(BUILD_ITEMS)],
                cwd=project.parent, check=True, capture_output=True, text=True, encoding="utf-8", timeout=180)
            data = json.loads(result.stdout)
            items = data["Items"]
            paths = set()
            for kind in BUILD_ITEMS:
                for item in items[kind]:
                    path = Path(item["FullPath"]).resolve()
                    if kind == "ProjectReference":
                        pending.append(path)
                    if path.is_relative_to(checkout):
                        paths.add(path.relative_to(checkout).as_posix())
            for imported in data["Properties"]["MSBuildAllProjects"].split(";"):
                if imported and Path(imported).resolve().is_relative_to(checkout):
                    paths.add(Path(imported).resolve().relative_to(checkout).as_posix())
            projects[name] = sorted(paths)
            inputs.update(paths)
        return {"projects": projects, "test_sources": sorted(
            name for name in inputs if name.startswith("tests/") and name.endswith(".cs"))}
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        raise GateError(f"Cannot verify benchmark test sources under {checkout}: {error}") from error


def input_manifest(checkout, *, referenced_test_sources=None):
    """Hash actual checkout inputs after CI overlays the common harness and SDK."""
    checkout = Path(checkout).resolve()
    try:
        def git(*arguments):
            return subprocess.run(["git", "-C", str(checkout), *arguments], check=True,
                                  capture_output=True, text=True, encoding="utf-8").stdout

        if Path(git("rev-parse", "--show-toplevel").strip()).resolve() != checkout:
            raise GateError(f"Not a checkout root: {checkout}")
        paths = set(git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split("\0"))
        if referenced_test_sources is not None:
            paths.update(referenced_test_sources)
        manifest = {}
        for name in sorted(paths):
            if not name or name in NON_BUILD_FILES or name.startswith(("docs/", "website/")):
                continue
            path = checkout / name
            if path.is_symlink():
                raise GateError(f"Cannot verify symlink input: {path}")
            if path.is_dir():
                raise GateError(f"Cannot verify directory or submodule input: {path}")
            if (referenced_test_sources is not None and name.startswith("tests/")
                    and name.endswith(".cs") and name not in referenced_test_sources):
                continue
            if referenced_test_sources is not None and name in referenced_test_sources and not path.is_file():
                raise GateError(f"Referenced test source missing: {path}")
            # The common harness overlay can delete a baseline's tracked file.
            if path.is_file():
                manifest[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        if not REQUIRED_INPUTS <= manifest.keys():
            raise GateError(f"Required comparison inputs missing under {checkout}")
        return manifest
    except (OSError, subprocess.CalledProcessError) as error:
        raise GateError(f"Cannot verify comparison inputs under {checkout}: {error}") from error


def input_change_summary(baseline, candidate):
    """Expose attribution inputs without changing the regression decision."""
    changes = []
    for name in sorted(baseline.keys() | candidate.keys()):
        if baseline.get(name) == candidate.get(name):
            continue
        if name not in baseline:
            kind = "Added"
        elif name not in candidate:
            kind = "Removed"
        else:
            kind = "Changed"
        # Keep unusual filenames inside the code span in the Markdown summary.
        display = name.replace("`", "&#96;").replace("\n", "&#10;").replace("\r", "&#13;")
        changes.append(f"- {kind}: `{display}`")
    if not changes:
        return ""
    return "\n".join([
        "", "Comparison inputs differ; strict regression thresholds remain active.",
        "Unchanged benchmark or call-path source does not establish an unchanged containing assembly, generated code or execution.",
        "These paths guide investigation; they do not attribute a measured difference to a code change.",
        "", *changes,
    ])


@dataclass(frozen=True)
class Measurement:
    mean_ns: float
    lower_ns: float
    upper_ns: float
    allocated_bytes: float
    type_name: str
    method: str


def number(value, field, case, *, positive=False, allow_negative=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{case}: missing or nonnumeric {field}")
    if not math.isfinite(value) or (value < 0 and not allow_negative) or (positive and value == 0):
        raise GateError(f"{case}: invalid {field}: {value}")
    return float(value)


def load_reports(directory):
    directory = Path(directory)
    files = sorted(directory.rglob("*-report-full.json"))
    if not files:
        raise GateError(f"No BenchmarkDotNet full JSON reports under {directory}")
    cases = {}
    for path in files:
        try:
            report = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as error:
            raise GateError(f"Cannot read {path}: {error}") from error
        benchmarks = report.get("Benchmarks") if isinstance(report, dict) else None
        if not isinstance(benchmarks, list) or not benchmarks:
            raise GateError(f"{path}: no benchmark measurements")
        for benchmark in benchmarks:
            if not isinstance(benchmark, dict):
                raise GateError(f"{path}: invalid benchmark record")
            identity = benchmark.get("FullName")
            type_name = benchmark.get("Type")
            method = benchmark.get("Method")
            if not all(isinstance(item, str) and item for item in (identity, type_name, method)):
                raise GateError(f"{path}: missing FullName, Type, or Method")
            if identity in cases:
                raise GateError(f"Duplicate benchmark case: {identity}")
            statistics = benchmark.get("Statistics") or {}
            memory = benchmark.get("Memory") or {}
            if not isinstance(statistics, dict) or not isinstance(memory, dict):
                raise GateError(f"{identity}: invalid Statistics or Memory")
            mean = number(statistics.get("Mean"), "Mean", identity, positive=True)
            interval = statistics.get("ConfidenceInterval")
            if not isinstance(interval, dict):
                raise GateError(f"{identity}: missing ConfidenceInterval")
            lower = number(interval.get("Lower"), "ConfidenceInterval.Lower", identity, allow_negative=True)
            upper = number(interval.get("Upper"), "ConfidenceInterval.Upper", identity, positive=True)
            if not lower <= mean <= upper:
                raise GateError(f"{identity}: confidence bounds do not enclose Mean")
            cases[identity] = Measurement(
                mean, lower, upper,
                number(memory.get("BytesAllocatedPerOperation"), "BytesAllocatedPerOperation", identity),
                type_name.rsplit(".", 1)[-1], method,
            )
    missing = REQUIRED_METHODS - {(case.type_name, case.method) for case in cases.values()}
    if missing:
        raise GateError("Required benchmark methods missing: " + ", ".join(f"{t}.{m}" for t, m in sorted(missing)))
    return cases


def compare(baseline, candidate, *, baseline_after=None, enforce_startup_budget=True, identical_inputs=False):
    if baseline.keys() != candidate.keys():
        missing = sorted(baseline.keys() - candidate.keys())
        added = sorted(candidate.keys() - baseline.keys())
        raise GateError(f"Benchmark case sets differ. Missing: {missing}; added: {added}")
    if baseline_after is not None and baseline_after.keys() != baseline.keys():
        raise GateError("Repeated baseline case sets differ")
    failures = []
    lines = [
        "| Benchmark | Throughput change vs first baseline | First baseline mean CI (ns) | Repeated baseline mean CI (ns) | Candidate mean CI (ns) | First / repeated baseline B/op | Candidate B/op | Result |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for identity in sorted(baseline):
        before, after = baseline[identity], candidate[identity]
        repeated = before if baseline_after is None else baseline_after[identity]
        if (before.type_name, before.method) != (after.type_name, after.method):
            raise GateError(f"{identity}: benchmark metadata differs")
        if (before.type_name, before.method) != (repeated.type_name, repeated.method):
            raise GateError(f"{identity}: repeated baseline metadata differs")
        throughput = before.mean_ns / after.mean_ns - 1
        # A fixed baseline-candidate-baseline sequence exposes drift between processes.
        # Confirm regressions against both references; report baseline drift explicitly.
        upper = max(before.upper_ns, repeated.upper_ns)
        allocated = max(before.allocated_bytes, repeated.allocated_bytes)
        throughput_failure = max(0, after.lower_ns) > upper / (1 - MAX_THROUGHPUT_LOSS)
        startup = (after.type_name, after.method) == ("StartupBenchmarks", "Build100Handlers")
        reasons = []
        if throughput_failure and not identical_inputs:
            reasons.append("throughput loss exceeds 5%")
        allocation_increase = not startup and after.allocated_bytes > allocated + ALLOCATION_TOLERANCE_BYTES
        if allocation_increase and not identical_inputs:
            reasons.append("allocations increased by more than 0.5 B/op")
        if enforce_startup_budget and startup and after.mean_ns >= STARTUP_LIMIT_NS:
            reasons.append("100-handler startup is not under 50 ms")
        inconclusive = after.mean_ns > min(before.mean_ns, repeated.mean_ns) / (1 - MAX_THROUGHPUT_LOSS) and not throughput_failure
        statuses = list(reasons)
        if identical_inputs and (throughput_failure or inconclusive):
            statuses.append("INCONCLUSIVE: throughput variation with byte-identical comparison inputs")
        elif inconclusive:
            statuses.append("INCONCLUSIVE: observed throughput loss exceeds 5%; confidence bounds or baseline drift prevent confirmation")
        allocation_inconclusive = (not startup
                                  and after.allocated_bytes > min(before.allocated_bytes, repeated.allocated_bytes) + ALLOCATION_TOLERANCE_BYTES
                                  and after.allocated_bytes <= allocated + ALLOCATION_TOLERANCE_BYTES)
        if identical_inputs and (allocation_increase or allocation_inconclusive):
            statuses.append("INCONCLUSIVE: allocation variation with byte-identical comparison inputs")
        elif allocation_inconclusive:
            statuses.append("INCONCLUSIVE: observed allocation increase exceeds 0.5 B/op; baseline drift prevents confirmation")
        if startup:
            statuses.append("startup allocations are informational")
        status = "; ".join(statuses) if statuses else "PASS"
        repeat_interval = "not measured" if baseline_after is None else f"[{repeated.lower_ns:g}, {repeated.upper_ns:g}]"
        repeat_allocated = "not measured" if baseline_after is None else f"{repeated.allocated_bytes:g}"
        lines.append(f"| {identity} | {throughput:+.2%} | [{before.lower_ns:g}, {before.upper_ns:g}] | {repeat_interval} | [{after.lower_ns:g}, {after.upper_ns:g}] | {before.allocated_bytes:g} / {repeat_allocated} | {after.allocated_bytes:g} | {status} |")
        failures.extend(f"{identity}: {reason}" for reason in reasons)
    lines.extend([
        "",
        "This gate measures regressions against main, not proof of the raw-client 5% budget.",
        "Throughput fails only when the candidate lower confidence bound exceeds each baseline upper bound / 0.95, unless comparison inputs are verified byte-identical. Overlapping bounds or drift with an observed loss are inconclusive, not proof of unchanged performance.",
        "Per-message allocations fail only when the candidate exceeds both baselines by more than 0.5 B/op, unless comparison inputs are verified byte-identical. An increase against only the lower baseline is inconclusive, not proof of unchanged allocations. Startup allocations are informational and the candidate startup budget remains under 50 ms.",
        "Total receive allocations include fresh DI scopes. Equal scoped baselines establish only zero incremental composition allocations.",
        "D18 still requires zero pipeline allocations, excluding serializer/native-client allocations; existing positive totals remain visible above.",
    ])
    if identical_inputs:
        lines.append("Verified byte-identical comparison inputs: throughput and allocation differences are inconclusive execution variation, not evidence of a source-change regression. Timings, bounds and totals remain visible; startup and input validation still apply. This does not prove unchanged performance or compliance with D18.")
    return failures, "\n".join(lines)


def self_test(cases):
    # The baseline can exceed the startup budget that the real candidate must meet.
    failures, _ = compare(cases, cases, baseline_after=cases, enforce_startup_budget=False)
    if failures:
        raise GateError("Detector self-test rejected identical measurements: " + "; ".join(failures))
    identity = next(identity for identity, case in cases.items()
                    if (case.type_name, case.method) != ("StartupBenchmarks", "Build100Handlers"))
    slow = dict(cases)
    measurement = cases[identity]
    # Translate the whole interval to prove detection even when the original run is noisy.
    shift = measurement.upper_ns / (1 - MAX_THROUGHPUT_LOSS) * 2 - measurement.lower_ns
    slow[identity] = replace(measurement, mean_ns=measurement.mean_ns + shift,
                             lower_ns=measurement.lower_ns + shift, upper_ns=measurement.upper_ns + shift)
    failures, _ = compare(cases, slow, baseline_after=cases, enforce_startup_budget=False)
    if not any("throughput loss" in failure for failure in failures):
        raise GateError("Detector self-test failed to reject injected throughput regression")
    allocated = dict(cases)
    allocated[identity] = replace(cases[identity], allocated_bytes=cases[identity].allocated_bytes + 64)
    failures, _ = compare(cases, allocated, baseline_after=cases, enforce_startup_budget=False)
    if not any("allocations increased" in failure for failure in failures):
        raise GateError("Detector self-test failed to reject injected allocation regression")
    print("PASS: identical measurements; REJECTED: injected throughput loss with separated confidence bounds and +64 B/op allocation regression")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("baseline")
    compare_parser.add_argument("candidate")
    compare_parser.add_argument("--baseline-after", type=Path, help="Fixed baseline repeat measured after the candidate")
    compare_parser.add_argument("--summary", type=Path)
    compare_parser.add_argument("--baseline-source", type=Path)
    compare_parser.add_argument("--candidate-source", type=Path)
    compare_parser.add_argument("--input-manifest", type=Path, help="Retain SHA-256 inventories of both source checkouts")
    compare_parser.add_argument("--verify-test-sources", action="store_true",
                                help="Build and inspect the Release project graph before excluding unbuilt test C# files")
    test_parser = commands.add_parser("self-test")
    test_parser.add_argument("baseline")
    args = parser.parse_args(argv)
    try:
        baseline = load_reports(args.baseline)
        if args.command == "self-test":
            self_test(baseline)
            return 0
        baseline_after = None if args.baseline_after is None else load_reports(args.baseline_after)
        identical_inputs = False
        source_arguments = (args.baseline_source, args.candidate_source, args.input_manifest)
        if args.verify_test_sources and not all(source_arguments):
            raise GateError("Test source verification requires both source checkouts and --input-manifest")
        if any(source_arguments):
            if not all(source_arguments):
                raise GateError("Source verification requires --baseline-source, --candidate-source and --input-manifest together")
            if args.baseline_source.resolve() == args.candidate_source.resolve():
                raise GateError("Source verification requires separate checkouts")
            verification, manifest = {}, {}
            for side, checkout in (("baseline", args.baseline_source), ("candidate", args.candidate_source)):
                referenced = None
                if args.verify_test_sources:
                    verification[side] = test_source_inputs(checkout)
                    referenced = verification[side]["test_sources"]
                manifest[side] = input_manifest(checkout, referenced_test_sources=referenced)
            identical_inputs = manifest["baseline"] == manifest["candidate"]
            manifest["identical"] = identical_inputs
            if verification:
                manifest["test_source_verification"] = verification
            args.input_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        failures, summary = compare(baseline, load_reports(args.candidate), baseline_after=baseline_after,
                                    identical_inputs=identical_inputs)
        if any(source_arguments):
            summary += input_change_summary(manifest["baseline"], manifest["candidate"])
        print(summary)
        if args.summary:
            args.summary.write_text(summary + "\n", encoding="utf-8")
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1 if failures else 0
    except (GateError, OSError) as error:
        print(f"Benchmark gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
