#!/usr/bin/env python3
"""Compare BenchmarkDotNet full JSON reports from identical benchmark harnesses."""

import argparse
from dataclasses import dataclass, replace
import json
import math
from pathlib import Path
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


class GateError(ValueError):
    """Missing or unusable input; never turn an incomplete run into a pass."""


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


def compare(baseline, candidate, *, baseline_after=None, enforce_startup_budget=True):
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
        # Throughput must regress against both references; allocations must satisfy both.
        upper = max(before.upper_ns, repeated.upper_ns)
        allocated = min(before.allocated_bytes, repeated.allocated_bytes)
        throughput_failure = max(0, after.lower_ns) > upper / (1 - MAX_THROUGHPUT_LOSS)
        startup = (after.type_name, after.method) == ("StartupBenchmarks", "Build100Handlers")
        reasons = []
        if throughput_failure:
            reasons.append("throughput loss exceeds 5%")
        if not startup and after.allocated_bytes > allocated + ALLOCATION_TOLERANCE_BYTES:
            reasons.append("allocations increased by more than 0.5 B/op")
        if enforce_startup_budget and startup and after.mean_ns >= STARTUP_LIMIT_NS:
            reasons.append("100-handler startup is not under 50 ms")
        inconclusive = after.mean_ns > min(before.mean_ns, repeated.mean_ns) / (1 - MAX_THROUGHPUT_LOSS) and not throughput_failure
        statuses = reasons + (["INCONCLUSIVE: observed throughput loss exceeds 5%; confidence bounds or baseline drift prevent confirmation"] if inconclusive else [])
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
        "Throughput fails only when the candidate lower confidence bound exceeds each baseline upper bound / 0.95. Overlapping bounds or drift with an observed loss are inconclusive, not proof of unchanged performance.",
        "Per-message allocations must stay within 0.5 B/op of both baselines; startup allocations are informational and the candidate startup budget remains under 50 ms.",
        "Total receive allocations include fresh DI scopes. Equal scoped baselines establish only zero incremental composition allocations.",
        "D18 still requires zero pipeline allocations, excluding serializer/native-client allocations; existing positive totals remain visible above.",
    ])
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
    test_parser = commands.add_parser("self-test")
    test_parser.add_argument("baseline")
    args = parser.parse_args(argv)
    try:
        baseline = load_reports(args.baseline)
        if args.command == "self-test":
            self_test(baseline)
            return 0
        baseline_after = None if args.baseline_after is None else load_reports(args.baseline_after)
        failures, summary = compare(baseline, load_reports(args.candidate), baseline_after=baseline_after)
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
