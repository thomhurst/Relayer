#!/usr/bin/env python3
"""Audit historical native GC totals against exported BDN reports, without executing images."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from frozen_bdn_control import BENCHMARK, result


def audit_side(root):
    logs = list(root.glob("*.log"))
    if len(logs) != 1:
        raise ValueError("Expected one original BenchmarkDotNet log per side")
    log = logs[0]
    sections = re.split(r"^// Benchmark: ", log.read_text(), flags=re.M)[1:]
    sections = [section for section in sections
                if section.splitlines()[0] == "EndToEndBenchmarks.ConcurrentRoundTrip: DefaultJob"]
    if len(sections) != 1:
        raise ValueError("Expected one original ConcurrentRoundTrip DefaultJob section")
    native = result(sections[0])
    report = root / "results/Relayer.Benchmarks.EndToEndBenchmarks-report-full.json"
    benchmarks = [entry for entry in json.loads(report.read_text())["Benchmarks"]
                  if entry["FullName"] == BENCHMARK]
    if len(benchmarks) != 1:
        raise ValueError("Expected one exported ConcurrentRoundTrip result")
    memory = benchmarks[0]["Memory"]
    if (memory["TotalOperations"] != native["operations"]
            or memory["BytesAllocatedPerOperation"] != native["bdnRoundedBytesPerMessage"]
            or [memory[f"Gen{generation}Collections"] for generation in range(3)] != native["collections"]):
        raise ValueError("Native GC counters and exported memory report disagree")
    # Bind the result to the exact retained log and report, not just their filenames.
    native["inputs"] = [{"path": path.relative_to(root).as_posix(),
                         "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                        for path in (log, report)]
    return native


def audit(artifact):
    sides = {side: audit_side(artifact / "performance-results" / side)
             for side in ("baseline", "candidate", "baseline-after")}
    candidate = sides["candidate"]["bytesPerMessage"]
    return {"benchmark": BENCHMARK, "sides": sides,
            "candidateDeltaBytesPerMessage": {
                side: candidate - sides[side]["bytesPerMessage"]
                for side in ("baseline", "baseline-after")},
            "limits": "Historical inclusive GC counters, not allocation owner counts. "
                      "Native TotalOperations already includes eight messages per invocation. "
                      "BDN 0.15.8 exports bytes per operation rounded to even. "
                      "No new execution, gate override, historical owner attribution or production fix."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.artifact), indent=2))


if __name__ == "__main__":
    main()
