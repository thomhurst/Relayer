#!/usr/bin/env python3
"""Matched core-DLL substitution in retained native BDN engines; never a gate."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from frozen_bdn_control import BENCHMARK, executable, result
from frozen_concurrent_control import verify


def inventory(root):
    return {path.relative_to(root).as_posix(): {
        "length": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in sorted(root.rglob("*")) if path.is_file()}


def prepare(builds, output):
    # Verify all original inputs before copying. Never modify the frozen archives.
    commits = {side: verify(builds / side) for side in ("baseline", "candidate", "baseline-after")}
    runners = {side: executable(builds / side) for side in ("baseline", "candidate")}
    prepared = {}
    for engine, (runner, identity) in runners.items():
        other = "candidate" if engine == "baseline" else "baseline"
        for core in (engine, other):
            name = f"{engine}-engine-{core}-core"
            destination = output / name
            shutil.copytree(runner.parent, destination)
            original = inventory(destination)
            donor = runners[core][0].parent / "Relayer.dll"
            if not donor.is_file() or not (destination / "Relayer.dll").is_file():
                raise ValueError("Both frozen images must contain Relayer.dll")
            shutil.copyfile(donor, destination / "Relayer.dll")
            actual = inventory(destination)
            changed = [path for path in original.keys() | actual.keys()
                       if original.get(path) != actual.get(path)]
            expected = [] if core == engine else ["Relayer.dll"]
            if sorted(changed) != expected:
                raise ValueError(f"Expected only {expected} to change, observed {changed}")
            prepared[name] = {"directory": destination, "runner": runner.name,
                              "identity": identity, "inventory": actual}
    return commits, prepared


def run(artifact, output):
    output.mkdir(parents=True, exist_ok=False)
    commits, prepared = prepare(artifact / "performance-builds", output / "images")
    # Reciprocal, separately bracketed controls. No comparison of independent totals.
    launches = []
    for engine in ("baseline", "candidate"):
        other = "candidate" if engine == "baseline" else "baseline"
        launches.extend(f"{engine}-engine-{core}-core" for core in (engine, other, other, engine))
    protocol = {"commits": commits, "launches": launches,
                "images": {name: item["inventory"] for name, item in prepared.items()},
                "limits": "New executions with exactly one on-disk core DLL substitution in each fixed engine image. "
                "No rebuild, PDB substitution, profiler, job change, threshold change or historical attribution. "
                "A DLL swap includes metadata and IL; it is not an identity-only intervention or proof of loaded-byte equivalence. "
                "On-disk hashes do not identify loaded runtime/JIT bytes. PDBs remain original and no symbolic analysis is performed. "
                "Reciprocal controls may narrow an image effect; they cannot identify exact historical object owners."}
    (output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    outcomes = []
    failed = False
    for index, name in enumerate(launches, 1):
        image = prepared[name]
        if inventory(image["directory"]) != image["inventory"]:
            raise ValueError("Prepared image changed before execution")
        command = ["dotnet", str((image["directory"] / image["runner"]).resolve()),
                   "--benchmarkName", BENCHMARK, "--job", "Default", "--benchmarkId", image["identity"]]
        outcome = {"launch": index, "image": name, "command": command}
        with (output / f"{index}-{name}.log").open("w") as log:
            try:
                subprocess.run(command, cwd=image["directory"], stdout=log,
                               stderr=subprocess.STDOUT, check=True, timeout=900)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
                outcome["error"] = str(error)
                log.write(f"\nLaunch failed: {error}\n")
        if "error" not in outcome:
            try:
                outcome.update(result((output / f"{index}-{name}.log").read_text()))
            except ValueError as error:
                outcome["error"] = str(error)
        if inventory(image["directory"]) != image["inventory"]:
            outcome["error"] = "Prepared image changed during execution"
        failed |= "error" in outcome
        outcomes.append(outcome)
        (output / "outcomes.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    for side in ("baseline", "candidate", "baseline-after"):
        verify(artifact / "performance-builds" / side)
    if failed:
        raise SystemExit("Crossover launches failed; all outcomes retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    run(args.artifact, args.output)


if __name__ == "__main__":
    main()
