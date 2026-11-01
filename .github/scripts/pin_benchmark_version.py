#!/usr/bin/env python3
"""Pin only the baseline benchmark runner to the candidate's BenchmarkDotNet version."""

import argparse
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


PROJECT = Path("benchmarks/Relayer.Benchmarks/Relayer.Benchmarks.csproj")


def package_element(root, tag):
    elements = [element for element in root.iter()
                if element.tag == tag and element.get("Include") == "BenchmarkDotNet"]
    if len(elements) != 1:
        raise ValueError(f"Expected exactly one BenchmarkDotNet {tag}")
    return elements[0]


def pin_version(candidate, baseline):
    candidate, baseline = Path(candidate).resolve(), Path(baseline).resolve()
    if candidate == baseline:
        raise ValueError("Candidate and baseline must be separate checkouts")
    candidate_reference = package_element(ET.parse(candidate / PROJECT).getroot(), "PackageReference")
    version = candidate_reference.get("VersionOverride")
    if not version:
        central = ET.parse(candidate / "Directory.Packages.props").getroot()
        version = package_element(central, "PackageVersion").get("Version")
    if not version or any(expression in version for expression in ("$(", "@(", "%(")):
        raise ValueError("Candidate BenchmarkDotNet version must be a literal package version")
    # Do not replace central versions: production/native dependencies must remain those of main.
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    tree = ET.parse(baseline / PROJECT, parser=parser)
    reference = package_element(tree.getroot(), "PackageReference")
    reference.set("VersionOverride", version)
    tree.write(baseline / PROJECT, encoding="utf-8", xml_declaration=False)
    return version


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate")
    parser.add_argument("baseline")
    args = parser.parse_args(argv)
    try:
        print(f"BenchmarkDotNet: {pin_version(args.candidate, args.baseline)}")
        return 0
    except (OSError, ValueError, ET.ParseError) as error:
        print(f"Cannot pin benchmark runner: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
