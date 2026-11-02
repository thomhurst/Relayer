"""Inspect the actual release artifacts, not just the MSBuild configuration."""

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET
from zipfile import ZipFile


def verify_packages(directory, commit, version=None):
    root = Path(__file__).resolve().parents[2]
    expected = {project.stem for project in (root / "src").glob("*/*.csproj")
                if ET.parse(project).findtext(".//IsPackable") != "false"}
    found = set()
    versions = set()
    for package in sorted(directory.glob("*.nupkg")):
        with ZipFile(package) as archive:
            # NuGet uses different schema versions depending on package features.
            metadata = ET.fromstring(archive.read(next(n for n in archive.namelist() if n.endswith(".nuspec"))))
            namespace = {"n": metadata.tag[1:].split("}")[0]}
            metadata = metadata.find("n:metadata", namespace)
            package_id = metadata.findtext("n:id", namespaces=namespace)
            package_version = metadata.findtext("n:version", namespaces=namespace)
            assert package_id not in found, f"Duplicate package: {package_id}"
            found.add(package_id)
            versions.add(package_version)
            if version:
                assert package_version == version, f"{package_id}: expected {version}, got {package_version}"
            for element in ("readme", "icon"):
                path = metadata.findtext(f"n:{element}", namespaces=namespace)
                assert path and archive.read(path), f"{package_id}: missing {element}"
            assert archive.read(metadata.findtext("n:icon", namespaces=namespace)).startswith(b"\x89PNG\r\n\x1a\n")
            repository = metadata.find("n:repository", namespace)
            assert repository is not None and repository.get("commit") == commit, f"{package_id}: wrong repository commit"
            assert repository.get("url") == "https://github.com/thomhurst/Relayer"
            for dependency in metadata.findall(".//n:dependency", namespace):
                assert dependency.get("id") != "MinVer", f"{package_id}: build dependency leaked"
                if dependency.get("id") in expected:
                    assert dependency.get("version") == package_version, f"{package_id}: inconsistent internal dependency"
            if package_id == "Relayer":
                for assembly in ("Relayer.SourceGenerators.dll", "Relayer.CodeFixes.dll", "System.Text.Json.SourceGeneration.dll"):
                    assert f"analyzers/dotnet/cs/{assembly}" in archive.namelist(), f"Missing bundled {assembly}"
        with ZipFile(package.with_suffix(".snupkg")) as symbols:
            pdb = next(name for name in symbols.namelist() if name.endswith(f"/{package_id}.pdb"))
            content = symbols.read(pdb)
            assert content.startswith(b"BSJB"), f"{package_id}: symbols must be portable PDBs"
            source_url = f"https://raw.githubusercontent.com/thomhurst/Relayer/{commit}/".encode()
            assert source_url in content, f"{package_id}: missing commit-bound SourceLink"
        print(f"Verified {package_id} {package_version}")
    assert found == expected, f"Package inventory mismatch: missing={expected - found}, unexpected={found - expected}"
    assert len(versions) == 1, f"Package versions differ: {versions}"
    expected_symbols = {path.with_suffix(".snupkg") for path in directory.glob("*.nupkg")}
    assert set(directory.glob("*.snupkg")) == expected_symbols, "Symbol package inventory mismatch"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--version")
    arguments = parser.parse_args()
    verify_packages(arguments.directory, arguments.commit, arguments.version)
