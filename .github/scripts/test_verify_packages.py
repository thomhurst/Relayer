import io
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from verify_packages import verify_analyzers, verify_packages


class AnalyzerPackageTests(unittest.TestCase):
    def verify(self, *, loose=False, missing=None):
        files = ["Relayer.SourceGenerators.dll", "Relayer.CodeFixes.dll",
                 "System.Text.Json.SourceGeneration.LICENSE.txt",
                 "System.Text.Json.SourceGeneration.NOTICES.txt"]
        if loose:
            files.append("System.Text.Json.SourceGeneration.dll")
        buffer = io.BytesIO()
        with ZipFile(buffer, "w") as archive:
            for name in files:
                if name != missing:
                    archive.writestr("analyzers/dotnet/cs/" + name, b"content")
        with ZipFile(buffer) as archive:
            verify_analyzers(archive)

    def test_embedded_generator_package_layout(self):
        self.verify()

    def test_rejects_loose_generator(self):
        with self.assertRaisesRegex(AssertionError, "must be embedded"):
            self.verify(loose=True)

    def test_requires_analyzers_and_notices(self):
        for name in ("Relayer.SourceGenerators.dll", "Relayer.CodeFixes.dll",
                     "System.Text.Json.SourceGeneration.LICENSE.txt",
                     "System.Text.Json.SourceGeneration.NOTICES.txt"):
            with self.subTest(name=name), self.assertRaises((AssertionError, KeyError)):
                self.verify(missing=name)


class RuntimeAssetTests(unittest.TestCase):
    commit = "a" * 40
    source_url = f"https://raw.githubusercontent.com/thomhurst/Relayer/{commit}/".encode()
    portable_pdb = b"BSJB" + source_url
    targets = ("net10.0", "net11.0")

    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.directory = self.root / "packages"
        self.directory.mkdir()
        self.enterContext(patch("verify_packages.__file__",
                                self.root / ".github/scripts/verify_packages.py"))
        self.make_project("Probe")
        (self.root / "src/Directory.Build.props").write_text(
            "<Project><PropertyGroup><TargetFrameworks>net10.0;net11.0</TargetFrameworks>"
            "</PropertyGroup></Project>", encoding="utf-8")

    def make_project(self, package_id):
        project = self.root / "src" / package_id / f"{package_id}.csproj"
        project.parent.mkdir(parents=True)
        project.write_text("<Project />", encoding="utf-8")

    def write_package(self, package_id, assemblies, pdbs):
        with ZipFile(self.directory / f"{package_id}.nupkg", "w") as archive:
            archive.writestr(f"{package_id}.nuspec", f"""
                <package xmlns="http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd">
                  <metadata>
                    <id>{package_id}</id><version>1.0.0</version>
                    <readme>README.md</readme><icon>icon.png</icon>
                    <repository url="https://github.com/thomhurst/Relayer" commit="{self.commit}" />
                  </metadata>
                </package>""")
            archive.writestr("README.md", b"Package readme")
            archive.writestr("icon.png", b"\x89PNG\r\n\x1a\n")
            for assembly in assemblies:
                archive.writestr(assembly, b"assembly")
        with ZipFile(self.directory / f"{package_id}.snupkg", "w") as symbols:
            for path, content in pdbs.items():
                symbols.writestr(path, content)

    def write_library(self, targets=targets, pdbs=None):
        if pdbs is None:
            pdbs = {f"lib/{framework}/Probe.pdb": self.portable_pdb for framework in self.targets}
        self.write_package("Probe", [f"lib/{framework}/Probe.dll" for framework in targets], pdbs)

    def verify(self):
        with patch("builtins.print"):
            verify_packages(self.directory, self.commit, version="1.0.0")

    def test_accepts_multitarget_library_and_cli_tool(self):
        self.write_library()
        self.make_project("Relayer.Cli")
        assets = [f"tools/{framework}/any/Relayer.Cli" for framework in self.targets]
        self.write_package("Relayer.Cli", [f"{asset}.dll" for asset in assets],
                           {f"{asset}.pdb": self.portable_pdb for asset in assets})
        self.verify()

    def test_requires_runtime_assembly_for_each_target(self):
        for targets, missing in (((), "net10.0"), (("net10.0",), "net11.0"), (("net11.0",), "net10.0")):
            with self.subTest(targets=targets), self.assertRaisesRegex(
                AssertionError, re.escape(f"Probe: missing lib/{missing}/Probe.dll")
            ):
                self.write_library(targets)
                self.verify()

    def test_rejects_target_missing_both_assembly_and_symbols(self):
        self.write_library(("net10.0",), {"lib/net10.0/Probe.pdb": self.portable_pdb})
        with self.assertRaisesRegex(AssertionError, r"Probe: missing lib/net11\.0/Probe\.dll"):
            self.verify()

    def test_requires_symbols_at_each_runtime_target(self):
        self.write_library(pdbs={"lib/net10.0/Probe.pdb": self.portable_pdb,
                                 "ref/net11.0/Probe.pdb": self.portable_pdb})
        with self.assertRaisesRegex(AssertionError, r"Probe: missing lib/net11\.0/Probe\.pdb"):
            self.verify()

    def test_rejects_invalid_symbols_for_later_target(self):
        for content, error in ((self.source_url, "portable PDB"), (b"BSJB", "commit-bound SourceLink"),
                               (b"BSJB" + self.source_url.replace(self.commit.encode(), b"b" * 40),
                                "commit-bound SourceLink")):
            with self.subTest(error=error, content=content):
                self.write_library(pdbs={"lib/net10.0/Probe.pdb": self.portable_pdb,
                                         "lib/net11.0/Probe.pdb": content})
                with self.assertRaisesRegex(AssertionError, rf"Probe: lib/net11\.0/Probe\.pdb.*{error}"):
                    self.verify()


if __name__ == "__main__":
    unittest.main()
