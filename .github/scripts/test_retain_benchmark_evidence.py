import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

import retain_benchmark_evidence as evidence


class RetainBenchmarkEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.checkout = self.root / "checkout"
        self.checkout.mkdir()
        self.destination = self.root / "evidence"
        self.harness = Path("benchmarks/Relayer.Benchmarks")
        for name in ("global.json", self.harness / "Program.cs", self.harness / "Relayer.Benchmarks.csproj",
                     self.harness / "RegressionGate/Workload.cs"):
            self.write(name, b"original source")
        self.git("init", "--quiet")
        self.git("add", ".")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "fixture")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.checkout), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def write(self, relative, contents):
        path = self.checkout / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def build(self, name):
        path = self.harness / ".bdn" / name
        self.write(path / "gen.csproj", b"generated project")
        self.write(path / "gen.cs", b"generated driver")
        self.write(path / "b/Relayer.dll", b"measured DLL")
        return path

    def test_keeps_all_generated_builds_and_verifies_every_copy(self):
        self.build("first")
        self.build("second")
        self.write(self.harness / ".bdn/first/a/obj/intermediate.dll", b"not loaded")
        self.write(self.harness / ".bdn/first/o/intermediate.dll", b"not loaded")
        self.write(self.harness / "RegressionGate/Workload.cs", b"common harness overlay")
        manifest = evidence.retain(self.checkout, self.destination)
        self.assertEqual(self.git("rev-parse", "HEAD"), manifest["commit"])
        self.assertEqual(manifest, json.loads((self.destination / "inventory-sha256.json").read_text()))
        self.assertEqual(11, len(manifest["files"]))
        self.assertFalse((self.destination / "builds/first/a").exists())
        self.assertFalse((self.destination / "builds/first/o").exists())
        for item in manifest["files"]:
            data = (self.destination / item["path"]).read_bytes()
            self.assertEqual(len(data), item["length"])
            self.assertEqual(hashlib.sha256(data).hexdigest(), item["sha256"])
            if "source" in item:
                self.assertEqual(Path(item["source"]).read_bytes(), data)
        with zipfile.ZipFile(self.destination / "source.zip") as archive:
            self.assertEqual(b"original source", archive.read(str(self.harness / "RegressionGate/Workload.cs").replace("\\", "/")))
        self.assertEqual(b"common harness overlay",
                         (self.destination / "overlay" / self.harness / "RegressionGate/Workload.cs").read_bytes())

    def test_missing_keep_files_fails_instead_of_claiming_binaries_retained(self):
        with self.assertRaisesRegex(ValueError, "use --keepFiles"):
            evidence.retain(self.checkout, self.destination)
        self.assertFalse(self.destination.exists())

    def test_existing_evidence_is_never_overwritten(self):
        self.build("first")
        evidence.retain(self.checkout, self.destination)
        with self.assertRaisesRegex(ValueError, "already exists"):
            evidence.retain(self.checkout, self.destination)

    def test_reused_build_path_keeps_each_execution_independent(self):
        build = self.build("reused")
        evidence.retain(self.checkout, self.destination)
        self.write(build / "b/Relayer.dll", b"rebuilt image")
        repeated = self.root / "repeated"
        evidence.retain(self.checkout, repeated)
        relative = Path("builds/reused/b/Relayer.dll")
        self.assertEqual(b"measured DLL", (self.destination / relative).read_bytes())
        self.assertEqual(b"rebuilt image", (repeated / relative).read_bytes())

    def test_symlink_cannot_copy_files_outside_generated_build(self):
        path = self.build("first")
        try:
            (self.checkout / path / "external.dll").symlink_to(self.checkout / "global.json")
        except OSError:
            self.skipTest("Creating symlinks requires privileges on Windows")
        with self.assertRaisesRegex(ValueError, "symlink"):
            evidence.retain(self.checkout, self.destination)


if __name__ == "__main__":
    unittest.main()
