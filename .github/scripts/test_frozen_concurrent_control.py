import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

from frozen_concurrent_control import main, verify


class FrozenInventoryTests(unittest.TestCase):
    def test_verifies_content_and_rejects_same_length_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = root / "image.dll"
            payload.write_bytes(b"original")
            manifest = {"commit": "frozen", "files": [{"path": "image.dll", "length": 8,
                        "sha256": hashlib.sha256(payload.read_bytes()).hexdigest()}]}
            (root / "inventory-sha256.json").write_text(json.dumps(manifest))
            self.assertEqual(verify(root), "frozen")
            payload.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "Retained input changed"):
                verify(root)

    def test_rejects_parent_path_before_reading_outside_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inventory-sha256.json").write_text(json.dumps({"files": [
                {"path": "../outside", "length": 0, "sha256": ""}]}))
            with self.assertRaisesRegex(ValueError, "escapes archive"):
                verify(root)

    def test_missing_executable_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inventory-sha256.json").write_text(json.dumps({"files": [
                {"path": "missing.dll", "length": 0, "sha256": ""}]}))
            with self.assertRaises(FileNotFoundError):
                verify(root)


class ProfileLaunchTests(unittest.TestCase):
    def run_profile(self, extraction_fails):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            commands = []

            def execute(command, **kwargs):
                commands.append(command)
                if "collect" in command:
                    trace = Path(command[command.index("--output") + 1])
                    trace.write_bytes(b"trace")
                    Path(str(trace) + ".etlx").write_bytes(b"index")
                elif extraction_fails:
                    raise subprocess.CalledProcessError(1, command)
                else:
                    kwargs["stdout"].write('{"kind":"measurement"}\n')

            arguments = ["control", str(root), str(root / "probe.dll"), str(output),
                         "--trace-tool", str(root / "dotnet-trace"), "--profile-reader", str(root / "reader.dll")]
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run", side_effect=execute):
                if extraction_fails:
                    with self.assertRaisesRegex(SystemExit, "One or more launches failed"):
                        main()
                else:
                    main()
            self.assertEqual(len(commands), 8)
            self.assertEqual(len(list(output.glob("*.nettrace"))), 4 if extraction_fails else 0)
            self.assertEqual(len(list(output.glob("*.etlx"))), 4 if extraction_fails else 0)
            self.assertEqual(len(list(output.glob("*-allocations.jsonl"))), 4)
            self.assertTrue(json.loads((output / "protocol.json").read_text())["instrumented"])
            for command in commands[::2]:
                self.assertIn("Relayer-FrozenMeasurement", command)
                self.assertIn("gc-verbose", command)

    def test_profile_extracts_all_launches_and_removes_successful_raw_captures(self):
        self.run_profile(False)

    def test_profile_preserves_raw_capture_and_continues_after_extraction_failure(self):
        self.run_profile(True)


if __name__ == "__main__":
    unittest.main()
