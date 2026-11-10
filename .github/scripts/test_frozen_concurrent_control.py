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
    def run_profile(self, extraction_fails, keep_traces=False):
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
                    kwargs["stdout"].write('{"kind":"measurement","samplesWithoutStacks":1}\n')
                    kwargs["stderr"].write("Missing measured allocation stack\n")
                    raise subprocess.CalledProcessError(1, command)
                else:
                    kwargs["stdout"].write('{"kind":"measurement"}\n')

            arguments = ["control", str(root), str(root / "probe.dll"), str(output),
                         "--trace-tool", str(root / "dotnet-trace"), "--profile-reader", str(root / "reader.dll")]
            if keep_traces:
                arguments.append("--keep-traces")
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run", side_effect=execute):
                if extraction_fails:
                    with self.assertRaisesRegex(SystemExit, "One or more launches failed"):
                        main()
                else:
                    main()
            self.assertEqual(len(commands), 8)
            self.assertEqual(len(list(output.glob("*.nettrace"))), 4 if extraction_fails or keep_traces else 0)
            self.assertEqual(len(list(output.glob("*.etlx"))), 4 if extraction_fails else 0)
            self.assertEqual(len(list(output.glob("*-allocations.jsonl"))), 4)
            for profile in output.glob("*-allocations.jsonl"):
                records = [json.loads(line) for line in profile.read_text().splitlines()]
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["kind"], "measurement")
            if extraction_fails:
                for diagnostics in output.glob("*-extraction.log"):
                    self.assertIn("Missing measured allocation stack", diagnostics.read_text())
                    self.assertIn("Profile extraction failed", diagnostics.read_text())
            self.assertTrue(json.loads((output / "protocol.json").read_text())["instrumented"])
            self.assertEqual(json.loads((output / "protocol.json").read_text())["raw_traces_retained"], keep_traces)
            for command in commands[::2]:
                self.assertIn("Relayer-FrozenMeasurement", command)
                self.assertIn("gc-verbose", command)

    def test_retains_raw_traces_for_reanalysis_after_successful_extraction(self):
        self.run_profile(extraction_fails=False, keep_traces=True)

    def test_keep_traces_requires_a_profile_before_creating_output(self):
        with patch("sys.argv", ["control", "inputs", "probe", "output", "--keep-traces"]), self.assertRaises(SystemExit), patch("frozen_concurrent_control.subprocess.run") as execute:
            main()
        execute.assert_not_called()

    def test_exact_invocations_use_separate_processes_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            arguments = ["control", str(root), str(root / "probe.dll"), str(output), "--exact-invocations"]
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            for call in execute.call_args_list:
                self.assertEqual(call.args[0][-1], "--exact-invocations")
                self.assertNotIn("collect", call.args[0])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_invocations"])

    def test_exact_driver_rejects_other_regions_and_profiles(self):
        for extra in (["--exact-invocations"], ["--exact-releases"], ["--exact-roundtrips"],
                      ["--trace-tool", "trace", "--profile-reader", "reader"]):
            with self.subTest(extra=extra), patch("sys.argv", ["control", "inputs", "probe", "output", "--exact-driver", *extra]), self.assertRaises(SystemExit):
                main()

    def test_exact_driver_uses_separate_processes_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            with patch("sys.argv", ["control", str(root), "probe.dll", str(output), "--exact-driver"]), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            for call in execute.call_args_list:
                self.assertEqual(call.args[0][-1], "--exact-driver")
                self.assertNotIn("collect", call.args[0])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_driver"])
            self.assertEqual(protocol["completion_boundary"], "original-BenchmarkDotNet-AwaitHelper.GetResult")
            self.assertIn("other threads", protocol["driver_boundary"])

    def test_bdn_waiter_uses_separate_processes_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            arguments = ["control", str(root), str(root / "probe.dll"), str(output), "--bdn-await"]
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            for call in execute.call_args_list:
                self.assertEqual(call.args[0][-1], "--bdn-await")
                self.assertNotIn("collect", call.args[0])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertFalse(protocol["instrumented"])
            self.assertEqual(protocol["completion_boundary"], "original-BenchmarkDotNet-AwaitHelper.GetResult")

    def test_exact_releases_use_separate_processes_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            arguments = ["control", str(root), str(root / "probe.dll"), str(output), "--exact-releases"]
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            for call in execute.call_args_list:
                self.assertEqual(call.args[0][-1], "--exact-releases")
                self.assertNotIn("collect", call.args[0])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_releases"])

    def test_exact_roundtrips_use_separate_processes_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            arguments = ["control", str(root), str(root / "probe.dll"), str(output), "--exact-roundtrips"]
            with patch("sys.argv", arguments), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            for call in execute.call_args_list:
                self.assertEqual(call.args[0][-1], "--exact-roundtrips")
                self.assertNotIn("collect", call.args[0])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_roundtrips"])

    def test_bdn_waiter_rejects_fixture_counters(self):
        for counter in ("--exact-invocations", "--exact-releases", "--exact-roundtrips", "--exact-driver"):
            with self.subTest(counter=counter), patch("sys.argv", ["control", "inputs", "probe.dll", "output", "--bdn-await", counter]), self.assertRaises(SystemExit):
                main()

    def test_release_counters_reject_combined_sampled_profiles(self):
        arguments = ["control", "inputs", "probe.dll", "output", "--exact-releases",
                     "--trace-tool", "dotnet-trace", "--profile-reader", "reader.dll"]
        with patch("sys.argv", arguments), self.assertRaises(SystemExit):
            main()

    def test_exact_counters_reject_combined_sampled_profiles(self):
        arguments = ["control", "inputs", "probe.dll", "output", "--exact-invocations",
                     "--trace-tool", "dotnet-trace", "--profile-reader", "reader.dll"]
        with patch("sys.argv", arguments), self.assertRaises(SystemExit):
            main()

    def test_roundtrip_counters_reject_combined_sampled_profiles(self):
        arguments = ["control", "inputs", "probe.dll", "output", "--exact-roundtrips",
                     "--trace-tool", "dotnet-trace", "--profile-reader", "reader.dll"]
        with patch("sys.argv", arguments), self.assertRaises(SystemExit):
            main()

    def test_rejects_combining_exact_regions(self):
        arguments = ["control", "inputs", "probe.dll", "output", "--exact-invocations", "--exact-releases"]
        with patch("sys.argv", arguments), self.assertRaises(SystemExit):
            main()

    def test_profile_extracts_all_launches_and_removes_successful_raw_captures(self):
        self.run_profile(False)

    def test_profile_preserves_raw_capture_and_continues_after_extraction_failure(self):
        self.run_profile(True)


if __name__ == "__main__":
    unittest.main()

class SourceLaunchTests(unittest.TestCase):
    def test_source_control_rejects_mixed_regions_and_profiles_before_launching(self):
        for options in (("--exact-invocations",), ("--exact-releases",), ("--exact-driver",),
                        ("--exact-roundtrips",), ("--exact-worker",), ("--bdn-await",),
                        ("--trace-tool", "trace", "--profile-reader", "reader")):
            with self.subTest(options=options), patch("sys.argv", ["control", "inputs", "probe", "output", "--exact-sources", *options]), patch("frozen_concurrent_control.subprocess.run") as execute:
                with self.assertRaises(SystemExit):
                    main()
                execute.assert_not_called()

    def test_source_flag_protocol_and_all_original_image_sides_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            with patch("sys.argv", ["control", str(root), "probe", str(output), "--exact-sources"]), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            sides = []
            for call in execute.call_args_list:
                command = call.args[0]
                self.assertEqual(command[-1], "--exact-sources")
                sides.append(Path(command[-2]).relative_to((root / "performance-builds").resolve()).parts[0])
            self.assertEqual(sides, ["baseline", "candidate", "candidate", "baseline"])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_sources"])
            self.assertIn("First observation is not proof", protocol["source_boundary"])


class WorkerLaunchTests(unittest.TestCase):
    def test_worker_protocol_uses_original_waiter_and_keeps_both_candidate_launches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate", "baseline-after"):
                image = root / "performance-builds" / side / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0/Relayer.Benchmarks.dll"
                image.parent.mkdir(parents=True)
                image.touch()
            output = root / "results"
            with patch("sys.argv", ["control", str(root), "probe", str(output), "--exact-worker"]), patch("frozen_concurrent_control.verify", return_value="frozen"), patch("frozen_concurrent_control.subprocess.run") as execute:
                main()
            self.assertEqual(execute.call_count, 4)
            sides = []
            for call in execute.call_args_list:
                command = call.args[0]
                self.assertEqual(command[-1], "--exact-worker")
                sides.append(Path(command[-2]).relative_to((root / "performance-builds").resolve()).parts[0])
            self.assertEqual(sides, ["baseline", "candidate", "candidate", "baseline"])
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertTrue(protocol["instrumented"])
            self.assertTrue(protocol["exact_worker"])
            self.assertEqual(protocol["completion_boundary"], "original-BenchmarkDotNet-AwaitHelper.GetResult")
            self.assertIn("excludes initial handler work before read return", protocol["worker_boundary"])

    def test_worker_rejects_other_regions_or_profile_before_launching(self):
        for options in (("--exact-invocations",), ("--exact-releases",), ("--exact-driver",),
                        ("--exact-roundtrips",), ("--exact-sources",), ("--bdn-await",),
                        ("--trace-tool", "trace", "--profile-reader", "reader")):
            with self.subTest(options=options), patch("sys.argv", ["control", "inputs", "probe", "output", "--exact-worker", *options]), patch("frozen_concurrent_control.subprocess.run") as execute:
                with self.assertRaises(SystemExit):
                    main()
                execute.assert_not_called()
