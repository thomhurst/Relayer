import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_bdn_control import executable, main, result


COMPLETE = "WorkloadResult   1: 65536 op, 1 ns, 1 ns/op\n// GC:  0 0 0 45823680 65536\n// AfterAll\n"


class NativeResultTests(unittest.TestCase):
    def test_uses_native_operations_without_dividing_by_eight_again(self):
        actual = result(COMPLETE)
        self.assertEqual(actual["operations"], 65536)
        self.assertEqual(actual["allocatedBytes"], 45823680)
        self.assertEqual(actual["bytesPerMessage"], 45823680 / 65536)
        self.assertEqual(actual["bdnRoundedBytesPerMessage"], 699)

    def test_rejects_incomplete_ambiguous_and_invalid_results(self):
        for text in ("", COMPLETE * 2, COMPLETE.replace("// AfterAll", ""),
                     COMPLETE.replace("45823680", "?"), COMPLETE.replace("65536", "0"),
                     COMPLETE.replace("65536", "9"), COMPLETE.replace("WorkloadResult", "WorkloadWarmup")):
            with self.subTest(text=text), self.assertRaises(ValueError):
                result(text)


class OriginalRunnerTests(unittest.TestCase):
    def fixture(self, root, identity="10"):
        generated = root / "builds/net10.0/Relayer.Benchmarks-DefaultJob-1"
        generated.mkdir(parents=True)
        source = generated / "Relayer.Benchmarks-DefaultJob-1.notcs"
        source.write_text(f"""public unsafe class Runnable_{identity} : global::Relayer.Benchmarks.EndToEndBenchmarks
        {{ OperationsPerInvoke = 8, MeasureExtraStats = true,
        workloadDelegate = () => BenchmarkDotNet.Helpers.AwaitHelper.GetResult(ConcurrentRoundTrip()); }}""")
        image = generated / "bin/Release/net10.0/Relayer.Benchmarks-DefaultJob-1.dll"
        image.parent.mkdir(parents=True)
        image.touch()
        (root / "inventory-sha256.json").write_text(json.dumps({"files": [
            {"path": path.relative_to(root).as_posix()} for path in (source, image)]}))
        return source, image

    def test_discovers_runner_from_inventoried_generated_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, image = self.fixture(root, "17")
            self.assertEqual(executable(root), (image.resolve(), "17"))

    def test_rejects_uninventoried_or_wrong_job_input(self):
        for invalid in ("inventory", "operations", "stats", "runner"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, _ = self.fixture(root)
                if invalid == "inventory":
                    (root / "inventory-sha256.json").write_text('{"files": []}')
                else:
                    old, new = {"operations": ("OperationsPerInvoke = 8", "OperationsPerInvoke = 1"),
                                "stats": ("MeasureExtraStats = true", "MeasureExtraStats = false"),
                                "runner": ("ConcurrentRoundTrip()", "SynchronousRoundTrip()")}[invalid]
                    source.write_text(source.read_text().replace(old, new))
                with self.assertRaises(ValueError):
                    executable(root)

    def test_attempts_all_launches_and_retains_error_without_summarizing_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate"):
                self.fixture(root / "performance-builds" / side)
            output = root / "results"
            calls = []

            def execute(command, **kwargs):
                calls.append(command)
                if len(calls) == 2:
                    raise subprocess.CalledProcessError(1, command)
                kwargs["stdout"].write(COMPLETE)

            with patch("sys.argv", ["control", str(root), str(output)]), \
                 patch("frozen_bdn_control.verify", return_value="frozen") as verify_input, \
                 patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                with self.assertRaisesRegex(SystemExit, "launches failed"):
                    main()
            self.assertEqual(len(calls), 4)
            self.assertEqual(calls[0], calls[3])
            self.assertEqual(calls[1], calls[2])
            self.assertEqual(calls[0][-1], "10")
            self.assertNotIn("--anonymousPipes", calls[0])
            self.assertEqual(verify_input.call_count, 6)
            outcomes = json.loads((output / "outcomes.json").read_text())
            self.assertIn("error", outcomes[1])
            self.assertNotIn("bytesPerMessage", outcomes[1])
            self.assertEqual(outcomes[3]["bdnRoundedBytesPerMessage"], 699)
            self.assertTrue((output / "2-candidate.log").read_text().startswith("\nLaunch failed:"))

    def test_invalid_input_prevents_every_process_launch(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch("sys.argv", ["control", directory, str(Path(directory) / "results")]), \
             patch("frozen_bdn_control.verify", side_effect=ValueError("Retained input changed")), \
             patch("frozen_bdn_control.subprocess.run") as execute:
            with self.assertRaisesRegex(ValueError, "Retained input changed"):
                main()
            execute.assert_not_called()

    def test_complete_abba_preserves_native_stats_and_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate"):
                self.fixture(root / "performance-builds" / side)
            output = root / "results"
            with patch("sys.argv", ["control", str(root), str(output)]), \
                 patch("frozen_bdn_control.verify", return_value="frozen"), \
                 patch("frozen_bdn_control.subprocess.run", side_effect=lambda *args, **kwargs: kwargs["stdout"].write(COMPLETE)):
                main()
            outcomes = json.loads((output / "outcomes.json").read_text())
            self.assertEqual([item["side"] for item in outcomes], ["baseline", "candidate", "candidate", "baseline"])
            self.assertTrue(all(item["operations"] == 65536 for item in outcomes))
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertFalse(protocol["instrumented"])
            self.assertEqual(protocol["operationsPerInvoke"], 8)


if __name__ == "__main__":
    unittest.main()
