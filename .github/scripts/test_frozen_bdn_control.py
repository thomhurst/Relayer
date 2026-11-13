import json
import shutil
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_bdn_control import executable, main, result, runtime_command


COMPLETE = "WorkloadResult   1: 65536 op, 1 ns, 1 ns/op\n// GC:  0 0 0 45823680 65536\n// AfterAll\n"


class NativeResultTests(unittest.TestCase):
    def test_retained_uninstrumented_launcher_needs_no_allocation_readers(self):
        # Existing control artifacts retain these two scripts without allocation readers.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("frozen_bdn_control.py", "frozen_concurrent_control.py"):
                shutil.copyfile(Path(__file__).with_name(name), root / name)
            completed = subprocess.run([sys.executable, str(root / "frozen_bdn_control.py"), "--help"],
                                       cwd=root, capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("--allocation-profiler", completed.stdout)

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

    def test_same_image_holds_original_runner_constant_and_preserves_all_outcomes(self):
        for side in ("baseline", "candidate"):
            for profiled in (False, True):
                with self.subTest(side=side, profiled=profiled), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    for image, identity in (("baseline", "10"), ("candidate", "17")):
                        self.fixture(root / "performance-builds" / image, identity)
                    output = root / "results"
                    calls = []

                    def execute(command, **kwargs):
                        calls.append(command)
                        original = command[command.index("--") + 1:] if profiled else command
                        self.assertIn(f"/performance-builds/{side}/", original[1])
                        self.assertEqual(original[-1], "10" if side == "baseline" else "17")
                        if profiled:
                            Path(command[command.index("--output") + 1]).write_bytes(b"raw trace")
                        if len(calls) == 2:
                            raise subprocess.CalledProcessError(1, command)
                        kwargs["stdout"].write(COMPLETE)

                    arguments = ["control", str(root), str(output), "--same-image", side]
                    if profiled:
                        arguments.extend(["--trace-tool", str(root / "dotnet-trace")])
                    with patch("sys.argv", arguments), \
                         patch("frozen_bdn_control.verify", return_value="frozen") as verified, \
                         patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    self.assertEqual(len(calls), 4)
                    self.assertEqual(verified.call_count, 6)
                    protocol = json.loads((output / "protocol.json").read_text())
                    self.assertEqual(protocol["sameImage"], side)
                    self.assertEqual(protocol["launches"], [side] * 4)
                    self.assertIn("no candidate-versus-baseline comparison", protocol["limits"])
                    outcomes = json.loads((output / "outcomes.json").read_text())
                    self.assertEqual([o["side"] for o in outcomes], [side] * 4)
                    self.assertIn("error", outcomes[1])
                    self.assertNotIn("bytesPerMessage", outcomes[1])
                    self.assertEqual(outcomes[3]["operations"], 65536)
                    if profiled:
                        self.assertEqual(len(list(output.glob("*.nettrace"))), 4)

    def test_native_parent_retains_broker_receipts_and_all_failed_launches(self):
        for invalid in (None, "exit", "errors", "missing", "malformed"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / "results"
                calls = []
                (root / "image.runtimeconfig.json").write_text(json.dumps({"runtimeOptions": {"framework": {"name": "Microsoft.NETCore.App", "version": "10.0.0"}}}))

                def execute(command, **kwargs):
                    calls.append(command)
                    self.assertEqual(command[1], str((root / "host.dll").resolve()))
                    self.assertEqual(command[3], "10")
                    receipt = {"FoundExecutable": True, "ExitCode": 0, "IsSuccess": True, "Errors": [],
                               "Results": ["WorkloadResult   1: 65536 op, 1 ns, 1 ns/op"],
                               "PrefixedLines": ["// GC:  0 0 0 45823680 65536", "// AfterAll"],
                               "StandardOutput": ["// Runtime=.NET 10.0.15 (test), ARM64"]}
                    if len(calls) == 2:
                        if invalid == "exit": receipt["ExitCode"] = 1
                        if invalid == "errors": receipt["Errors"] = ["native error"]
                        if invalid == "missing": return
                        if invalid == "malformed":
                            Path(command[-1]).write_text("{")
                            return
                    Path(command[-1]).write_text(json.dumps(receipt))
                    Path(command[-2]).write_text("// Runtime=.NET 10.0.15 (test), ARM64\n" + COMPLETE)

                with patch("sys.argv", ["control", str(root), str(output), "--native-executor", str(root / "host.dll"),
                                        "--runtime-version", "10.0.15"]), \
                     patch("frozen_bdn_control.verify", return_value="frozen"), \
                     patch("frozen_bdn_control.executable", return_value=(root / "image.dll", "10")), \
                     patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                    if invalid:
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    else:
                        main()
                self.assertEqual(len(calls), 4)
                outcomes = json.loads((output / "outcomes.json").read_text())
                self.assertEqual("error" in outcomes[1], invalid is not None)
                self.assertEqual(outcomes[3]["operations"], 65536)
                self.assertEqual(outcomes[3]["runtime"], "10.0.15")
                self.assertIn("--fx-version 10.0.15 --roll-forward Disable", (output / "native-dotnet.sh").read_text())
                protocol = json.loads((output / "protocol.json").read_text())
                self.assertIn("anonymous pipes/Broker", protocol["host"])
                self.assertIn("bundled protocol intervention", protocol["limits"])

    def test_native_parent_rejects_unsupported_combinations(self):
        for extra in (["--case", "scoped-dispatch"], ["--trace-tool", "trace"], ["--allocation-profiler", "profiler"]):
            with self.subTest(extra=extra), patch("sys.argv", ["control", "inputs", "output", "--native-executor", "host", *extra]):
                with self.assertRaises(SystemExit):
                    main()

    def test_selects_throughput_cases_from_original_parameterized_runners(self):
        cases = (
            ("scoped-dispatch", "ConsumePipelineBenchmarks", "DirectScopedDispatch",
             "instance.ScopedHandler = true;", "instance.ScopedHandler = false;"),
            ("log-composed", "InMemoryTransportBenchmarks", "ComposedRoundTrip",
             "instance.Mode = (Relayer.Transports.ConsumptionModel)(1);", "instance.Mode = (Relayer.Transports.ConsumptionModel)(0);"),
            ("queue-composed", "InMemoryTransportBenchmarks", "ComposedRoundTrip",
             "instance.Mode = (Relayer.Transports.ConsumptionModel)(0);", "instance.Mode = (Relayer.Transports.ConsumptionModel)(1);"),
            ("queue-native", "InMemoryTransportBenchmarks", "NativeRoundTrip",
             "instance.Mode = (Relayer.Transports.ConsumptionModel)(0);", "instance.Mode = (Relayer.Transports.ConsumptionModel)(1);"),
        )
        for case, owner, method, parameter, opposite in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, image = self.fixture(root)
                # Include the same method with the opposite parameter. Selecting only
                # by method would execute a different benchmark than the failing case.
                runner = lambda identity, assignment: f"""public unsafe class Runnable_{identity} : global::Relayer.Benchmarks.{owner}
                {{ OperationsPerInvoke = 1, MeasureExtraStats = true,
                {assignment} workloadDelegate = () => BenchmarkDotNet.Helpers.AwaitHelper.GetResult({method}()); }}"""
                source.write_text(runner("31", opposite) + runner("32", parameter))
                self.assertEqual(executable(root, case), (image.resolve(), "32"))
                source.write_text(runner("31", opposite))
                with self.assertRaisesRegex(ValueError, "selected parameter"):
                    executable(root, case)
                source.write_text(runner("31", parameter) + runner("32", parameter))
                with self.assertRaisesRegex(ValueError, "selected parameter"):
                    executable(root, case)

    def test_one_operation_case_retains_native_timings_without_message_denominator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "results"
            image = root / "original.dll"
            log = "WorkloadResult   1: 3 op, 9 ns, 3 ns/op\nWorkloadResult   2: 3 op, 15 ns, 5 ns/op\n// GC:  0 0 0 72 3\n// AfterAll\n"
            calls = []

            def execute(command, **kwargs):
                calls.append(command)
                kwargs["stdout"].write(log)

            with patch("sys.argv", ["control", str(root), str(output), "--case", "scoped-dispatch"]), \
                 patch("frozen_bdn_control.verify", return_value="frozen"), \
                 patch("frozen_bdn_control.executable", return_value=(image, "6")), \
                 patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                main()
            self.assertEqual(len(calls), 4)
            self.assertTrue(all(command[command.index("--benchmarkName") + 1] ==
                                "Relayer.Benchmarks.ConsumePipelineBenchmarks.DirectScopedDispatch" for command in calls))
            outcomes = json.loads((output / "outcomes.json").read_text())
            self.assertTrue(all(item["bytesPerOperation"] == 24 and item["meanNs"] == 4 for item in outcomes))
            self.assertTrue(all(item["workloadResultNs"] == [3, 5] for item in outcomes))
            self.assertTrue(all("bytesPerMessage" not in item for item in outcomes))
            protocol = json.loads((output / "protocol.json").read_text())
            self.assertEqual(protocol["operationsPerInvoke"], 1)
            self.assertEqual(protocol["parameter"], "instance.ScopedHandler = true;")

    def test_exact_engine_callbacks_preserve_every_launch_and_reject_incomplete_boundaries(self):
        for invalid in (None, "missing", "manual", "repeated", "callback", "malformed"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for side in ("baseline", "candidate"):
                    self.fixture(root / "performance-builds" / side)
                output = root / "results"
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    self.assertNotIn("--anonymousPipes", command)
                    env = kwargs["env"]
                    self.assertEqual(env["RELAYER_ALLOCATION_TYPE"], "BenchmarkDotNet.Engines.Engine")
                    self.assertEqual(env["RELAYER_ALLOCATION_METHOD"], "MeasureWithGc")
                    self.assertEqual(env["CORECLR_PROFILER_PATH"], str((root / "profiler.so").resolve()))
                    kwargs["stdout"].write(COMPLETE)
                    if len(calls) == 2 and invalid == "missing":
                        return
                    coverage = {"kind": "coverage", "objects": 1, "bytes": 24, "failures": 0,
                                "boundary": "method-enter-leave", "methodEntries": 1, "methodLeaves": 1}
                    if len(calls) == 2 and invalid == "malformed":
                        Path(env["RELAYER_ALLOCATION_OUTPUT"]).write_text('{"kind":"coverage"}\n')
                        return
                    if len(calls) == 2:
                        if invalid == "manual": coverage["boundary"] = "external-marker"
                        if invalid == "repeated": coverage["methodEntries"] = 2
                        if invalid == "callback": coverage["failures"] = 1
                    rows = [{"kind": "type", "classId": 42, "type": "Message", "objects": 1, "bytes": 24}, coverage]
                    Path(env["RELAYER_ALLOCATION_OUTPUT"]).write_text("\n".join(map(json.dumps, rows)))

                with patch("sys.argv", ["control", str(root), str(output), "--allocation-profiler", str(root / "profiler.so"), "--allocation-boundary", "method"]), \
                     patch("frozen_bdn_control.verify", return_value="frozen") as verified, \
                     patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                    if invalid:
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    else:
                        main()
                self.assertEqual(len(calls), 4)
                self.assertEqual(verified.call_count, 6)
                outcomes = json.loads((output / "outcomes.json").read_text())
                self.assertEqual("allocationError" in outcomes[1], invalid is not None)
                self.assertEqual(outcomes[3]["callbackCoverage"]["methodEntries"], 1)
                self.assertEqual(outcomes[3]["operations"], 65536)
                protocol = json.loads((output / "protocol.json").read_text())
                self.assertTrue(protocol["instrumented"])
                self.assertEqual(protocol["allocationMethod"], "BenchmarkDotNet.Engines.Engine.MeasureWithGc")

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

    def test_profiled_replay_retains_native_stats_and_requires_every_raw_capture(self):
        for missing in (False, True):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for side in ("baseline", "candidate"):
                    self.fixture(root / "performance-builds" / side)
                output = root / "results"
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    self.assertEqual(command[1:4], ["collect", "--profile", "gc-verbose"])
                    self.assertIn("--show-child-io", command)
                    self.assertEqual(command[command.index("--providers") + 1], "BenchmarkDotNet.EngineEventSource:0xffffffffffffffff:4")
                    original = command[command.index("--") + 1:]
                    self.assertEqual(original[0], "dotnet")
                    self.assertIn("--job", original)
                    kwargs["stdout"].write(COMPLETE)
                    trace = Path(command[command.index("--output") + 1])
                    if not (missing and len(calls) == 2):
                        trace.write_bytes(b"retained raw trace")

                with patch("sys.argv", ["control", str(root), str(output), "--trace-tool", str(root / "dotnet-trace")]), \
                     patch("frozen_bdn_control.verify", return_value="frozen"), \
                     patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                    if missing:
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    else:
                        main()
                self.assertEqual(len(calls), 4)
                outcomes = json.loads((output / "outcomes.json").read_text())
                self.assertTrue(all(item["operations"] == 65536 for item in outcomes))
                self.assertEqual("profileError" in outcomes[1], missing)
                self.assertTrue((output / "4-baseline.nettrace").exists())
                protocol = json.loads((output / "protocol.json").read_text())
                self.assertTrue(protocol["instrumented"])
                self.assertIsNone(protocol["sameImage"])
                self.assertEqual(protocol["launches"], ["baseline", "candidate", "candidate", "baseline"])
                self.assertIn("Whole-process", protocol["profile_limits"])
                self.assertIn("no Relayer measurement markers", protocol["profile_limits"])

    def test_exact_patch_selection_preserves_image_and_rejects_other_frameworks(self):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "runner.dll"
            config = image.with_suffix(".runtimeconfig.json")
            original = json.dumps({"runtimeOptions": {"framework": {
                "name": "Microsoft.NETCore.App", "version": "10.0.0"}}})
            config.write_text(original)
            self.assertEqual(runtime_command(image, "10.0.14"),
                             ["dotnet", "--fx-version", "10.0.14", "--roll-forward", "Disable", str(image)])
            self.assertEqual(config.read_text(), original)
            self.assertEqual(runtime_command(image, None), ["dotnet", str(image)])
            for invalid in ("11.0.0", "10.0.x", "10.0.14 --help"):
                with self.assertRaises(ValueError):
                    runtime_command(image, invalid)
            config.write_text(original.replace("Microsoft.NETCore.App", "Microsoft.AspNetCore.App"))
            with self.assertRaises(ValueError):
                runtime_command(image, "10.0.14")
            config.write_text(json.dumps({"runtimeOptions": {"frameworks": [
                {"name": "Microsoft.NETCore.App", "version": "10.0.0"}]}}))
            with self.assertRaises(ValueError):
                runtime_command(image, "10.0.14")

    def test_same_image_runtime_mismatch_fails_closed_and_attempts_every_profile(self):
        for mismatch in (False, True):
            with self.subTest(mismatch=mismatch), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for side in ("baseline", "candidate"):
                    _, image = self.fixture(root / "performance-builds" / side)
                    image.with_suffix(".runtimeconfig.json").write_text(json.dumps({"runtimeOptions": {
                        "framework": {"name": "Microsoft.NETCore.App", "version": "10.0.0"}}}))
                output = root / "results"
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    original = command[command.index("--") + 1:]
                    self.assertEqual(original[:5], ["dotnet", "--fx-version", "10.0.14", "--roll-forward", "Disable"])
                    self.assertIn("/performance-builds/candidate/", original[5])
                    version = "10.0.15" if mismatch and len(calls) == 2 else "10.0.14"
                    kwargs["stdout"].write("// Runtime=.NET " + version + " (test), X64\n" + COMPLETE)
                    Path(command[command.index("--output") + 1]).write_bytes(b"trace")

                with patch("sys.argv", ["control", str(root), str(output), "--runtime-version", "10.0.14",
                                        "--trace-tool", str(root / "dotnet-trace"), "--same-image", "candidate"]), \
                     patch("frozen_bdn_control.verify", return_value="frozen"), \
                     patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                    if mismatch:
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    else:
                        main()
                self.assertEqual(len(calls), 4)
                outcomes = json.loads((output / "outcomes.json").read_text())
                self.assertEqual("error" in outcomes[1], mismatch)
                self.assertEqual(outcomes[3]["runtime"], "10.0.14")
                self.assertEqual([o["side"] for o in outcomes], ["candidate"] * 4)
                self.assertEqual(json.loads((output / "protocol.json").read_text())["requestedRuntime"], "10.0.14")

    def test_exact_callbacks_attempt_every_launch_and_fail_closed_on_denominator_mismatch(self):
        from test_native_tail_allocations import report
        for invalid, same_image in ((invalid, same) for invalid in (None, "coverage", "denominator", "missing") for same in (False, True)):
            with self.subTest(invalid=invalid, same_image=same_image), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for side in ("baseline", "candidate"):
                    _, image = self.fixture(root / "performance-builds" / side)
                    image.with_suffix(".runtimeconfig.json").write_text(json.dumps({"runtimeOptions": {
                        "framework": {"name": "Microsoft.NETCore.App", "version": "10.0.0"}}}))
                output = root / "results"
                calls = []

                def execute(command, **kwargs):
                    calls.append(command)
                    environment = kwargs["env"]
                    self.assertEqual(environment["RELAYER_ALLOCATION_BDN_TAIL"], "1")
                    self.assertEqual(environment["CORECLR_ENABLE_PROFILING"], "1")
                    self.assertIn("--job", command)
                    self.assertNotIn("collect", command)
                    if same_image:
                        self.assertEqual(command[:5], ["dotnet", "--fx-version", "10.0.14", "--roll-forward", "Disable"])
                        self.assertIn("/performance-builds/candidate/", command[5])
                    kwargs["stdout"].write("// Runtime=.NET 10.0.14 (test), X64\n" + COMPLETE)
                    rows = report()
                    # Keep every native iteration payload and final scope consistent.
                    for row in rows:
                        if row["kind"] == "engine-event" and row["id"] in (15, 16):
                            row["operations"] = 65536
                        if row["kind"] == "native-engine-tail":
                            row["nativeOperations"] = 65536
                    if len(calls) == 2:
                        if invalid == "coverage":
                            rows[-1]["failures"] = 1
                        elif invalid == "denominator":
                            rows[-3]["nativeOperations"] = 8
                        elif invalid == "missing":
                            return
                    Path(environment["RELAYER_ALLOCATION_OUTPUT"]).write_text("\n".join(map(json.dumps, rows)))

                with patch("sys.argv", ["control", str(root), str(output), "--allocation-profiler", str(root / "profiler.so"),
                                        *(["--same-image", "candidate", "--runtime-version", "10.0.14"] if same_image else [])]), \
                     patch("frozen_bdn_control.verify", return_value="frozen"), \
                     patch("frozen_bdn_control.subprocess.run", side_effect=execute):
                    if invalid:
                        with self.assertRaisesRegex(SystemExit, "launches failed"):
                            main()
                    else:
                        main()
                self.assertEqual(len(calls), 4)
                outcomes = json.loads((output / "outcomes.json").read_text())
                self.assertEqual("allocationError" in outcomes[1], invalid is not None)
                self.assertEqual(outcomes[3]["callbackBytes"], 72)
                self.assertTrue((output / f"4-{'candidate' if same_image else 'baseline'}-objects.jsonl").exists())
                if same_image:
                    self.assertEqual([item["side"] for item in outcomes], ["candidate"] * 4)
                    self.assertEqual(outcomes[3]["runtime"], "10.0.14")
                    self.assertIn("Same-image negative control", json.loads((output / "protocol.json").read_text())["limits"])

    def test_exact_callbacks_reject_other_benchmark_cases_before_launch(self):
        for case in ("scoped-dispatch", "log-composed", "queue-composed", "queue-native"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory, \
                 patch("sys.argv", ["control", directory, str(Path(directory) / "results"),
                                    "--allocation-profiler", "profiler.so", "--case", case]), \
                 patch("frozen_bdn_control.verify") as verify_input, \
                 patch("frozen_bdn_control.subprocess.run") as execute:
                with self.assertRaises(SystemExit):
                    main()
                verify_input.assert_not_called()
                execute.assert_not_called()

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
