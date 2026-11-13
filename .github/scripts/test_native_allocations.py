import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from read_native_allocations import read, read_rows
from read_receive_allocations import correlate


class NativeAllocationReportTests(unittest.TestCase):
    def check(self, mutate=None):
        rows = [{"kind": "type", "classId": 42, "type": "Message", "objects": 10, "bytes": 240},
                {"kind": "coverage", "objects": 10, "bytes": 240, "failures": 0}]
        if mutate:
            mutate(rows)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.jsonl"
            path.write_text("\n".join(map(json.dumps, rows)))
            return read(path)

    def test_valid_report(self):
        self.assertEqual(self.check()["coverage"]["objects"], 10)

    def test_missing_coverage(self):
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            self.check(lambda rows: rows.pop())

    def test_failed_resolution(self):
        with self.assertRaisesRegex(ValueError, "failures"):
            self.check(lambda rows: rows[-1].update(failures=1))

    def test_inconsistent_sum(self):
        with self.assertRaisesRegex(ValueError, "total"):
            self.check(lambda rows: rows[-1].update(bytes=241))

    def test_duplicate_class(self):
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.check(lambda rows: rows.insert(0, rows[0].copy()))

    def test_negative_count(self):
        with self.assertRaisesRegex(ValueError, "Invalid"):
            self.check(lambda rows: rows[0].update(objects=-1))


class WindowAllocationReportTests(unittest.TestCase):
    def rows(self):
        return [
            {"kind": "window-type", "index": 0, "classId": 42, "type": "Message", "objects": 2, "bytes": 48},
            {"kind": "window", "index": 0, "mode": 0, "measured": 111, "objects": 2, "bytes": 48},
            {"kind": "window", "index": 1, "mode": 1, "measured": 222, "objects": 0, "bytes": 0},
            {"kind": "window", "index": 2, "mode": 2, "measured": 333, "objects": 0, "bytes": 0},
            {"kind": "type", "classId": 42, "type": "Message", "objects": 2, "bytes": 48},
            {"kind": "coverage", "objects": 2, "bytes": 48, "failures": 0, "windows": 3},
        ]

    def test_valid_empty_windows_and_counters_distinct_from_object_bytes(self):
        result = read_rows(self.rows())
        self.assertEqual([row["measured"] for row in result["windows"]], [111, 222, 333])
        self.assertEqual(result["windows"][1]["types"], [])

    def test_incomplete_triplet(self):
        rows = self.rows()
        rows[-1]["windows"] = 2
        with self.assertRaisesRegex(ValueError, "Incomplete window"):
            read_rows(rows)

    def test_wrong_boundary_order(self):
        rows = self.rows()
        rows[2]["index"] = 2
        with self.assertRaisesRegex(ValueError, "window boundaries"):
            read_rows(rows)

    def test_wrong_type_window(self):
        rows = self.rows()
        rows[0]["index"] = 1
        with self.assertRaisesRegex(ValueError, "association"):
            read_rows(rows)

    def test_wrong_window_sum(self):
        rows = self.rows()
        rows[1]["bytes"] += 1
        with self.assertRaisesRegex(ValueError, "Window total"):
            read_rows(rows)

    def test_unassigned_aggregate_object(self):
        rows = self.rows()
        rows[-2]["objects"] += 1
        rows[-1]["objects"] += 1
        with self.assertRaisesRegex(ValueError, "aggregate"):
            read_rows(rows)


class ReceiveWindowCorrelationTests(unittest.TestCase):
    def data(self):
        report = {"windows": [{"measured": index} for index in range(3000)]}
        log = "\n".join(f"window={index}; scoped={index * 3}; consumer={index * 3 + 1}; receive={index * 3 + 2}" for index in range(1000))
        return report, log

    def test_all_counters_from_the_same_execution(self):
        report, log = self.data()
        self.assertIs(correlate(report, log), report)

    def test_missing_final_log_window(self):
        report, log = self.data()
        with self.assertRaisesRegex(ValueError, "1000"):
            correlate(report, log.rsplit("\n", 1)[0])

    def test_mislabelled_counter(self):
        report, log = self.data()
        report["windows"][7]["measured"] += 1
        with self.assertRaisesRegex(ValueError, "differs"):
            correlate(report, log)

    def test_missing_checkpoint(self):
        report, log = self.data()
        report["windows"].pop()
        with self.assertRaisesRegex(ValueError, "3000"):
            correlate(report, log)


@unittest.skipUnless(os.environ.get("TEST_NATIVE_PROFILER"), "Native profiler fixture paths not supplied")
class NativeAllocationFixtureTests(unittest.TestCase):
    def test_exact_counts_exclude_warmup_and_resolve_generic_arguments(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        fixture = str(Path(os.environ["TEST_NATIVE_FIXTURE"]).resolve())
        evidence = Path(os.environ["TEST_NATIVE_OUTPUT"])
        evidence.mkdir(parents=True, exist_ok=True)
        for count in (1, 1024, 16384):
            report = (evidence / f"fixture-{count}.jsonl").resolve()
            environment = dict(os.environ, CORECLR_ENABLE_PROFILING="1",
                               CORECLR_PROFILER="{32B7EF07-25E8-4AC7-B011-9A4321D702C8}",
                               CORECLR_PROFILER_PATH=profiler, RELAYER_ALLOCATION_PROFILER=profiler,
                               RELAYER_ALLOCATION_OUTPUT=str(report))
            result = subprocess.run(["dotnet", fixture, str(count)], env=environment,
                                    capture_output=True, text=True, timeout=60)
            (evidence / f"fixture-{count}.log").write_text(result.stdout + result.stderr)
            self.assertEqual(result.returncode, 0, result.stderr)
            types = read(report)["types"]
            for name in ("FixtureMessage", "FixtureTypes+Box`1[FixtureMessage]"):
                records = [row for row in types if row["type"] == name]
                self.assertEqual(len(records), 1, types)
                self.assertEqual(records[0]["objects"], count)
                self.assertEqual(records[0]["bytes"], count * 24)

    def test_window_markers_partition_multithread_and_caller_allocations(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        fixture = str(Path(os.environ["TEST_NATIVE_FIXTURE"]).resolve())
        evidence = Path(os.environ["TEST_NATIVE_OUTPUT"])
        evidence.mkdir(parents=True, exist_ok=True)
        report = (evidence / "windows.jsonl").resolve()
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING="1",
                           CORECLR_PROFILER="{32B7EF07-25E8-4AC7-B011-9A4321D702C8}",
                           CORECLR_PROFILER_PATH=profiler, RELAYER_ALLOCATION_PROFILER=profiler,
                           RELAYER_ALLOCATION_OUTPUT=str(report))
        result = subprocess.run(["dotnet", fixture, "1024", "--windows"], env=environment,
                                capture_output=True, text=True, timeout=60)
        (evidence / "windows.log").write_text(result.stdout + result.stderr)
        self.assertEqual(result.returncode, 0, result.stderr)
        actual = read(report)
        self.assertEqual([row["measured"] for row in actual["windows"]], [111, 222, 333])
        for window in actual["windows"]:
            for name in ("FixtureMessage", "FixtureTypes+Box`1[FixtureMessage]"):
                entries = [row for row in window["types"] if row["type"] == name]
                if window["index"] == 2:
                    self.assertEqual(entries, [])
                else:
                    self.assertEqual(len(entries), 1, window)
                    self.assertEqual(entries[0]["objects"], 1024)
                    self.assertEqual(entries[0]["bytes"], 1024 * 24)

    def test_unfinished_window_capture_fails_with_retained_coverage(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        evidence = Path(os.environ["TEST_NATIVE_OUTPUT"])
        evidence.mkdir(parents=True, exist_ok=True)
        report = (evidence / "windows-missing.jsonl").resolve()
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING="1",
                           CORECLR_PROFILER="{32B7EF07-25E8-4AC7-B011-9A4321D702C8}",
                           CORECLR_PROFILER_PATH=profiler, RELAYER_ALLOCATION_PROFILER=profiler,
                           RELAYER_ALLOCATION_OUTPUT=str(report))
        result = subprocess.run(["dotnet", os.environ["TEST_NATIVE_FIXTURE"], "1024", "--windows-missing"],
                                env=environment, capture_output=True, text=True, timeout=60)
        (evidence / "windows-missing.log").write_text(result.stdout + result.stderr)
        self.assertNotEqual(result.returncode, 0)
        with self.assertRaisesRegex(ValueError, "failures"):
            read(report)

    def test_exact_method_counts_exclude_outside_work_and_reject_missing_or_repeated_boundaries(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        fixture = str(Path(os.environ["TEST_NATIVE_FIXTURE"]).resolve())
        evidence = Path(os.environ["TEST_NATIVE_OUTPUT"])
        evidence.mkdir(parents=True, exist_ok=True)
        for mode in ("--method", "--twice", "--missing", "--throw"):
            for count in (1, 1024, 16384) if mode == "--method" else (1024,):
                report = (evidence / f"method-{mode[2:]}-{count}.jsonl").resolve()
                environment = dict(os.environ, CORECLR_ENABLE_PROFILING="1",
                                   CORECLR_PROFILER="{32B7EF07-25E8-4AC7-B011-9A4321D702C8}",
                                   CORECLR_PROFILER_PATH=profiler, RELAYER_ALLOCATION_OUTPUT=str(report),
                                   RELAYER_ALLOCATION_TYPE="FixtureMeasurement",
                                   RELAYER_ALLOCATION_METHOD="Absent" if mode == "--missing" else "MeasureWithGc")
                result = subprocess.run(["dotnet", fixture, str(count), mode], env=environment,
                                        capture_output=True, text=True, timeout=60)
                (evidence / f"method-{mode[2:]}-{count}.log").write_text(result.stdout + result.stderr)
                self.assertEqual(result.returncode, 0, result.stderr)
                if mode != "--method":
                    with self.assertRaises(ValueError):
                        read(report)
                    continue
                actual = read(report)
                self.assertEqual(actual["coverage"]["boundary"], "method-enter-leave")
                self.assertEqual(actual["coverage"]["methodEntries"], 1)
                self.assertEqual(actual["coverage"]["methodLeaves"], 1)
                for name in ("FixtureMessage", "FixtureTypes+Box`1[FixtureMessage]"):
                    records = [row for row in actual["types"] if row["type"] == name]
                    self.assertEqual(len(records), 1, actual)
                    self.assertEqual(records[0]["objects"], count)
                    self.assertEqual(records[0]["bytes"], count * 24)

    def test_missing_startup_profiler_fails_instead_of_emitting_empty_success(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING="0", RELAYER_ALLOCATION_PROFILER=profiler)
        result = subprocess.run(["dotnet", os.environ["TEST_NATIVE_FIXTURE"], "1"], env=environment,
                                capture_output=True, text=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absent, uninitialized", result.stderr)
