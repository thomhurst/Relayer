import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from read_native_allocations import read


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

    def test_missing_startup_profiler_fails_instead_of_emitting_empty_success(self):
        profiler = str(Path(os.environ["TEST_NATIVE_PROFILER"]).resolve())
        environment = dict(os.environ, CORECLR_ENABLE_PROFILING="0", RELAYER_ALLOCATION_PROFILER=profiler)
        result = subprocess.run(["dotnet", os.environ["TEST_NATIVE_FIXTURE"], "1"], env=environment,
                                capture_output=True, text=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absent, uninitialized", result.stderr)
