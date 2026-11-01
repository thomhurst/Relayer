import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

import benchmark_gate as gate
import pin_benchmark_version


def report():
    return {"Benchmarks": [
        {"FullName": f"Relayer.Benchmarks.{type_name}.{method}",
         "Type": type_name, "Method": method,
         "Statistics": {"Mean": 1000, "ConfidenceInterval": {"Lower": 1000, "Upper": 1000}},
         "Memory": {"BytesAllocatedPerOperation": 152}}
        for type_name, method in sorted(gate.REQUIRED_METHODS)
    ]}


def measurement(case, mean, lower=None, upper=None):
    case["Statistics"] = {"Mean": mean, "ConfidenceInterval": {
        "Lower": mean if lower is None else lower,
        "Upper": mean if upper is None else upper,
    }}


class BenchmarkGateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def write(self, name, data):
        directory = self.root / name
        directory.mkdir(exist_ok=True)
        (directory / "benchmarks-report-full.json").write_text(json.dumps(data), encoding="utf-8")
        return directory

    def compare(self, before, after):
        return gate.compare(gate.load_reports(self.write("before", before)), gate.load_reports(self.write("after", after)))[0]

    def test_unchanged_baseline_passes_with_scope_allocations_visible(self):
        self.assertEqual([], self.compare(report(), report()))

    def test_throughput_loss_is_reciprocal_of_mean_not_latency_percent(self):
        before, after = report(), report()
        measurement(after["Benchmarks"][0], 1000 / 0.95)
        self.assertEqual([], self.compare(before, after))
        measurement(after["Benchmarks"][0], 1000 / 0.95 + 1)
        self.assertIn("throughput loss exceeds 5%", self.compare(before, after)[0])

    def test_variable_unchanged_measurements_pass(self):
        data = report()
        measurement(data["Benchmarks"][0], 3500, 3300, 3700)
        self.assertEqual([], self.compare(data, data))

    def test_observed_regression_with_overlapping_bounds_is_inconclusive(self):
        before, after = report(), report()
        measurement(before["Benchmarks"][0], 3500, 3300, 3700)
        measurement(after["Benchmarks"][0], 3800, 3500, 4100)
        failures, summary = gate.compare(gate.load_reports(self.write("before", before)), gate.load_reports(self.write("after", after)))
        self.assertEqual([], failures)
        self.assertIn("INCONCLUSIVE", summary)
        self.assertIn("[3300, 3700]", summary)

    def test_material_regression_with_separated_bounds_fails(self):
        before, after = report(), report()
        measurement(before["Benchmarks"][0], 3500, 3300, 3700)
        measurement(after["Benchmarks"][0], 4500, 4400, 4600)
        self.assertIn("throughput loss exceeds 5%", self.compare(before, after)[0])

    def test_baseline_drift_is_inconclusive_but_sustained_regression_fails(self):
        before, candidate, after = report(), report(), report()
        measurement(candidate["Benchmarks"][0], 1100, 1090, 1110)
        measurement(after["Benchmarks"][0], 1080, 1070, 1090)
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        last = gate.load_reports(self.write("after", after))
        failures, summary = gate.compare(first, current, baseline_after=last)
        self.assertEqual([], failures)
        self.assertIn("INCONCLUSIVE", summary)
        self.assertIn("[1070, 1090]", summary)
        failures, _ = gate.compare(last, current, baseline_after=first)
        self.assertEqual([], failures)
        failures, _ = gate.compare(first, current, baseline_after=first)
        self.assertTrue(any("throughput loss" in failure for failure in failures))

    def test_baseline_repeat_cannot_hide_allocation_or_startup_failure(self):
        before, candidate, after = report(), report(), report()
        candidate["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 1
        after["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 64
        measurement(next(case for case in candidate["Benchmarks"] if case["Type"] == "StartupBenchmarks"), gate.STARTUP_LIMIT_NS)
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        last = gate.load_reports(self.write("after", after))
        for initial, repeat in ((first, last), (last, first)):
            failures, _ = gate.compare(initial, current, baseline_after=repeat)
            self.assertTrue(any("allocations increased" in failure for failure in failures))
            self.assertTrue(any("not under 50 ms" in failure for failure in failures))

    def test_repeated_baseline_keeps_exact_five_percent_threshold(self):
        first = gate.load_reports(self.write("before", report()))
        last = report()
        measurement(last["Benchmarks"][0], 1200)
        repeated = gate.load_reports(self.write("repeat", last))
        candidate = report()
        for excess, should_fail in ((0, False), (1, True)):
            measurement(candidate["Benchmarks"][0], 1200 / 0.95 + excess)
            current = gate.load_reports(self.write("candidate", candidate))
            failures, summary = gate.compare(first, current, baseline_after=repeated)
            self.assertEqual(should_fail, any("throughput loss" in failure for failure in failures))
            if not should_fail:
                self.assertIn("INCONCLUSIVE", summary)

    def test_baseline_repeat_must_have_matching_cases_and_metadata(self):
        cases = gate.load_reports(self.write("before", report()))
        repeat = dict(cases)
        identity = next(iter(repeat))
        repeat.pop(identity)
        with self.assertRaisesRegex(gate.GateError, "case sets differ"):
            gate.compare(cases, cases, baseline_after=repeat)
        repeat = dict(cases)
        repeat[identity] = gate.replace(repeat[identity], method="OtherMethod")
        with self.assertRaisesRegex(gate.GateError, "metadata differs"):
            gate.compare(cases, cases, baseline_after=repeat)

    def test_cli_requires_requested_baseline_repeat_and_keeps_regressions_blocking(self):
        before = self.write("before", report())
        candidate = report()
        measurement(candidate["Benchmarks"][0], 2000)
        current = self.write("candidate", candidate)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(0, gate.main(["compare", str(before), str(before), "--baseline-after", str(before)]))
            self.assertEqual(1, gate.main(["compare", str(before), str(current), "--baseline-after", str(before)]))
            self.assertEqual(1, gate.main(["compare", str(before), str(before), "--baseline-after", str(self.root / "missing")]))

    def test_finite_negative_lower_bound_is_accepted(self):
        data = report()
        measurement(data["Benchmarks"][0], 1000, -500, 2500)
        self.assertEqual([], self.compare(data, data))

    def test_missing_invalid_or_reversed_confidence_bounds_fail(self):
        for interval in (None, {}, {"Lower": float("nan"), "Upper": 1000},
                         {"Lower": 1000, "Upper": float("inf")},
                         {"Lower": 1001, "Upper": 2000}, {"Lower": 0, "Upper": 999}):
            with self.subTest(interval=interval):
                data = report()
                data["Benchmarks"][0]["Statistics"]["ConfidenceInterval"] = interval
                with self.assertRaises(gate.GateError):
                    gate.load_reports(self.write("invalid-bounds", data))

    def test_one_byte_allocation_increase_fails(self):
        after = report()
        after["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 1
        self.assertIn("allocations increased", self.compare(report(), after)[0])

    def test_fractional_allocation_rounding_passes(self):
        after = report()
        after["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 0.5
        self.assertEqual([], self.compare(report(), after))

    def test_startup_allocations_are_reported_without_a_per_message_budget(self):
        before, candidate, after = report(), report(), report()
        for data, allocated in ((before, 94640), (candidate, 96560), (after, 95520)):
            startup = next(case for case in data["Benchmarks"] if case["Type"] == "StartupBenchmarks")
            startup["Memory"]["BytesAllocatedPerOperation"] = allocated
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        last = gate.load_reports(self.write("after", after))
        for repeat in (None, last):
            failures, summary = gate.compare(first, current, baseline_after=repeat)
            self.assertEqual([], failures)
            self.assertIn("94640", summary)
            self.assertIn("96560", summary)
            self.assertIn("startup allocations are informational", summary)
        measurement(next(case for case in candidate["Benchmarks"] if case["Type"] == "StartupBenchmarks"), gate.STARTUP_LIMIT_NS)
        current = gate.load_reports(self.write("candidate", candidate))
        failures, _ = gate.compare(first, current, baseline_after=last)
        self.assertTrue(any("not under 50 ms" in failure for failure in failures))

    def test_self_test_injects_per_message_allocations_when_startup_is_first(self):
        data = report()
        data["Benchmarks"].sort(key=lambda case: case["Type"] != "StartupBenchmarks")
        cases = gate.load_reports(self.write("startup-first", data))
        with contextlib.redirect_stdout(io.StringIO()):
            gate.self_test(cases)

    def test_startup_absolute_budget_fails_even_when_baseline_is_equal(self):
        data = report()
        measurement(next(case for case in data["Benchmarks"] if case["Type"] == "StartupBenchmarks"), gate.STARTUP_LIMIT_NS)
        self.assertTrue(any("not under 50 ms" in failure for failure in self.compare(data, data)))

    def test_slow_baseline_does_not_block_self_test_or_fixed_candidate(self):
        for startup_ns in (gate.STARTUP_LIMIT_NS, gate.STARTUP_LIMIT_NS * 2):
            with self.subTest(startup_ns=startup_ns):
                data = report()
                measurement(next(case for case in data["Benchmarks"] if case["Type"] == "StartupBenchmarks"), startup_ns)
                baseline = self.write("slow-startup", data)
                candidate = self.write("fixed-startup", report())
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(0, gate.main(["self-test", str(baseline)]))
                    self.assertEqual(0, gate.main(["compare", str(baseline), str(candidate)]))
                    self.assertEqual(1, gate.main(["compare", str(baseline), str(baseline)]))
                    self.assertEqual(1, gate.main(["compare", str(candidate), str(baseline)]))

    def test_missing_required_method_fails(self):
        data = report()
        data["Benchmarks"].pop()
        with self.assertRaisesRegex(gate.GateError, "Required benchmark methods missing"):
            gate.load_reports(self.write("missing", data))

    def test_changed_parameter_case_fails(self):
        after = report()
        after["Benchmarks"][0]["FullName"] += "(Concurrency: 16)"
        with self.assertRaisesRegex(gate.GateError, "case sets differ"):
            self.compare(report(), after)

    def test_duplicate_case_fails(self):
        data = report()
        data["Benchmarks"].append(copy.deepcopy(data["Benchmarks"][0]))
        with self.assertRaisesRegex(gate.GateError, "Duplicate benchmark case"):
            gate.load_reports(self.write("duplicate", data))

    def test_invalid_missing_or_failed_measurements_fail_closed(self):
        for value in (None, -1, float("nan"), float("inf"), True, "100"):
            for section, field in (("Statistics", "Mean"), ("Memory", "BytesAllocatedPerOperation")):
                with self.subTest(value=value, section=section):
                    data = report()
                    data["Benchmarks"][0][section][field] = value
                    with self.assertRaises(gate.GateError):
                        gate.load_reports(self.write("invalid", data))
        data = report()
        data["Benchmarks"][0]["Statistics"] = None
        with self.assertRaises(gate.GateError):
            gate.load_reports(self.write("failed", data))

    def test_empty_or_absent_report_fails(self):
        with self.assertRaisesRegex(gate.GateError, "No BenchmarkDotNet"):
            gate.load_reports(self.root)
        with self.assertRaisesRegex(gate.GateError, "no benchmark measurements"):
            gate.load_reports(self.write("empty", {"Benchmarks": []}))

    def test_cli_proves_unchanged_passes_and_real_regression_exits_one(self):
        before = self.write("before", report())
        after = report()
        measurement(after["Benchmarks"][0], 2000)
        candidate = self.write("candidate", after)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(0, gate.main(["compare", str(before), str(before)]))
            self.assertEqual(1, gate.main(["compare", str(before), str(candidate)]))
            self.assertEqual(0, gate.main(["self-test", str(before)]))


class BenchmarkVersionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.candidate, self.baseline = root / "candidate", root / "baseline"
        for checkout, version in ((self.candidate, "0.15.8"), (self.baseline, "0.15.7")):
            (checkout / pin_benchmark_version.PROJECT).parent.mkdir(parents=True)
            (checkout / pin_benchmark_version.PROJECT).write_text(
                '<Project><ItemGroup><PackageReference Include="BenchmarkDotNet" />'
                '<PackageReference Include="Native.Client" /></ItemGroup></Project>')
            (checkout / "Directory.Packages.props").write_text(
                f'<Project><ItemGroup><PackageVersion Include="BenchmarkDotNet" Version="{version}" />'
                '<PackageVersion Include="Native.Client" Version="1.0.0" /></ItemGroup></Project>')

    def test_candidate_runner_version_pinned_without_changing_production_versions(self):
        original_central = (self.baseline / "Directory.Packages.props").read_bytes()
        original_candidate = (self.candidate / pin_benchmark_version.PROJECT).read_bytes()
        self.assertEqual("0.15.8", pin_benchmark_version.pin_version(self.candidate, self.baseline))
        references = ET.parse(self.baseline / pin_benchmark_version.PROJECT).findall(".//PackageReference")
        self.assertEqual("0.15.8", references[0].get("VersionOverride"))
        self.assertIsNone(references[1].get("VersionOverride"))
        self.assertEqual(original_central, (self.baseline / "Directory.Packages.props").read_bytes())
        self.assertEqual(original_candidate, (self.candidate / pin_benchmark_version.PROJECT).read_bytes())

    def test_explicit_candidate_override_wins_over_central_version(self):
        project = self.candidate / pin_benchmark_version.PROJECT
        project.write_text(project.read_text().replace('Include="BenchmarkDotNet"', 'Include="BenchmarkDotNet" VersionOverride="0.16.0"'))
        self.assertEqual("0.16.0", pin_benchmark_version.pin_version(self.candidate, self.baseline))

    def test_missing_or_unresolved_candidate_version_fails_without_baseline_change(self):
        original = (self.baseline / pin_benchmark_version.PROJECT).read_bytes()
        for attribute in ('', ' Version="$(BenchmarkVersion)"'):
            (self.candidate / "Directory.Packages.props").write_text(
                f'<Project><PackageVersion Include="BenchmarkDotNet"{attribute} /></Project>')
            with self.assertRaisesRegex(ValueError, "literal package version"):
                pin_benchmark_version.pin_version(self.candidate, self.baseline)
            self.assertEqual(original, (self.baseline / pin_benchmark_version.PROJECT).read_bytes())

    def test_missing_reference_or_same_checkout_fails(self):
        with self.assertRaisesRegex(ValueError, "separate checkouts"):
            pin_benchmark_version.pin_version(self.candidate, self.candidate)
        (self.baseline / pin_benchmark_version.PROJECT).write_text("<Project />")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            pin_benchmark_version.pin_version(self.candidate, self.baseline)


if __name__ == "__main__":
    unittest.main()
