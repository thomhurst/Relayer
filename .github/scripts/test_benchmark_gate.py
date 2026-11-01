import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
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

    def test_allocation_drift_is_inconclusive_in_both_baseline_orders(self):
        before, candidate, after = report(), report(), report()
        for data, allocated in ((before, 1025), (candidate, 1020), (after, 1006)):
            data["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] = allocated
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        last = gate.load_reports(self.write("after", after))
        for initial, repeat in ((first, last), (last, first)):
            failures, summary = gate.compare(initial, current, baseline_after=repeat)
            self.assertEqual([], failures)
            self.assertIn("INCONCLUSIVE: observed allocation increase exceeds 0.5 B/op; baseline drift prevents confirmation", summary)
            self.assertIn("1020", summary)
            self.assertIn("1025", summary)
            self.assertIn("1006", summary)

    def test_bracketed_allocations_keep_sub_byte_tolerance_and_confirm_increases(self):
        before, after = report(), report()
        after["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 64
        first = gate.load_reports(self.write("before", before))
        last = gate.load_reports(self.write("after", after))
        identity = next(iter(first))
        for repeat in (first, last):
            largest = max(first[identity].allocated_bytes, repeat[identity].allocated_bytes)
            for excess, should_fail in ((0.5, False), (0.501, True), (1, True)):
                candidate = report()
                candidate["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] = largest + excess
                current = gate.load_reports(self.write("candidate", candidate))
                for initial, repeated in ((first, repeat), (repeat, first)):
                    with self.subTest(repeated_bytes=repeat[identity].allocated_bytes, excess=excess):
                        failures, summary = gate.compare(initial, current, baseline_after=repeated)
                        self.assertEqual(should_fail, any("allocations increased" in failure for failure in failures))
                        self.assertEqual(repeat != first and not should_fail, "INCONCLUSIVE: observed allocation" in summary)

    def test_baseline_repeat_cannot_hide_confirmed_allocation_or_startup_failure(self):
        before, candidate, after = report(), report(), report()
        candidate["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 1
        after["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 0.25
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

    def test_verified_identical_inputs_report_variation_but_keep_startup_budget(self):
        before, candidate = report(), report()
        candidate["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 64
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        failures, summary = gate.compare(first, current, baseline_after=first, identical_inputs=True)
        self.assertEqual([], failures)
        self.assertIn("INCONCLUSIVE: allocation variation with byte-identical comparison inputs", summary)
        self.assertIn("216", summary)
        measurement(candidate["Benchmarks"][0], 2000)
        measurement(next(case for case in candidate["Benchmarks"] if case["Type"] == "StartupBenchmarks"), gate.STARTUP_LIMIT_NS)
        current = gate.load_reports(self.write("candidate", candidate))
        failures, _ = gate.compare(first, current, identical_inputs=True)
        self.assertFalse(any("throughput loss" in failure for failure in failures))
        self.assertTrue(any("not under 50 ms" in failure for failure in failures))

    def test_identical_input_throughput_variation_is_inconclusive_with_all_bounds_visible(self):
        before, candidate, repeated = report(), report(), report()
        measurement(before["Benchmarks"][0], 117.649, 117.104, 118.194)
        measurement(candidate["Benchmarks"][0], 135.996, 135.194, 136.798)
        measurement(repeated["Benchmarks"][0], 121.366, 120.883, 121.849)
        first = gate.load_reports(self.write("before", before))
        current = gate.load_reports(self.write("candidate", candidate))
        last = gate.load_reports(self.write("repeat", repeated))
        for repeat in (None, last):
            failures, summary = gate.compare(first, current, baseline_after=repeat, identical_inputs=True)
            self.assertEqual([], failures)
            self.assertIn("INCONCLUSIVE: throughput variation with byte-identical comparison inputs", summary)
            self.assertIn("[117.104, 118.194]", summary)
            self.assertIn("[135.194, 136.798]", summary)
            if repeat is not None:
                self.assertIn("[120.883, 121.849]", summary)
            failures, _ = gate.compare(first, current, baseline_after=repeat)
            self.assertTrue(any("throughput loss exceeds 5%" in failure for failure in failures))

    def test_identical_inputs_do_not_accept_mismatched_reports(self):
        cases = gate.load_reports(self.write("before", report()))
        with self.assertRaisesRegex(gate.GateError, "case sets differ"):
            gate.compare(cases, {}, identical_inputs=True)

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


class ComparisonInputTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.baseline, self.candidate = self.root / "baseline", self.root / "candidate"
        for checkout in (self.baseline, self.candidate):
            checkout.mkdir()
            subprocess.run(["git", "init", "--quiet", str(checkout)], check=True)
            for name in gate.REQUIRED_INPUTS:
                self.write(checkout, name, "same content")
            subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)

    def write(self, checkout, name, content):
        path = checkout / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def test_only_documentation_and_detector_changes_preserve_identity(self):
        for name in ("docs/design.md", "website/docs/contributing/performance.md",
                     ".github/scripts/benchmark_gate.py", ".github/scripts/test_benchmark_gate.py",
                     ".github/scripts/pin_benchmark_version.py",
                     ".github/workflows/ci.yml", ".github/workflows/performance.yml"):
            self.write(self.candidate, name, "changed")
        self.assertEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))

    def test_source_generator_dependencies_sdk_harness_and_unknown_inputs_disable_identity(self):
        for name in (*gate.REQUIRED_INPUTS, "src/Relayer/Changed.cs", "new-build-input.targets"):
            with self.subTest(name=name):
                path = self.candidate / name
                original = path.read_bytes() if path.exists() else None
                self.write(self.candidate, name, "changed")
                self.assertNotEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)

    def test_agent_workflow_aliases_do_not_enter_measured_builds(self):
        for name in (".claude/skills/issue-pr-loop/SKILL.md",
                     ".claude/skills/issue-pr-loop/references"):
            self.write(self.candidate, name, "agent instruction alias")
        self.assertEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))

    def test_merge_only_tooling_does_not_enter_measured_builds(self):
        for name in (".agents/skills/issue-pr-loop/references/repository.md",
                     "scripts/Assert-PrGreen.ps1", "scripts/AssertPrGreenChecks.ps1",
                     "scripts/Enable-AutoMerge.ps1", "scripts/Test-AssertPrGreenChecks.ps1"):
            with self.subTest(name=name):
                self.write(self.candidate, name, "changed merge tooling")
                self.assertEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))
        self.write(self.candidate, "scripts/unknown-build-input.ps1", "must remain hashed")
        self.assertNotEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))

    def symlink(self, name, target, *, directory=False):
        path = self.candidate / name
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.symlink_to(target, target_is_directory=directory)
        except OSError as error:
            self.skipTest(f"Cannot create symlinks: {error}")
        return path

    def test_agent_workflow_file_and_directory_symlinks_are_excluded(self):
        for checkout in (self.baseline, self.candidate):
            self.write(checkout, ".agents/skills/issue-pr-loop/SKILL.md", "instructions")
            self.write(checkout, ".agents/skills/issue-pr-loop/references/repository.md", "guidance")
        self.symlink(".claude/skills/issue-pr-loop/SKILL.md",
                     "../../../.agents/skills/issue-pr-loop/SKILL.md")
        self.symlink(".claude/skills/issue-pr-loop/references",
                     "../../../.agents/skills/issue-pr-loop/references", directory=True)
        self.assertEqual(gate.input_manifest(self.baseline), gate.input_manifest(self.candidate))

    def test_other_file_directory_and_broken_symlinks_fail_closed(self):
        for target, directory in (("global.json", False), ("src", True), ("missing", False)):
            with self.subTest(target=target):
                path = self.symlink("unknown-link", target, directory=directory)
                try:
                    with self.assertRaisesRegex(gate.GateError, "Cannot verify symlink input"):
                        gate.input_manifest(self.candidate)
                finally:
                    path.unlink()

    def test_missing_inputs_or_non_checkout_root_fail_closed(self):
        (self.candidate / "global.json").unlink()
        with self.assertRaises(gate.GateError):
            gate.input_manifest(self.candidate)
        with self.assertRaises(gate.GateError):
            gate.input_manifest(self.baseline / "src")

    def test_cli_verifies_identity_and_retains_manifest(self):
        before, candidate = report(), report()
        candidate["Benchmarks"][0]["Memory"]["BytesAllocatedPerOperation"] += 1
        measurement(candidate["Benchmarks"][0], 2000)
        first = self.write(self.root, "first/benchmarks-report-full.json", json.dumps(before)).parent
        current = self.write(self.root, "current/benchmarks-report-full.json", json.dumps(candidate)).parent
        manifest = self.root / "inputs.json"
        arguments = ["compare", str(first), str(current), "--baseline-source", str(self.baseline),
                     "--candidate-source", str(self.candidate), "--input-manifest", str(manifest)]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(0, gate.main(arguments))
            self.assertTrue(json.loads(manifest.read_text())["identical"])
            self.write(self.candidate, "Directory.Build.props", "changed")
            self.assertEqual(1, gate.main(arguments))
            self.assertFalse(json.loads(manifest.read_text())["identical"])
            self.assertEqual(1, gate.main(["compare", str(first), str(current), "--baseline-source", str(self.baseline)]))


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

    def test_equal_versions_preserve_byte_identical_harness(self):
        (self.baseline / "Directory.Packages.props").write_bytes((self.candidate / "Directory.Packages.props").read_bytes())
        original = (self.baseline / pin_benchmark_version.PROJECT).read_bytes()
        self.assertEqual("0.15.8", pin_benchmark_version.pin_version(self.candidate, self.baseline))
        self.assertEqual(original, (self.baseline / pin_benchmark_version.PROJECT).read_bytes())

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
