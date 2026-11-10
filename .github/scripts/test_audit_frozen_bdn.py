import json
from pathlib import Path
import tempfile
import unittest

from audit_frozen_bdn import BENCHMARK, audit, audit_side


class HistoricalGcAuditTests(unittest.TestCase):
    def side(self, root, allocated=96572704):
        root.mkdir(parents=True)
        log = root / "BenchmarkRun.log"
        log.write_text("// Benchmark: EndToEndBenchmarks.ConcurrentRoundTrip: DefaultJob\n"
                       "WorkloadResult 1: 131072 op, 1 ns, 1 ns/op\n"
                       f"// GC:  5 0 0 {allocated} 131072\n// AfterAll\n"
                       "// Benchmark: EndToEndBenchmarks.DirectSend: DefaultJob\n"
                       "// GC:  0 0 0 0 131072\n// AfterAll\n")
        report = root / "results/Relayer.Benchmarks.EndToEndBenchmarks-report-full.json"
        report.parent.mkdir()
        report.write_text(json.dumps({"Benchmarks": [{"FullName": BENCHMARK, "Memory": {
            "TotalOperations": 131072, "BytesAllocatedPerOperation": round(allocated / 131072),
            "Gen0Collections": 5, "Gen1Collections": 0, "Gen2Collections": 0}}]}))
        return log, report

    def test_audits_unrounded_original_delta_and_fingerprints_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory)
            for side, allocated in (("baseline", 95566528), ("candidate", 96572704),
                                    ("baseline-after", 96393456)):
                self.side(artifact / "performance-results" / side, allocated)
            actual = audit(artifact)
            self.assertEqual(actual["candidateDeltaBytesPerMessage"]["baseline-after"], 1.3675537109375)
            self.assertEqual(actual["sides"]["candidate"]["operations"], 131072)
            self.assertEqual(actual["sides"]["candidate"]["bdnRoundedBytesPerMessage"], 737)
            self.assertEqual(len(actual["sides"]["candidate"]["inputs"]), 2)
            self.assertTrue(all(len(item["sha256"]) == 64 for item in actual["sides"]["candidate"]["inputs"]))

    def test_rounds_midpoints_to_even_like_bdn(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "side"
            self.side(root, allocated=2 * 131072 + 65536)
            self.assertEqual(audit_side(root)["bdnRoundedBytesPerMessage"], 2)

    def test_rejects_counter_or_export_disagreement(self):
        for key, value in (("TotalOperations", 16384), ("BytesAllocatedPerOperation", 736),
                           ("Gen0Collections", 0)):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "side"
                _, report = self.side(root)
                data = json.loads(report.read_text())
                data["Benchmarks"][0]["Memory"][key] = value
                report.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "disagree"):
                    audit_side(root)

    def test_rejects_missing_or_ambiguous_original_section(self):
        for mutation in ("missing", "duplicate", "multiple-logs", "wrong-job"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "side"
                log, _ = self.side(root)
                if mutation == "multiple-logs":
                    (root / "other.log").write_text(log.read_text())
                else:
                    text = log.read_text()
                    log.write_text({"missing": "", "duplicate": text + text,
                                    "wrong-job": text.replace("DefaultJob", "OtherJob")}[mutation])
                with self.assertRaises(ValueError):
                    audit_side(root)


if __name__ == "__main__":
    unittest.main()
