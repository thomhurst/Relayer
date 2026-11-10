import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_frozen_driver import LAUNCHES, summarize


class DriverSummaryTests(unittest.TestCase):
    def run_summary(self, change=None, corrupt_log=None):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            protocol = {"launches": LAUNCHES, "exact_driver": True, "instrumented": True,
                        "warmup_batches": 4096, "measured_batches": 32768, "messages_per_batch": 8}
            result = {"instrumentation": "exact-driver-calling-thread", "warmupBatches": 4096,
                      "measuredBatches": 32768, "messages": 262144, "runtime": ".NET 10.0.14", "arch": "X64",
                      "bytes": 26214400, "bytesPerMessage": 100,
                      "invocation": {"Invocations": 32768, "Bytes": 3145728,
                                     "Histogram": [{"Bytes": 96, "Invocations": 32768}]},
                      "completion": {"Invocations": 32768, "Bytes": 0,
                                     "Histogram": [{"Bytes": 0, "Invocations": 32768}]}}
            for index, side in enumerate(LAUNCHES, 1):
                record = copy.deepcopy(result)
                record["image"] = f"/inputs/performance-builds/{side}/builds/net10.0/Relayer.Benchmarks-DefaultJob-1/bin/Release/net10.0"
                if change and index == 2:
                    change(record, protocol)
                (root / f"{index}-{side}.log").write_text(json.dumps(record) + "\n")
            (root / "protocol.json").write_text(json.dumps(protocol))
            if corrupt_log is not None:
                (root / "2-candidate.log").write_text(corrupt_log)
            return summarize(root)

    def test_preserves_every_inclusive_and_regional_result(self):
        report = self.run_summary()
        self.assertEqual([record["side"] for record in report["launches"]], LAUNCHES)
        self.assertEqual(report["launches"][1]["regions"]["invocation"]["bytes_per_message"], 12)
        self.assertEqual(report["launches"][1]["bytes_per_message"], 100)

    def test_rejects_invalid_evidence(self):
        changes = [
            lambda r, p: r["invocation"].update(Bytes=1),
            lambda r, p: r["completion"].update(Invocations=32767),
            lambda r, p: r["completion"]["Histogram"][0].update(Invocations=32767),
            lambda r, p: r.update(bytesPerMessage=float("nan")),
            lambda r, p: r.update(bytes=True),
            lambda r, p: r.update(bytesPerMessage=101),
            lambda r, p: r.update(bytes=262144, bytesPerMessage=1),
            lambda r, p: r["completion"]["Histogram"].append({"Bytes": 0, "Invocations": 0}),
            lambda r, p: r.update(image=r["image"].replace("candidate", "baseline")),
            lambda r, p: r.update(arch="Arm64"),
            lambda r, p: p.update(exact_releases=True),
            lambda r, p: p.update(measured_batches=1),
            lambda r, p: r.update(measuredBatches=1),
            lambda r, p: r.update(measuredBatches=32768.0),
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.run_summary(change)

    def test_rejects_missing_partial_or_multiple_results(self):
        for text in ("", "{}\nLaunch failed\n", "{}\n{}\n", "{broken"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.run_summary(corrupt_log=text)


if __name__ == "__main__":
    unittest.main()
