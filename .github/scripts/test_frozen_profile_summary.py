import copy
import json
from pathlib import Path
import tempfile
import unittest

from frozen_profile_summary import summarize


class ProfileSummaryTests(unittest.TestCase):
    def setUp(self):
        self.records = [dict(kind="measurement", processId=1, startMilliseconds=1,
                             stopMilliseconds=2, startThreadId=10, stopThreadId=20,
                             excludedSamples=5, samplesWithoutStacks=0),
                        dict(kind="allocation", ThreadId=10, Type="Message", Stack="Allocate\nReceive",
                             Samples=2, SampledBytes=100),
                        dict(kind="allocation", ThreadId=30, Type="Message", Stack="Allocate\nReceive",
                             Samples=3, SampledBytes=200),
                        dict(kind="allocation", ThreadId=30, Type="Message", Stack="Allocate\nSend",
                             Samples=1, SampledBytes=50)]

    def run_summary(self, records):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "profile.jsonl"
            path.write_text("\n".join(json.dumps(record) for record in records))
            return summarize(path)

    def test_preserves_weights_threads_and_complete_stack_identity(self):
        result = self.run_summary(self.records)
        self.assertEqual((result["samples"], result["sampledBytes"]), (6, 350))
        self.assertEqual(result["owners"][0]["threadIds"], [10, 30])
        self.assertEqual(result["owners"][0]["samples"], 5)
        self.assertEqual(len(result["owners"]), 2)
        self.assertEqual([thread["emittedBoundaryMarker"] for thread in result["threads"]], [True, False])

    def test_rejects_incomplete_or_corrupt_profiles(self):
        variants = [[], self.records[:1], self.records[1:], self.records + [self.records[1]],
                    self.records + [self.records[0]]]
        for record, field, value in [(0, "samplesWithoutStacks", 1), (0, "stopMilliseconds", 1),
                                     (0, "startMilliseconds", float("nan")),
                                     (1, "Samples", True), (1, "SampledBytes", -1),
                                     (1, "ThreadId", 0), (1, "Stack", ""), (1, "Type", "")]:
            variant = copy.deepcopy(self.records)
            variant[record][field] = value
            variants.append(variant)
        for variant in variants:
            with self.subTest(records=variant), self.assertRaises(ValueError):
                self.run_summary(variant)


if __name__ == "__main__":
    unittest.main()
