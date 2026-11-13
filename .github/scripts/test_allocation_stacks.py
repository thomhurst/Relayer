import copy
import unittest

from read_allocation_stacks import validate


class AllocationStackTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {"kind": "frame", "functionId": 11, "name": "Fixture.FillRange"},
            {"kind": "allocation-stack", "index": 0, "thread": 7, "classId": 42,
             "snapshotResult": 0, "objects": 2, "bytes": 48, "frames": [11]},
            {"kind": "stack-coverage", "objects": 2, "bytes": 48, "failures": 0},
        ]
        self.report = {"coverage": {"objects": 2, "bytes": 48}, "windows": [
            {"index": 0, "types": [{"classId": 42, "objects": 2, "bytes": 48}]}]}

    def test_valid_same_execution_thread_and_window(self):
        self.assertEqual(validate(self.rows, self.report)["allocations"][0]["thread"], 7)

    def test_snapshot_failure_and_empty_frames_are_rejected(self):
        for key, value in (("snapshotResult", -1), ("frames", []), ("frames", [99])):
            rows = copy.deepcopy(self.rows)
            rows[1][key] = value
            with self.assertRaises(ValueError):
                validate(rows, self.report)

    def test_wrong_window_class_or_counts_are_rejected(self):
        for key, value in (("index", 1), ("classId", 43), ("bytes", 49), ("objects", 3), ("thread", 0)):
            rows = copy.deepcopy(self.rows)
            rows[1][key] = value
            with self.assertRaises(ValueError):
                validate(rows, self.report)

    def test_missing_or_duplicate_frame_and_coverage_are_rejected(self):
        for rows in (self.rows[1:], [self.rows[0], *self.rows], self.rows[:-1],
                     [*self.rows[:-1], {**self.rows[-1], "failures": 1}],
                     [*self.rows[:-1], {**self.rows[-1], "bytes": 49}]):
            with self.assertRaises(ValueError):
                validate(rows, self.report)

    def test_unresolved_names_remain_explicit(self):
        self.rows[0]["name"] = "<unresolved-function>"
        self.assertEqual(validate(self.rows, self.report)["unresolved_frames"], [11])
