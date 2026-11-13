import copy
import unittest

from read_parallel_receive_allocations import correlate_parallel


class ParallelReceiveAllocationTests(unittest.TestCase):
    def setUp(self):
        self.report = {"windows": [{"measured": 1024} for _ in range(3000)]}
        window = "Window { Bytes = 1024, CallerBytes = 1024, WorkerBytes = 0, Gen0 = 0, Gen1 = 0, Gen2 = 0 }"
        self.lines = [f"window={index}; scoped={window}; consumer={window}; receive={window}" for index in range(1000)]

    def test_correlates_indented_colored_tunit_echoes(self):
        log = "\n".join(self.lines) + "\n\x1b[32m" + "\n".join("    " + line for line in self.lines) + "\x1b[m\n"
        result = correlate_parallel(self.report, log)
        self.assertEqual(result["test_output"]["unique_triples"], 1000)
        self.assertEqual(result["test_output"]["rows_including_echoes"], 2000)

    def test_retains_unfavorable_window_without_requiring_equality(self):
        self.lines[305] = self.lines[305].replace("consumer=Window { Bytes = 1024", "consumer=Window { Bytes = 13384")
        self.report["windows"][305 * 3 + 1]["measured"] = 13384
        result = correlate_parallel(self.report, "\n".join(self.lines))
        self.assertEqual(result["windows"][916]["measured"], 13384)

    def test_rejects_conflicting_echo_instead_of_selecting_favorable_copy(self):
        log = "\n".join(self.lines + [self.lines[305].replace("consumer=Window { Bytes = 1024", "consumer=Window { Bytes = 13384")])
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            correlate_parallel(self.report, log)

    def test_rejects_missing_or_out_of_range_windows(self):
        for lines in (self.lines[:-1], self.lines + [self.lines[0].replace("window=0;", "window=1000;")]):
            with self.subTest(length=len(lines)), self.assertRaisesRegex(ValueError, "1000"):
                correlate_parallel(copy.deepcopy(self.report), "\n".join(lines))

    def test_rejects_checkpoint_from_a_different_counter_execution(self):
        self.report["windows"][916]["measured"] = 13384
        with self.assertRaisesRegex(ValueError, "differs"):
            correlate_parallel(self.report, "\n".join(self.lines))

    def test_rejects_incomplete_profiler_capture(self):
        self.report["windows"].pop()
        with self.assertRaisesRegex(ValueError, "3000"):
            correlate_parallel(self.report, "\n".join(self.lines))


if __name__ == "__main__":
    unittest.main()
