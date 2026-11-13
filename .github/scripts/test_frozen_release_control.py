import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_release_control import main


class ReleaseInterventionTests(unittest.TestCase):
    def test_arms_use_existing_profile_launcher_and_child_only_environment(self):
        for arm in ("async", "sync"):
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "capture"
                args = ["control", arm, "inputs", "probe.dll", str(output), "--trace-tool", "trace", "--profile-reader", "reader", "--keep-traces"]
                with patch.dict(os.environ, {"RELAYER_FROZEN_RELEASE_CHANNEL": "inherited"}), patch("sys.argv", args), patch("frozen_release_control.subprocess.run") as run:
                    main()
                    self.assertEqual(os.environ["RELAYER_FROZEN_RELEASE_CHANNEL"], "inherited")
                command = run.call_args.args[0]
                self.assertTrue(command[1].endswith("frozen_concurrent_control.py"))
                self.assertIn("--bdn-await", command)
                self.assertIn("--keep-traces", command)
                self.assertEqual(run.call_args.kwargs["env"]["RELAYER_FROZEN_RELEASE_CHANNEL"], arm)
                metadata = json.loads(output.with_name("capture-intervention.json").read_text())
                self.assertEqual(metadata["allowSynchronousContinuations"], arm == "sync")
                self.assertEqual(metadata["capacity"], 8)

    def test_failed_capture_retains_arm_metadata_and_propagates_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "capture"
            args = ["control", "sync", "inputs", "probe.dll", str(output), "--trace-tool", "trace", "--profile-reader", "reader", "--keep-traces"]
            with patch("sys.argv", args), patch("frozen_release_control.subprocess.run", side_effect=subprocess.CalledProcessError(1, "capture")), self.assertRaises(subprocess.CalledProcessError):
                main()
            self.assertEqual(json.loads(output.with_name("capture-intervention.json").read_text())["arm"], "sync")

    def test_exact_arms_reuse_callback_launcher_without_sampled_options(self):
        for arm in ("async", "sync"):
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "capture"
                args = ["control", arm, "inputs", "probe.dll", str(output), "--allocation-profiler", "profiler.so"]
                with patch("sys.argv", args), patch("frozen_release_control.subprocess.run") as run:
                    main()
                command = run.call_args.args[0]
                self.assertEqual(command[-2:], ["--allocation-profiler", "profiler.so"])
                self.assertNotIn("--trace-tool", command)
                self.assertEqual(run.call_args.kwargs["env"]["RELAYER_FROZEN_RELEASE_CHANNEL"], arm)
                metadata = json.loads(output.with_name("capture-intervention.json").read_text())
                self.assertEqual(metadata["allocationObservation"], "exact CLR callbacks")

    def test_incomplete_or_mixed_instrumentation_rejected_before_launch(self):
        for options in ([], ["--trace-tool", "trace"], ["--allocation-profiler", "profiler.so", "--keep-traces"]):
            with self.subTest(options=options), patch("sys.argv", ["control", "async", "inputs", "probe", "output", *options]), patch("frozen_release_control.subprocess.run") as run, self.assertRaises(SystemExit):
                main()
            run.assert_not_called()

    def test_rejects_unknown_arm_before_launch(self):
        with patch("sys.argv", ["control", "unknown"]), patch("frozen_release_control.subprocess.run") as run, self.assertRaises(SystemExit):
            main()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
