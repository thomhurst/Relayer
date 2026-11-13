import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from frozen_core_crossover import inventory, prepare, run
from test_frozen_bdn_control import COMPLETE
import test_frozen_bdn_control as fixtures


class CrossoverTests(unittest.TestCase):
    def fixture(self, root, same=False):
        builds = root / "performance-builds"
        for side in ("baseline", "candidate"):
            _, runner = fixtures.OriginalRunnerTests().fixture(builds / side)
            (runner.parent / "Relayer.dll").write_bytes(b"same" if same else side.encode())
            (runner.parent / "Relayer.pdb").write_bytes(side.encode())
        return builds

    def test_only_core_dll_changes_and_originals_remain_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            builds = self.fixture(root)
            before = inventory(builds)
            with patch("frozen_core_crossover.verify", return_value="commit"):
                _, images = prepare(builds, root / "prepared")
            self.assertEqual(inventory(builds), before)
            for name, image in images.items():
                engine, core = name.split("-engine-")
                core = core.removesuffix("-core")
                self.assertEqual((image["directory"] / "Relayer.dll").read_bytes(), core.encode())
                self.assertEqual((image["directory"] / "Relayer.pdb").read_bytes(), engine.encode())

    def test_identical_core_images_do_not_create_a_false_intervention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            builds = self.fixture(root, same=True)
            with patch("frozen_core_crossover.verify", return_value="commit"):
                with self.assertRaisesRegex(ValueError, "Expected only"):
                    prepare(builds, root / "prepared")

    def test_input_failure_prevents_launch(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch("frozen_core_crossover.verify", side_effect=ValueError("bad input")), \
             patch("frozen_core_crossover.subprocess.run") as execute:
            with self.assertRaisesRegex(ValueError, "bad input"):
                run(Path(directory), Path(directory) / "output")
            execute.assert_not_called()

    def test_reciprocal_brackets_preserve_failure_and_all_other_launches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            calls = []
            def execute(command, **kwargs):
                calls.append(command)
                if len(calls) == 2:
                    raise subprocess.CalledProcessError(1, command)
                kwargs["stdout"].write(COMPLETE)
            with patch("frozen_core_crossover.verify", return_value="commit"), \
                 patch("frozen_core_crossover.subprocess.run", side_effect=execute):
                with self.assertRaisesRegex(SystemExit, "all outcomes retained"):
                    run(root, root / "output")
            outcomes = json.loads((root / "output/outcomes.json").read_text())
            self.assertEqual(len(calls), 8)
            self.assertEqual(calls[0], calls[3])
            self.assertEqual(calls[1], calls[2])
            self.assertEqual(calls[4], calls[7])
            self.assertEqual(calls[5], calls[6])
            self.assertIn("error", outcomes[1])
            self.assertNotIn("bytesPerMessage", outcomes[1])
            self.assertEqual(outcomes[-1]["operations"], 65536)

    def test_detects_image_mutation_by_a_launched_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            def execute(command, **kwargs):
                (kwargs["cwd"] / "Relayer.dll").write_bytes(b"changed")
                kwargs["stdout"].write(COMPLETE)
            with patch("frozen_core_crossover.verify", return_value="commit"), \
                 patch("frozen_core_crossover.subprocess.run", side_effect=execute):
                with self.assertRaisesRegex(ValueError, "changed before execution"):
                    run(root, root / "output")
            outcomes = json.loads((root / "output/outcomes.json").read_text())
            self.assertIn("changed during execution", outcomes[0]["error"])
