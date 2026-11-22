"""Guard the release workflow's serial, multi-run queue."""

from pathlib import Path
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / 'workflows/release.yml'


class ReleaseConcurrencyTests(unittest.TestCase):
    def test_release_tags_share_a_queue_that_retains_pending_runs(self):
        workflow = WORKFLOW.read_text(encoding='utf-8')
        concurrency = workflow.split('\nconcurrency:\n', 1)[1].split('\njobs:', 1)[0]
        self.assertRegex(concurrency, r'(?m)^  group: nuget-release$')
        self.assertRegex(concurrency, r'(?m)^  cancel-in-progress: false$')
        self.assertRegex(concurrency, r'(?m)^  queue: max$')
