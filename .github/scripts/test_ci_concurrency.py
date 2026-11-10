"""Exercise CI concurrency expressions against event snapshots, including delayed drafts."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / 'workflows/ci.yml'


class CiConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text()
        cls.concurrency = cls.workflow.split('\nconcurrency:\n', 1)[1].split('\njobs:', 1)[0]
        cls.group = re.search(r'^  group: (.+)$', cls.concurrency, re.MULTILINE).group(1)

    def group_for(self, action='', draft=False, number=499, event='pull_request', ref='refs/pull/499/merge'):
        context = {
            'github.event_name': event,
            'github.event.action': action,
            'github.event.pull_request.draft': draft,
            'github.event.pull_request.number': number,
            'github.ref': ref,
        }

        def evaluate(match):
            # Evaluate the workflow's small boolean/string expression subset, rather
            # than repeating its routing logic. Fixtures use only booleans and strings.
            expression = re.sub(r'github\.[a-z_.]+', lambda token: repr(context[token.group()]), match[1])
            expression = expression.replace('&&', ' and ').replace('||', ' or ')
            return str(eval(expression, {'__builtins__': {}}, {}))

        return re.sub(r'\$\{\{ (.*?) \}\}', evaluate, self.group)

    def test_delayed_draft_snapshots_cannot_cancel_or_replace_ready_validation(self):
        ready = self.group_for('ready_for_review')
        for action in ('opened', 'synchronize', 'reopened'):
            with self.subTest(action=action):
                self.assertNotEqual(ready, self.group_for(action, draft=True))

    def test_conversion_to_draft_cancels_validation(self):
        self.assertEqual(self.group_for('ready_for_review'), self.group_for('converted_to_draft', draft=True))
        self.assertRegex(self.concurrency, r'(?m)^  cancel-in-progress: true$')
        self.assertIn('converted_to_draft]', self.workflow)

    def test_ready_updates_cancel_previous_validation_for_only_the_same_pr(self):
        ready = self.group_for('ready_for_review')
        for action in ('opened', 'synchronize', 'reopened'):
            self.assertEqual(ready, self.group_for(action))
        self.assertNotEqual(ready, self.group_for('synchronize', number=500))

    def test_push_and_manual_runs_keep_branch_isolation(self):
        main = self.group_for(event='push', number=None, ref='refs/heads/main')
        self.assertEqual('ci-refs/heads/main', main)
        self.assertEqual(main, self.group_for(event='workflow_dispatch', number=None, ref='refs/heads/main'))
        self.assertNotEqual(main, self.group_for(event='push', number=None, ref='refs/heads/other'))


if __name__ == '__main__':
    unittest.main()
