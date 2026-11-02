"""Keep dependency updates inside the repository's draft review lifecycle."""

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DependencyReviewPolicyTests(unittest.TestCase):
    def test_renovate_cannot_merge_or_arm_auto_merge(self):
        config = json.loads((ROOT / 'renovate.json').read_text())
        policy = {'automerge': False, 'platformAutomerge': False, 'draftPR': True}
        for key, value in policy.items():
            with self.subTest(key=key):
                self.assertIs(config.get(key), value)

        # Inherited package rules run after repository defaults. The final local
        # catch-all must override those too, including minor, digest and pin updates.
        final_rule = config['packageRules'][-1]
        self.assertEqual(final_rule['matchPackageNames'], ['*'])
        self.assertEqual(
            {key for key in final_rule if key.startswith('match')},
            {'matchPackageNames'},
        )
        for key, value in policy.items():
            with self.subTest(rule_key=key):
                self.assertIs(final_rule.get(key), value)

    def test_claude_reviews_dependency_branches_and_bot_actors(self):
        workflow = (ROOT / '.github/workflows/claude-code-review.yml').read_text()
        condition = re.search(
            r'^    if: \|\n(.*?)\n    runs-on:', workflow, re.MULTILINE | re.DOTALL,
        ).group(1)
        # Only Claude's own events are excluded. No author or branch exemption
        # may skip Renovate (including its human-account runner) or Dependabot.
        self.assertEqual(
            ' '.join(condition.split()),
            "github.event_name == 'workflow_dispatch' || github.actor != 'claude[bot]'",
        )
        allowed = re.search(r'^          allowed_bots: "([^"]+)"$', workflow, re.MULTILINE)
        bots = {actor.strip() for actor in allowed.group(1).split(',')}
        self.assertTrue({'dependabot[bot]', 'renovate[bot]'} <= bots)
        self.assertIn('types: [opened, synchronize, reopened]', workflow)

    def test_renovate_config_changes_run_policy_tests(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        tooling_filter = workflow.split('            tooling:\n', 1)[1].split('            website:', 1)[0]
        self.assertIn("'renovate.json'", tooling_filter)
        self.assertIn("python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v", workflow)


if __name__ == '__main__':
    unittest.main()
