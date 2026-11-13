import copy
import unittest

from verify_retained_ci import verify


class RetainedCiTests(unittest.TestCase):
    def setUp(self):
        self.commit = "a" * 40
        self.head = "b" * 40
        self.base = "c" * 40
        self.source = {"path": ".github/workflows/ci.yml@refs/pull/665/merge",
                       "event": "pull_request", "head_sha": self.head,
                       "pull_requests": [{"head": {"sha": self.head},
                                          "base": {"sha": self.base}}]}
        self.git_commit = {"sha": self.commit,
                           "parents": [{"sha": self.base}, {"sha": self.head}]}

    def test_accepts_pr_merge_and_workflow_ref_suffix(self):
        verify(self.source, self.commit, self.git_commit)

    def test_rejects_unrelated_merge_and_changed_run_association(self):
        for key in ("base", "head"):
            with self.subTest(key=key):
                source = copy.deepcopy(self.source)
                source["pull_requests"][0][key]["sha"] = "d" * 40
                with self.assertRaises(ValueError):
                    verify(source, self.commit, self.git_commit)

    def test_rejects_head_checkout_instead_of_tested_merge(self):
        with self.assertRaises(ValueError):
            verify(self.source, self.head, {"sha": self.head, "parents": []})

    def test_non_pr_requires_direct_equality(self):
        source = {**self.source, "event": "push", "head_sha": self.commit}
        verify(source, self.commit, self.git_commit)
        with self.assertRaises(ValueError):
            verify({**source, "head_sha": self.head}, self.commit, self.git_commit)

    def test_rejects_wrong_workflow_or_git_object(self):
        with self.assertRaises(ValueError):
            verify({**self.source, "path": ".github/workflows/other.yml"}, self.commit, self.git_commit)
        with self.assertRaises(ValueError):
            verify(self.source, self.commit, {**self.git_commit, "sha": self.head})


if __name__ == "__main__":
    unittest.main()
