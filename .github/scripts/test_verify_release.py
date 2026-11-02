import unittest

from verify_release import release_version, require_ci


class ReleaseTests(unittest.TestCase):
    def test_stable_and_prerelease_versions(self):
        for version in ("0.1.0", "1.2.3", "1.2.3-preview.1", "1.2.3-alpha-beta.0"):
            self.assertEqual(release_version("v" + version), version)

    def test_rejects_invalid_or_ambiguous_versions(self):
        for tag in ("1.2.3", "v1.2", "v01.2.3", "v1.2.3-preview.01", "v1.2.3+build",
                    "v1.2.3-", "v1.2.3\n", "v1.2.3;echo bad"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release_version(tag)

    def test_requires_successful_ci_for_exact_commit_and_app(self):
        check = {"name": "CI Passed", "head_sha": "abc", "app": {"id": 15368},
                 "status": "completed", "conclusion": "success"}
        require_ci([{"check_runs": []}, {"check_runs": [check]}], "abc")
        for change in ({"head_sha": "old"}, {"app": {"id": 1}}, {"status": "in_progress"},
                       {"conclusion": "failure"}, {"conclusion": "skipped"}, {"name": "Build & Test"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                require_ci([{"check_runs": [check | change]}], "abc")
        with self.assertRaises(ValueError):
            require_ci([{"check_runs": []}], "abc")


if __name__ == "__main__":
    unittest.main()
