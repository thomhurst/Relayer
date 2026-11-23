import unittest

from release_draft import suggest


class SuggestVersionTests(unittest.TestCase):
    def test_one_patch_above_latest_release(self):
        self.assertEqual(suggest(["v0.1.0", "v0.1.4", "v0.2.0", "v0.1.10"]), ("0.2.1", "v0.2.0"))
        self.assertEqual(suggest(["v0.1.9", "v0.1.10"]), ("0.1.11", "v0.1.10"))

    def test_first_release(self):
        self.assertEqual(suggest([]), ("0.1.0", None))

    def test_ignores_tags_that_are_not_release_versions(self):
        tags = ["v0.2.0", "performance-evidence-555", "v9", "v1.0.0+build", "v01.0.0", "1.5.0"]
        self.assertEqual(suggest(tags), ("0.2.1", "v0.2.0"))

    def test_prerelease_suggests_the_release_it_leads_to(self):
        self.assertEqual(suggest(["v0.2.0", "v1.0.0-rc.1"]), ("1.0.0", "v1.0.0-rc.1"))
        self.assertEqual(suggest(["v1.0.0-rc.1", "v1.0.0"]), ("1.0.1", "v1.0.0"))

    def test_prerelease_precedence(self):
        tags = ["v1.0.0-alpha", "v1.0.0-alpha.1", "v1.0.0-alpha.beta", "v1.0.0-beta.2", "v1.0.0-beta.11"]
        self.assertEqual(suggest(tags), ("1.0.0", "v1.0.0-beta.11"))
        self.assertEqual(suggest(["v1.0.0-rc.1", "v1.0.0-alpha.beta"])[1], "v1.0.0-rc.1")
        self.assertEqual(suggest(["v1.0.0-2", "v1.0.0-alpha"])[1], "v1.0.0-alpha")


if __name__ == "__main__":
    unittest.main()
