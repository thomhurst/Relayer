"""Suggest the version of main's draft release: one patch above the latest released version.

Released versions are the version tags on the pushed commit's history: a published GitHub release
creates its tag, and versions released before drafts were introduced are plain tags. Tags that are
not release versions (see verify_release.py) are ignored.
"""

import json
import os
import subprocess
import sys

from verify_release import release_version

# MinVerMinimumMajorMinor in Directory.Build.props: the version before any release.
FIRST_VERSION = "0.1.0"


def precedence(version):
    """Sort key implementing SemVer 2.0.0 precedence for versions verify_release accepts."""
    core, _, prerelease = version.partition("-")
    major, minor, patch = (int(part) for part in core.split("."))
    if not prerelease:
        # A release sorts above every prerelease of the same version.
        return (major, minor, patch, 1, ())
    identifiers = tuple((0, int(part), "") if part.isdigit() else (1, 0, part)
                        for part in prerelease.split("."))
    return (major, minor, patch, 0, identifiers)


def suggest(tags):
    """Return (suggested version, latest release tag or None) for the given tag names.

    After a release X.Y.Z the suggestion is X.Y.(Z+1). After a prerelease X.Y.Z-id it is X.Y.Z,
    the release that prerelease leads to. The maintainer chooses any other version by editing the
    draft's tag before publishing.
    """
    versions = {}
    for tag in tags:
        try:
            versions[release_version(tag)] = tag
        except ValueError:
            continue
    if not versions:
        return FIRST_VERSION, None
    latest = max(versions, key=precedence)
    core, _, prerelease = latest.partition("-")
    if prerelease:
        return core, versions[latest]
    major, minor, patch = core.split(".")
    return f"{major}.{minor}.{int(patch) + 1}", versions[latest]


def is_greater(version, other):
    """Whether one version accepted by verify_release has higher SemVer precedence than another."""
    return precedence(version) > precedence(other)


def kept_override(draft_tag, suggested):
    """The draft's tag when the maintainer set it to a valid version above the suggestion, else None.

    Later pushes then keep that version; setting the tag back to the suggestion or lower returns
    the draft to the default.
    """
    try:
        version = release_version(draft_tag)
    except ValueError:
        return None
    return draft_tag if is_greater(version, suggested) else None


def owned_drafts(pages, marker):
    """The ids of the drafts carrying the marker, newest first, from `gh api --paginate --slurp`."""
    drafts = [release for page in pages for release in page
              if release.get("draft") and marker in (release.get("body") or "")]
    drafts.sort(key=lambda release: (release.get("created_at") or "", release["id"]), reverse=True)
    return [release["id"] for release in drafts]


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "drafts":
        # Reads the release pages on stdin and prints the owned drafts' ids, newest first.
        print(" ".join(str(draft) for draft in owned_drafts(json.load(sys.stdin), sys.argv[2])))
        return
    if len(sys.argv) == 4 and sys.argv[1] == "keep":
        # Prints the draft's tag when it should be kept, or nothing.
        print(kept_override(sys.argv[2], sys.argv[3]) or "")
        return
    tags = subprocess.check_output(["git", "tag", "--merged", "HEAD", "--list", "v*"], text=True).split()
    version, previous = suggest(tags)
    print(f"Suggested version {version} (latest release tag: {previous or 'none'})")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"version={version}\nprevious_tag={previous or ''}\n")


if __name__ == "__main__":
    main()
