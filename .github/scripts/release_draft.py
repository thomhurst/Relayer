"""Suggest the version of main's draft release: one patch above the latest released version.

Released versions are the version tags on the pushed commit's history: a published GitHub release
creates its tag, and versions released before drafts were introduced are plain tags. Tags that are
not release versions (see verify_release.py) are ignored.
"""

import os
import subprocess

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


def main():
    tags = subprocess.check_output(["git", "tag", "--merged", "HEAD", "--list", "v*"], text=True).split()
    version, previous = suggest(tags)
    print(f"Suggested version {version} (latest release tag: {previous or 'none'})")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"version={version}\nprevious_tag={previous or ''}\n")


if __name__ == "__main__":
    main()
