"""Require a version tag on main with successful CI before releasing."""

import json
import os
import re
import subprocess


def release_version(tag):
    # NuGet ignores build metadata. Exclude it so two tags cannot name the same package version.
    number = r"(?:0|[1-9][0-9]*)"
    identifier = rf"(?:{number}|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    if not re.fullmatch(rf"v{number}\.{number}\.{number}(?:-{identifier}(?:\.{identifier})*)?", tag):
        raise ValueError("Release tag must be vMAJOR.MINOR.PATCH with optional SemVer prerelease identifiers.")
    return tag[1:]


def require_ci(pages, commit):
    for page in pages:
        for check in page["check_runs"]:
            if (check["name"] == "CI Passed" and check["head_sha"] == commit
                    and check["app"]["id"] == 15368 and check["status"] == "completed"
                    and check["conclusion"] == "success"):
                return
    raise ValueError("Release commit requires a successful CI Passed check from GitHub Actions.")


def main():
    version = release_version(os.environ["RELEASE_TAG"])
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"], check=True)
    pages = json.loads(subprocess.check_output([
        "gh", "api", "--paginate", "--slurp",
        f"repos/{os.environ['GITHUB_REPOSITORY']}/commits/{commit}/check-runs?per_page=100&filter=latest",
    ], text=True))
    require_ci(pages, commit)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"version={version}\ncommit={commit}\n")


if __name__ == "__main__":
    main()
