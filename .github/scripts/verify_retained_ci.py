"""Bind retained CI inputs to the selected run, including PR merge checkouts."""
import json
from pathlib import Path
import re
import sys


def verify(source, commit, git_commit):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Invalid retained commit")
    if source["path"].split("@", 1)[0] != ".github/workflows/ci.yml":
        raise ValueError("Selected run is not the CI workflow")
    if git_commit["sha"] != commit:
        raise ValueError("Git commit response does not match retained commit")
    if source["event"] == "pull_request":
        parents = [parent["sha"] for parent in git_commit["parents"]]
        # The run's head_sha is fixed, but pull_requests contains the PR's
        # current base/head after subsequent pushes. It cannot authenticate
        # the historical merge's base. Bind the downloaded merge to the
        # selected run's original head instead, preserving parent order.
        if len(parents) != 2 or parents[1] != source["head_sha"]:
            raise ValueError("Retained merge second parent does not match the CI run's head")
    elif source["head_sha"] != commit:
        raise ValueError("Retained commit does not match the selected CI run")


if __name__ == "__main__":
    verify(json.loads(Path(sys.argv[1]).read_text()),
           Path(sys.argv[2]).read_text().strip(),
           json.loads(Path(sys.argv[3]).read_text()))
