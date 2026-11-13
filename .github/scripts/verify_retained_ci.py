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
        if not any(
            source["head_sha"] == pr["head"]["sha"]
            and parents == [pr["base"]["sha"], pr["head"]["sha"]]
            for pr in source["pull_requests"]
        ):
            raise ValueError("Retained merge parents do not match the CI run's PR base/head")
    elif source["head_sha"] != commit:
        raise ValueError("Retained commit does not match the selected CI run")


if __name__ == "__main__":
    verify(json.loads(Path(sys.argv[1]).read_text()),
           Path(sys.argv[2]).read_text().strip(),
           json.loads(Path(sys.argv[3]).read_text()))
