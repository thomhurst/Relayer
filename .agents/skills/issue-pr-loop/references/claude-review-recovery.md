# Claude receipt recovery reproduction

This is a read-only reproduction of the GitHub check association failure in [#220](https://github.com/thomhurst/Relayer/issues/220). It does not create a deliberate privileged workflow failure or repeat a model review.

Observed on PR #216 at `c54d4cadee8246891c52833c427cf6eaa96c977b`:

1. [Original run 41446807882, attempt 1](https://github.com/thomhurst/Relayer/actions/runs/41446807882/attempts/1) failed receipt verification with `FileNotFoundError` and posted no verdict. Its event is `pull_request_target`, and its `pull_requests` association includes #216 at this head.
2. [Manual dispatch 41448815382](https://github.com/thomhurst/Relayer/actions/runs/41448815382) succeeded and posted a `CLEAR` verdict for that head. Its event is `workflow_dispatch`, its `head_sha` is the default-branch commit `5e40e0151774f4dada618f7cb4ab276e6a783649`, and `pull_requests` is empty. Before the original retry, #216's rollup still contained the original failed `claude-review`, not this successful dispatch. The dispatch supplied review evidence but did not repair the PR check.
3. [Original run 41446807882, attempt 2](https://github.com/thomhurst/Relayer/actions/runs/41446807882/attempts/2) succeeded after the one diagnosed receipt-delivery retry. It retained check suite `112316872354` and the PR association. It posted `<!-- REVIEW_RUN: 41446807882/2; HEAD: c54d4cadee8246891c52833c427cf6eaa96c977b -->` with `CLEAR`. The PR rollup then reported successful `claude-review` job `125291091709` from the original run.

Inspect the retained attempts and comments with PowerShell:

```powershell
$repo = 'thomhurst/Relayer'
gh api "repos/$repo/actions/runs/41446807882/attempts/1" --jq '{id,event,conclusion,run_attempt,check_suite_id,pull_requests}'
gh api "repos/$repo/actions/runs/41448815382" --jq '{id,event,head_sha,conclusion,run_attempt,check_suite_id,pull_requests}'
gh api "repos/$repo/actions/runs/41446807882/attempts/2" --jq '{id,event,conclusion,run_attempt,check_suite_id,pull_requests}'
gh api "repos/$repo/actions/runs/41446807882/attempts/2/jobs" --jq '.jobs[] | {id,name,conclusion,check_run_url}'
gh api "repos/$repo/issues/216/comments" --paginate --jq '.[] | select(.user.login == "github-actions[bot]" and (.body | startswith("<!-- REVIEW_RUN:"))) | {id,body}'
gh pr view 216 --repo $repo --json headRefOid,statusCheckRollup --jq '{headRefOid,checks:[.statusCheckRollup[] | select(.name == "claude-review") | {conclusion,detailsUrl}]}'
```

The final command shows live state; later pushes can replace the head and rollup. Use the immutable attempt endpoints above for the historical run results. To reproduce the transition on a future genuine delivery failure, capture the PR rollup before recovery, apply [Claude review recovery](repository.md#claude-review-recovery) under the PR lock, and capture it again after completion. Do not dispatch merely to demonstrate the defect. A successful dispatch absent from the rollup must not be inserted into it locally or used to waive the failed check.

Run `pwsh scripts/Test-AssertPrGreenChecks.ps1` for the offline regression: a head-bound dispatched verdict leaves the failed PR check blocking in both merge and auto-merge modes; a later successful PR-associated execution replaces the failure. Run `python -m unittest discover -s .github/scripts -p test_pr_review_comment.py` for the receipt verifier regressions. These tests preserve the existing gates; live GitHub association is covered by the recorded reproduction above.
