# Relayer

Read [AGENTS.md](../../../../AGENTS.md) for validation, public contracts, and performance acceptance. It owns the zero-allocation pipeline rules, the AOT and dependency policy, and the benchmark rules; review/simplification must preserve them. [docs/design.md](../../../../docs/design.md) and [docs/decisions.md](../../../../docs/decisions.md) are the specification: an issue that conflicts with a recorded decision either follows the decision or updates the record in the same PR.

- Issues carry `phase-*` and `area: *` labels. Within a priority, prefer the earliest phase: later phases build on the core.
- Transport work against a real broker or emulator uses Docker/Testcontainers; stop only resources created for this work.
- Validate narrowly, as [AGENTS.md](../../../../AGENTS.md#build-and-test) describes: run only the test classes that cover the change, in the test project that owns them. Never run the whole test suite or unfiltered benchmarks in a unit; CI on the ready PR is the full run.
- Add a benchmark only when [AGENTS.md](../../../../AGENTS.md#performance-requirements) requires one: the change materially moves steady-state per-message cost. Most bug fixes do not; state the one-line per-message cost in the PR instead.

The mechanics this loop defers to:

| Need | Owner |
|---|---|
| One PR's review cycle: fetch every surface, verify findings, fix, one push, reply, resolve and unresolve threads | [Review cycle](#review-cycle) below; `AGENTS.md` § Pull requests holds the surface and thread rules |
| PR review lifecycle | [draft-review.md](draft-review.md), with this repository's rules in [Draft review lifecycle](#draft-review-lifecycle) below |
| Merge protocol | [Merge gate](#merge-gate) below |
| Out-of-scope filing bar and procedure | `AGENTS.md` § Autonomous self-improvement and § Writing issues |
| Native sub-issue and `blocked_by` writes | [Issue relationships](#issue-relationships) below |
| Worktree location, evidence directory, hazards, cleanup | [Worktrees and evidence](#worktrees-and-evidence) below and [WorktreeLifecycle.md](../../../../scripts/WorktreeLifecycle.md) |
| Issues only the owner approves | [Owner-reserved decisions](#owner-reserved-decisions) below |
| Automated review rounds, draft CI | [Draft review lifecycle](#draft-review-lifecycle) below |

Lock owner variable: `RELAYER_AGENT_LOCK_OWNER_ID` (read by `scripts/AgentLocks.ps1` after `-OwnerId`, before `CODEX_THREAD_ID`).

## Issue queue

- Priority labels are `P0` (critical), `P1` (high), `P2` (medium), and `P3` (low); read them for the `priority:*` tiers in [pick-an-issue.md](pick-an-issue.md). An issue without one comes after `P3`, which is how `scripts/Start-IssuePrLoop.ps1` sorts it.
- There is no `ci` label. Within a priority, prefer issues that block other issues, then clear, smaller tasks with fewer discussion comments.
- Give every issue a triage or discovery unit files one priority label, so the queue orders it, and write it as [Writing issues](../../../../AGENTS.md#writing-issues) describes.

## Owner-reserved decisions

Defer these as `[human]` with `needs-human`. Only the maintainer approves them, and triage never clears the label:

- Trading throughput, latency, CPU, or allocations against each other, including in refactoring or review feedback ([Performance requirements](../../../../AGENTS.md#performance-requirements)).
- Any change to repository rulesets, branch protection, or their bypass lists, and any protection problem `scripts/Enable-AutoMerge.ps1` reports: stop and report it ([Pull requests](../../../../AGENTS.md#pull-requests)).

No other issue class is reserved; the `[human]` list in SKILL.md still applies. A breaking change is allowed on a milestone that permits breaking changes.

## Review cycle

Work under the PR lock.

Inspect all review surfaces, including pagination and replies: REST `pulls/<N>/reviews`, `pulls/<N>/comments`, `issues/<N>/comments` under `repos/{owner}/{repo}`, and GraphQL `reviewThreads` with IDs, resolution state, bodies, authors, and timestamps.

Never infer a reviewer's surface from its identity: Claude often posts its review as a PR conversation comment (`issues/<N>/comments`), while other agents usually post review threads. Check both surfaces every cycle. Treat a conversation comment that carries findings or a verdict as a review; it has no thread to resolve, so disposition it with one PR comment that quotes or links it.

A `COMMENTED` review body can block with zero unresolved threads. Address outstanding concerns, `CHANGES_REQUESTED`, current inline findings, and unresolved threads. Empty acknowledgments are non-blocking. Assess older findings against the current head and later discussion; uncertain concerns block merging.

Verify every finding against code and classify it. **Blocking**: a correctness, concurrency, security, contract, or performance defect, or missing tests for changed behavior. Fix each valid blocking finding in this PR.

**Non-blocking**: naming, style, wording, optional refactors, extra tests for unchanged behavior, "consider" suggestions, and requests for a benchmark that [AGENTS.md](../../../../AGENTS.md#performance-requirements) does not require (decline with the one-line per-message cost).

Implement a non-blocking suggestion only when it is small and you already push a blocking fix. Otherwise decline it in the disposition with a one-line reason and push nothing for it. Decline a wrong finding with evidence from the code. Move architectural work that is genuinely out of scope to a filed follow-up issue, linked in the reply. "Not done" or "left as is" without a classification, reason, or linked issue is not a disposition. Reply once per review covering each finding, and reply on each thread. Discuss disagreement once; implement reaffirmed blocking feedback. A disposition-only reply on the current head needs no new push, so it does not start another review cycle.

Resolve every review thread under the PR lock, whether a human or a bot opened it, as soon as it is dispositioned: the fix commit is in the remote PR head and the reply names it, or the reply pushes back on the finding with evidence. Do not wait for another review/CI cycle to resolve a dispositioned thread; that cycle still gates merging. Leave a thread open only while it has no disposition: the fix is not pushed, the item waits on the user, or the reviewer replied after your disposition. On each later review cycle, re-fetch every thread, resolved ones included; a reviewer reply newer than your disposition reopens it: run GraphQL `unresolveReviewThread` and handle it as reaffirmed feedback. Reply to addressed review-body findings and request re-review when an outstanding changes-requested review needs updating; do not dismiss reviews to bypass approval requirements. Use GraphQL `resolveReviewThread`, then re-fetch reviews, threads, checks, and head SHA.

## Draft review lifecycle

Repository protection: the active `Main` repository ruleset targets `refs/heads/main` and requires `CI Passed` from GitHub Actions (app ID `15368`). The maintainer is a bypass actor on purpose; never edit the ruleset or its bypass list (see [AGENTS.md](../../../../AGENTS.md#pull-requests)). Repository auto-merge must be enabled. `Enable-AutoMerge.ps1` verifies the effective target-branch rules and the enforcing ruleset before promotion and again before arming; missing protection, unreadable rules, or a changed PR target fail closed. Because agents use the maintainer's credential, GitHub can report a PR as mergeable before CI finishes, and `gh pr merge --auto` would then merge at once; the script refuses to arm in that state until `CI Passed` has succeeded on the head. Classic branch protection alone does not satisfy this guard. See [GitHub's rules API](https://docs.github.com/en/rest/repos/rules) and `scripts/Test-AutoMergeProtection.ps1`.

Draft policy: [AGENTS.md](../../../../AGENTS.md) applies the skill's draft review lifecycle to every agent-created PR, inside or outside the loop. Before creating or promoting a PR from an existing worktree, re-read `AGENTS.md` and this skill from the shared checkout; branch copies can predate the current policy. This replaces draft-review.md's loop-only scope.

Dependency updates have no review exception. `renovate.json` creates drafts and disables both Renovate's own merging and platform auto-merge, including inherited package rules. Claude reviews dependency branches and bot actors. Request Codex on each draft head, disposition findings on every review surface, then use `Enable-AutoMerge.ps1 -MarkReady` only after both current-head verdicts are positive. When taking over an older dependency PR that is already ready or armed, disable auto-merge under its PR lock and restore the draft before reviewing it; Renovate's `draftPR` setting only applies at creation.

Required reviewers are Claude Code Review and Codex. Other review bots (CodeRabbit, Greptile) are not approvers, but their findings are review surfaces like any other.

- **Claude:** `.github/workflows/claude-code-review.yml` reviews drafts on open, push, and reopen, not on `ready_for_review`. It posts one `github-actions[bot]` issue comment per run whose first line is `<!-- REVIEW_RUN: <run>/<attempt>; HEAD: <full-head-SHA> -->` and whose last line is `<!-- REVIEW_VERDICT: CLEAR -->` or `<!-- REVIEW_VERDICT: BLOCKING -->`. A positive verdict is `CLEAR` with `HEAD` equal to the current head. Recover missing reviews through [Claude review recovery](#claude-review-recovery), not an unconditional manual dispatch. The automatic workflow uses `pull_request_target` and trusted base code; a PR that changes the workflow is reviewed by the base version, never its own privileged workflow. It needs the `CLAUDE_CODE_OAUTH_TOKEN` repository secret.
- **Codex:** `chatgpt-codex-connector[bot]` reviews automatically when a PR opens ready or leaves draft, and on drafts only on request. Its mutable `<!-- codex-pull-request-review-summary -->` issue comment carries `<!-- codex-review-state:v1 {...} -->`; a positive verdict has `code_review.commit_sha` equal to the full current head, `status: completed`, and `has_any_findings: false`. A thumbs-up from `chatgpt-codex-connector[bot]` on this head's request (verify the author through `issues/comments/<id>/reactions`) also counts. Inline findings arrive as review threads. Re-read the summary on each visit; never infer the reviewed commit from timing.

Automated review rounds (rework budget): count Claude review comments with `gh api --paginate "repos/{owner}/{repo}/issues/<N>/comments" --jq '.[] | select(.user.login == "github-actions[bot]" and (.body | startswith("<!-- REVIEW_RUN:"))) | .id' | wc -l`.

Draft CI: build/test CI runs once, on a reviewed head, instead of on every review-fix push. `ci.yml` skips its jobs on draft PR events before renting a build runner. `ready_for_review` starts it on the approved head; `converted_to_draft` cancels an in-flight run through workflow concurrency. The Claude review runs on drafts and is not repeated on `ready_for_review`. `Assert-PrGreen.ps1` denies drafts.

Auto-merge: the `main` ruleset requires `CI Passed`, and GitHub's auto-merge waits only for it. The `CI Passed` job name is a `${{ }}` expression that a job skipped by `if:` never evaluates, so a draft run reports the expression text, never the required name, and cannot satisfy it; auto-merge armed right after `gh pr ready` waits for the ready run. Every `CI` job feeds `CI Passed` (`Test-AssertPrGreenChecks.ps1` pins this). `Enable-AutoMerge.ps1` (with `-MarkReady` for the promotion) waits for the ready run's `Detect Changes` job to register, then runs `Assert-PrGreen.ps1 -ForAutoMerge`, which lets only `CI` checks be pending and requires a Claude review comment for the head. On any push to a PR with auto-merge armed, `ci.yml`'s `disarm-auto-merge` job disarms it, and `CI Passed` fails if disarming fails.

The `Performance` workflow is advisory under D18, including failed, cancelled and pending runs. It is excluded from merge and arming check requirements; `UNSTABLE` permits an immediate merge only when an advisory check explains it and every other check passes. Required CI, review and conflict gates remain enforced. Investigate and disposition advisory failures without retrying for green.

Promotion replaces the `gh pr ready` and arming commands in steps 4 and 5 of [draft-review.md](draft-review.md). When every required reviewer approves the same current head and every finding is dispositioned, archive cited evidence as the [Merge gate](#merge-gate) requires, then promote and arm in one command from the shared checkout: `pwsh scripts/Enable-AutoMerge.ps1 -Pr <N> -MarkReady`. The script runs `gh pr ready`, restores the draft if the head changed during promotion (review the new head), waits for the ready CI run to register, and arms auto-merge pinned to the head. Never run a bare `gh pr ready` for a promotion.

Do not wait for CI: take another item once the script arms. Marking ready does not trigger another Claude review, so the draft review of this head satisfies the gate. GitHub squash-merges the armed head once the required check (`CI Passed`) passes. Never arm a draft, and never call `gh pr merge --auto` directly. When the script denies after promotion, the PR is ready but unarmed: fix the blocker, then arm it with `pwsh scripts/Enable-AutoMerge.ps1 -Pr <N>`.

Ready, not armed: A ready PR whose head passed the draft review lifecycle but has no auto-merge request (`autoMergeRequest` is null): arm it with `Enable-AutoMerge.ps1`, or merge it with the merge wrapper once everything is green.

Codex reviews automatically when a PR leaves draft; read that review on the next visit like any other. A push disarms auto-merge, so every new head needs its own review cycle and a new arming. To fix a CI failure or a blocking finding on a ready PR, convert it back to draft (`gh pr ready <N> --undo`) before pushing, then repeat this lifecycle from step 1.

## Merge gate

Arming auto-merge ([Draft review lifecycle](#draft-review-lifecycle)) is the normal way a PR merges; this gate applies when the merge wrapper merges a green PR immediately. Merge requires an open, ready-for-review PR; `MERGEABLE` and `CLEAN`; every applicable check present, terminal, and passing (`SUCCESS`, `SKIPPED`, or `NEUTRAL`, with successful legacy commit statuses); no unresolved threads, unaddressed comments, or outstanding review-body concerns; approval when required; and a bot review/CI cycle after the last fix push. Missing expected checks and nonterminal checks block merging. Reassess if the head changes.

Archive validation evidence when first citing it in a PR, outside every removable worktree: in `$evidenceRoot/pr-<N>/<measured SHA>/` ([Worktrees and evidence](#worktrees-and-evidence)), or in a durable uploaded artifact when it must outlive the PR: local evidence is deleted once the item closes. Include raw benchmark/test reports, experiment inputs and hashes, fixture/probe sources, and loaded binaries needed to reproduce or reanalyze the evidence. Include both head-branch and detached review checkouts.

Before invoking the merge wrapper or arming auto-merge, verify the archived file inventory and SHA-256 hashes against the originals, then update the PR with the durable location. `.artifacts`, `bin`, `TestResults`, and similar generated directories inside a worktree are disposable under [WorktreeCleanup.ps1](../../../../scripts/WorktreeCleanup.ps1); another worker's merge or sweep can remove them. If archival fails, defer the merge and take another item. If evidence is already lost, correct its retention claim explicitly; a rerun is new evidence, not recovery of deleted results.

`scripts/Merge-Pr.ps1` calls `Assert-PrGreen.ps1`, merges on a passing mechanical gate, and cleans up. Confirm every condition above before invoking it.

Never bypass the scripts with direct `gh pr merge` or `--auto`: arm with `Enable-AutoMerge.ps1`, merge with `Merge-Pr.ps1`. An armed PR leaves its worktree for `Remove-MergedWorktrees.ps1` to clean up after the merge.

## Claude review recovery

For a failed or cancelled review-delivery run, prefer one diagnosed retry of the original PR-associated run when suitable. A successful manual dispatch can supply a verdict without replacing the PR's failed check; verify both the head-bound verdict and the PR check rollup before promotion. Never suppress a failed check or retry a code finding for green.

Work under the PR lock. Re-fetch the current head, all review surfaces and `statusCheckRollup` first. If a review is still running, report `waiting #N: run <id>`; do not start a competing run. A `BLOCKING` verdict is feedback to disposition, not a delivery failure to rerun.

1. Identify the failed or cancelled `claude-review` check in the PR rollup and its run ID from `detailsUrl`. Read its jobs and failing logs. Verify with `gh api repos/<owner>/<repo>/actions/runs/<id>` that the path is `.github/workflows/claude-code-review.yml`, the event is `pull_request_target`, and `pull_requests` includes this PR and its current head SHA. Use that PR association rather than `head_sha` alone to identify the reviewed head. Check earlier attempts too: a suitable run has not already been retried, is within GitHub's rerun window, and has a diagnosed delivery failure (for example a missing receipt) or infrastructure cancellation. An unrelated successful dispatch does not spend this original run's one recovery attempt.
2. For a suitable run, rerun the **whole original run once** with `gh run rerun <id>`. This keeps the PR check association and creates a new receipt for the new attempt. GitHub [retains the original event's SHA, ref and actor privileges on reruns](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs); a rerun does not pick up a workflow fix that landed later. If the original trusted workflow cannot deliver a valid receipt, fix that workflow on the base through a separate reviewed PR before seeking a fresh run. Never run the PR branch's workflow with secrets.
3. When no suitable original run exists, dispatch once on the trusted default branch: `gh workflow run claude-code-review.yml --ref main -f pr_number=<N>`. Record the run ID after verifying its event, branch and target PR input. A dispatch can post a current-head verdict, but its check can belong to `main` instead of the PR. Re-fetch the PR rollup: an outstanding failed or cancelled non-advisory check still blocks promotion and auto-merge even after a dispatched `CLEAR`. Recover that original check when eligible; otherwise fix the cause and obtain a fresh PR-associated run on a new head. If a repeat delivery failure needs an agent-workable prerequisite, work that prerequisite under its own lock; do not suppress, delete or ignore the check. With no failed original check, a verified dispatched verdict may satisfy the review requirement, while all other PR checks and protection requirements still apply.
4. After completion, re-fetch the PR head, comments and rollup. Require the receipt-verified run to succeed and its comment to name the run/attempt and the same current head with a positive verdict. Verify that the latest PR-associated `claude-review` execution is terminal and passing when replacing a failed check. A head change requires review of the new head. Keep receipt verification and the protected merge gates unchanged; a successful retry alone is not permission to mark ready or merge.

The [receipt recovery reproduction](claude-review-recovery.md) records the failed original run, successful unassociated dispatch, successful original retry and resulting PR rollup.

## Worktrees and evidence

Capture these from the shared checkout, after `$repo` and `$repoSlug` in SKILL.md:

```powershell
$repoName = ($repoSlug -split '/')[-1]
$worktreeRoot = Join-Path (Split-Path $repo -Parent) ($repoName + '-worktrees')
$evidenceRoot = "$repo-evidence"
```

Verify the repository and intended base; examples use `origin/main`. Use a writable temporary `<repoName>-worktrees` root if needed. Choose a unique worktree path when one already exists. No sparse-checkout step or custom push refspec applies here. Keep explicit lock names.

Write nothing loose beside the repository. Every file an agent produces outside its worktree has one home, so cleanup can find it:

- Evidence (build and test logs, reports, PR and comment body drafts, diffs, validation projects) goes in `$evidenceRoot/<lock-name>/`, for example `C:/git/Relayer-evidence/issue-12/build.log`. `scripts/Remove-MergedWorktrees.ps1` deletes a `pr-<N>` or `issue-<N>` entry once that item is closed (merged or not) and no registered worktree uses it, and any other entry once its newest file is older than 7 days (`-EvidenceRetentionDays`). Release keeps the evidence of an open item.
- Throwaway checkouts and baselines are worktrees under `$worktreeRoot`, never plain copies and never sibling folders such as `C:/git/Relayer-wt-12`.
- Downloaded SDKs and CLI tools go in `<repo-parent>/toolchains/<name>-<version>/` (for example `C:/git/toolchains/`), shared by every repository.
- Never write to the repository's parent directory itself or to the shared checkout.
- Send long command output (builds, tests, `gh api` dumps, logs) to a file under `$evidenceRoot`.

Never use `git stash` in a worktree. Every worktree of a repository shares one stash list, so a concurrent worker's push or pop changes what `stash@{0}` names, and `git stash pop` can apply another worker's changes. To set changes aside, commit them on the item's branch, or compare against a detached worktree of the base.

Use repository scripts for merged-worktree cleanup; `[gone]` or ancestry alone is insufficient. Preserve dirty, locked, open-PR, harness-managed, and locally divergent worktrees. Inspect untracked source before cleanup: scripts may clear build artifacts. Manually remove abandoned worktrees only after verifying repository/path, ownership, and absence of valuable unpublished work. Do not enable stale/scratch cleanup during surveys.

Stop only services/processes/containers started for this worktree. Never blanket-delete containers/volumes or stop shared lock Redis. Follow local lifecycle guidance; use Aspire only where an AppHost exists.

## Issue relationships

Native relationships take the numeric REST issue `.id`, not the issue number or `gh issue view --json id` (a GraphQL ID). Pass it with `-F` so it is sent as an integer:

```powershell
$childId = gh api "repos/{owner}/{repo}/issues/<child>" --jq .id
gh api "repos/{owner}/{repo}/issues/<parent>/sub_issues" -X POST -F "sub_issue_id=$childId"

$blockerId = gh api "repos/{owner}/{repo}/issues/<blocker>" --jq .id
gh api "repos/{owner}/{repo}/issues/<blocked>/dependencies/blocked_by" -X POST -F "issue_id=$blockerId"
```

Verify each write with `gh api "repos/{owner}/{repo}/issues/<parent>/sub_issues"` or `gh api "repos/{owner}/{repo}/issues/<blocked>/dependencies/blocked_by"`.

## Implement and ship

Before writing code, do the native-first search in AGENTS.md ("Search before you build") and record what you found in the PR description. If the issue asks for something the native client already provides, comment on the issue and build on the native feature instead.

Follow local validation, resource limits, target frameworks, generated-code boundaries, and performance requirements. Use TDD for bugs/behavior changes where feasible; explain exceptions. Run focused and relevant broader checks, including docs/frontend checks when affected. Update `website/docs` in the same PR for any user-facing change, as AGENTS.md's Documentation section requires.

Review the full diff for correctness, reuse, and efficiency (`/simplify` when available, otherwise equivalent review). Preserve performance/contracts and batch related fixes into one push.

Post commentary while active.
