# Enable-AutoMerge.ps1
# Arms GitHub native auto-merge (squash) for PR #<N>, pinned to the reviewed head.
#
#   0. with -MarkReady, promotes the approved draft (`gh pr ready`), undoes the promotion if
#      the head moved, and waits for the ready CI run to register
#   1. captures the head, then runs  Assert-PrGreen.ps1 -ForAutoMerge  (ready, no failed
#      check, nothing pending outside the required CI workflow,
#      finished verdicts from the non-required path-filtered workflows the PR triggers, a
#      Claude review of this head, no unresolved threads) and stops on any denial
#   2. arms  gh pr merge --auto --squash --match-head-commit <head>
#
# GitHub then merges once the `Main` ruleset's required check (`CI Passed`) passes on that
# head. Draft runs never report the required `CI Passed` name, so arming right after
# `gh pr ready` waits for the ready run. A later push
# disarms auto-merge (the `disarm-auto-merge` job in ci.yml), so each new head needs its own
# review cycle and a new call to this script.
#
# The caller must read and dispose of every review body, inline finding and issue comment,
# including both required reviewers' verdicts on this head, BEFORE running this; the gate
# cannot judge review content.
#
# Auto-merge leaves the PR's worktree in place; scripts/Remove-MergedWorktrees.ps1 removes it
# after the merge. Use scripts/Merge-Pr.ps1 instead to merge an already green PR immediately.
#
# Usage: pwsh scripts/Enable-AutoMerge.ps1 -Pr 1234 [-MarkReady] [-Repo owner/name]
# Exit:  0 = armed; 1 = not armed (reason printed to stderr).

[CmdletBinding()]
param(
    [Parameter(Mandatory)][int]$Pr,
    [switch]$MarkReady,
    [string]$Repo
)

$ErrorActionPreference = 'Stop'

function Fail([string]$reason) {
    [Console]::Error.WriteLine("NOT ARMED #${Pr} -- $reason")
    exit 1
}

$repoArgs = @()
if ($Repo) { $repoArgs = @('--repo', $Repo) }

if ($MarkReady) {
    $before = gh pr view $Pr @repoArgs --json isDraft,headRefOid 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $before) { Fail "could not read PR state (exit $LASTEXITCODE)" }
    if ($before.isDraft) {
        gh pr ready $Pr @repoArgs
        if ($LASTEXITCODE -ne 0) { Fail "gh pr ready failed (exit $LASTEXITCODE)" }
    }
    $after = gh pr view $Pr @repoArgs --json isDraft,headRefOid 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $after -or $after.isDraft) { Fail 'PR is still a draft after gh pr ready' }
    if ($after.headRefOid -ne $before.headRefOid) {
        gh pr ready $Pr @repoArgs --undo
        Fail "head moved from $($before.headRefOid) to $($after.headRefOid) during promotion; restored the draft, review the new head"
    }
    # Wait for the ready run's first CI job to register on the head; that takes seconds. Draft
    # runs skip it, so a skipped execution does not count.
    $readyRun = '[.statusCheckRollup[] | select(.workflowName == "CI" and .name == "Detect Changes" and (.status != "COMPLETED" or .conclusion == "SUCCESS"))] | length'
    $deadline = [DateTimeOffset]::UtcNow.AddMinutes(3)
    while ([int](gh pr view $Pr @repoArgs --json statusCheckRollup --jq $readyRun 2>$null) -eq 0) {
        if ([DateTimeOffset]::UtcNow -ge $deadline) { Fail 'CI did not start on the ready head within 3 minutes; re-run this script without -MarkReady once it does' }
        Start-Sleep -Seconds 10
    }
}

# Capture the head before the gate so a push racing the gate cannot be armed unreviewed:
# --match-head-commit makes GitHub refuse if the head moved since.
$head = gh pr view $Pr @repoArgs --json headRefOid --jq '.headRefOid' 2>$null
if ($LASTEXITCODE -ne 0 -or -not $head) { Fail "could not read PR head (exit $LASTEXITCODE)" }
$head = $head.Trim()

$gateArgs = @('-Pr', $Pr, '-ForAutoMerge')
if ($Repo) { $gateArgs += @('-Repo', $Repo) }
& pwsh -NoProfile -File (Join-Path $PSScriptRoot 'Assert-PrGreen.ps1') @gateArgs
if ($LASTEXITCODE -ne 0) { Fail "auto-merge gate denied (see above)" }

gh pr merge $Pr @repoArgs --auto --squash --match-head-commit $head
if ($LASTEXITCODE -ne 0) { Fail "gh pr merge --auto failed (exit $LASTEXITCODE)" }

Write-Host "Armed auto-merge for #${Pr} at $head."
exit 0
