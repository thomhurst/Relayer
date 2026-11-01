# Exercises the merge/auto-merge check gate in AssertPrGreenChecks.ps1 and pins the workflow
# contracts it relies on, without GitHub.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'AssertPrGreenChecks.ps1')
$workflows = Join-Path $PSScriptRoot '../.github/workflows'

function Assert-Equal($Actual, $Expected, [string]$Message) {
    $actualJson = ConvertTo-Json -InputObject $Actual -Compress -Depth 10
    $expectedJson = ConvertTo-Json -InputObject $Expected -Compress -Depth 10
    if ($actualJson -ne $expectedJson) { throw "$Message. Expected $expectedJson, got $actualJson." }
}

function New-Check([string]$Name, [string]$Workflow, [string]$Status, [string]$Conclusion = '', $StartedAt = '2026-10-28T12:00:00Z') {
    [pscustomobject]@{ __typename = 'CheckRun'; name = $Name; workflowName = $Workflow; status = $Status; conclusion = $Conclusion; startedAt = $StartedAt }
}

# Superseded executions on the same head: the newest per workflow and name wins.
$draftSkip = New-Check 'Detect Changes' 'CI' 'COMPLETED' 'SKIPPED' '2026-10-28T10:00:00Z'
$cancelled = New-Check 'Detect Changes' 'CI' 'COMPLETED' 'CANCELLED' '2026-10-28T11:00:00Z'
$current = New-Check 'Detect Changes' 'CI' 'COMPLETED' 'SUCCESS' ([DateTime]::new(2026, 10, 28, 12, 0, 0, [DateTimeKind]::Utc))
$selected = @(Select-CurrentCheck @($cancelled, $current, $draftSkip))
Assert-Equal $selected.Count 1 'Only the newest execution of a check counts'
Assert-Equal $selected[0].conclusion 'SUCCESS' 'A cancelled or skipped superseded execution must not deny'
$queued = New-Check 'Detect Changes' 'CI' 'QUEUED' '' '0001-01-01T00:00:00Z'
Assert-Equal (@(Select-CurrentCheck @($current, $queued))[0].status) 'QUEUED' 'A check that has not started is the newest execution'
$status = [pscustomobject]@{ __typename = 'StatusContext'; context = 'legacy'; state = 'SUCCESS' }
Assert-Equal (@(Select-CurrentCheck @($status, $current)).Count) 2 'Status contexts pass through'
Assert-Equal (@(Select-CurrentCheck @(New-Check 'Build' 'CI' 'COMPLETED' 'SUCCESS'; New-Check 'Build' 'Other' 'COMPLETED' 'FAILURE')).Count) 2 'Same-named checks of different workflows are distinct'

# Merge mode: everything terminal and passing.
$green = @((New-Check 'CI Passed' 'CI' 'COMPLETED' 'SUCCESS'), (New-Check 'Docs' 'CI' 'COMPLETED' 'SKIPPED'), (New-Check 'CodeRabbit' '' 'COMPLETED' 'NEUTRAL'))
Assert-Equal @(Get-CheckDenial $green).Count 0 'Passing, skipped and neutral checks allow a merge'
$pendingCi = New-Check 'Build & Pack' 'CI' 'IN_PROGRESS'
Assert-Equal @(Get-CheckDenial @($pendingCi)) @('Build & Pack: state=IN_PROGRESS') 'Merge mode denies pending checks'
Assert-Equal @(Get-CheckDenial @([pscustomobject]@{ context = 'legacy'; state = 'PENDING' })) @('legacy: state=PENDING') 'Merge mode denies a pending status context'

# Auto-merge mode: only the required workflows may still be pending.
Assert-Equal @(Get-CheckDenial @($pendingCi, (New-Check 'CI Passed' 'CI' 'QUEUED')) -ForAutoMerge).Count 0 'Required workflows may be pending when arming'
Assert-Equal @(Get-CheckDenial @(New-Check 'Unit Tests' 'CI' 'COMPLETED' 'FAILURE') -ForAutoMerge) @('Unit Tests: state=FAILURE') 'A failed check denies arming'
Assert-Equal @(Get-CheckDenial @(New-Check 'Lint' 'Other' 'IN_PROGRESS') -ForAutoMerge) @('Lint: state=IN_PROGRESS') 'A pending check of a non-required workflow denies arming'
Assert-Equal @(Get-CheckDenial @(New-Check 'Greptile Review' '' 'IN_PROGRESS') -ForAutoMerge) @('Greptile Review: state=IN_PROGRESS') 'A pending app check denies arming'

# The Performance workflow is advisory (D18); it must not become a local merge requirement.
foreach ($mode in @($false, $true)) {
    foreach ($state in @('FAILURE', 'CANCELLED', 'TIMED_OUT', 'IN_PROGRESS', 'QUEUED')) {
        $check = if ($state -in @('IN_PROGRESS', 'QUEUED')) {
            New-Check 'Performance regression gate' 'Performance' $state
        } else { New-Check 'Performance regression gate' 'Performance' 'COMPLETED' $state }
        Assert-Equal @(Get-CheckDenial @($green + $check) -ForAutoMerge:$mode).Count 0 "Advisory $state does not block (auto=$mode)"
    }
    $requiredFailure = New-Check 'Performance regression gate' 'CI' 'COMPLETED' 'FAILURE'
    Assert-Equal @(Get-CheckDenial @($requiredFailure) -ForAutoMerge:$mode) @('Performance regression gate: state=FAILURE') 'A matching job name in CI is still required'
}
$advisoryFailure = New-Check 'Performance regression gate' 'Performance' 'COMPLETED' 'FAILURE'
Assert-Equal (Test-MergeStateAllowed 'UNSTABLE' @($green + $advisoryFailure)) $true 'Advisory failure can explain UNSTABLE'
Assert-Equal (Test-MergeStateAllowed 'UNSTABLE' $green) $false 'Unexplained UNSTABLE still denies merging'
Assert-Equal (Test-MergeStateAllowed 'BLOCKED' @($green + $advisoryFailure)) $false 'Advisory failure cannot explain blocked protection'
Assert-Equal (Test-MergeStateAllowed 'DIRTY' @($green + $advisoryFailure) -ForAutoMerge) $false 'Conflicts still deny arming'
Assert-Equal @(Get-CheckDenial @($green + $advisoryFailure + $pendingCi)) @('Build & Pack: state=IN_PROGRESS') 'Advisory failure does not hide pending CI'
Assert-Equal @(Get-CheckDenial @($advisoryFailure, $requiredFailure) -ForAutoMerge) @('Performance regression gate: state=FAILURE') 'Advisory failure does not hide failed CI'
Assert-Equal @(Get-CheckDenial @([pscustomobject]@{ context = 'Performance'; state = 'FAILURE' })) @('Performance: state=FAILURE') 'A legacy status cannot impersonate the advisory workflow'

# Claude review of the current head.
$review = [pscustomobject]@{ user = [pscustomobject]@{ login = 'github-actions[bot]' }; body = "<!-- REVIEW_RUN: 1/1; HEAD: abc -->`nLooks good." }
Assert-Equal (Test-ClaudeReviewForHead -Comments @($review) -Head 'abc') $true 'A review comment for the head counts'
Assert-Equal (Test-ClaudeReviewForHead -Comments @($review) -Head 'def') $false 'A review of an older head does not count'
$forged = [pscustomobject]@{ user = [pscustomobject]@{ login = 'someone' }; body = $review.body }
Assert-Equal (Test-ClaudeReviewForHead -Comments @($forged) -Head 'abc') $false 'Only the workflow account posts reviews'

# Workflow contracts the auto-merge gate relies on.
$ci = Get-Content -Raw (Join-Path $workflows 'ci.yml')
Assert-Equal ([regex]::Match($ci, '(?m)^name:\s*(.+)$').Groups[1].Value.Trim()) 'CI' 'ci.yml is the awaited CI workflow'
Assert-Equal (@($AutoMergeAwaitedWorkflows) -join ',') 'CI' 'Only required-check workflows are awaited'
$performance = Get-Content -Raw (Join-Path $workflows 'performance.yml')
Assert-Equal ([regex]::Match($performance, '(?m)^name:\s*(.+)$').Groups[1].Value.Trim()) 'Performance' 'The advisory workflow identity matches the gate'
$aggregator = [regex]::Match($ci, '(?ms)^  ci-passed:\r?\n(.*?)(?=^  \S|\z)').Groups[1].Value
if (-not $aggregator) { throw 'ci.yml has no ci-passed job' }
if ($aggregator -notmatch "(?m)^    name: \$\{\{ needs\.changes\.result == 'success' && 'CI Passed' \|\| 'CI Passed \(incomplete\)' \}\}\s*$") {
    throw 'The required CI Passed name must be computed so a skipped draft run cannot report it'
}
if ($aggregator -notmatch "(?m)^    if: always\(\) && \(github\.event_name != 'pull_request' \|\| github\.event\.pull_request\.draft == false\)\s*$") {
    throw 'ci-passed must skip draft PR events'
}
$jobsSection = [regex]::Match($ci, '(?ms)^jobs:\s*$(.*)').Groups[1].Value
$jobs = @([regex]::Matches($jobsSection, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value } | Where-Object { $_ -ne 'ci-passed' })
$needs = @(([regex]::Match($aggregator, '(?m)^    needs: \[(.*)\]').Groups[1].Value -split ',') | ForEach-Object { $_.Trim() })
$missing = @($jobs | Where-Object { $needs -notcontains $_ })
Assert-Equal $missing @() 'ci-passed must need every ci.yml job, or the required check misses its failure'

Write-Host 'Assert-PrGreen check gate tests passed.'
