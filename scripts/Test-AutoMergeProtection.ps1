#!/usr/bin/env pwsh
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'AssertPrGreenChecks.ps1')
. (Join-Path $PSScriptRoot 'AutoMergeProtection.ps1')

function New-Rule {
    [pscustomobject]@{
        type = 'required_status_checks'
        ruleset_source_type = 'Repository'
        ruleset_source = 'owner/repo'
        ruleset_id = 42
        parameters = [pscustomobject]@{
            required_status_checks = @([pscustomobject]@{ context = 'CI Passed'; integration_id = 15368 })
        }
    }
}

function New-Ruleset {
    [pscustomobject]@{
        id = 42
        source = 'owner/repo'
        target = 'branch'
        enforcement = 'active'
        current_user_can_bypass = 'never'
        bypass_actors = @()
        rules = @(New-Rule)
    }
}

function Assert-Denied([scriptblock]$Action, [string]$Case) {
    $denied = $false
    try { & $Action }
    catch { $denied = $true }
    if (-not $denied) { throw "Expected protection denial: $Case" }
}

$rule = New-Rule
$ruleset = New-Ruleset
$readApi = {
    param($endpoint)
    if ($endpoint -eq 'repos/owner/repo/rules/branches/release%2Fnext') { return $rule }
    if ($endpoint -eq 'repos/owner/repo/rulesets/42') { return $ruleset }
    throw "Unexpected endpoint: $endpoint"
}
Assert-AutoMergeProtection -Repo owner/repo -Branch release/next -ReadApi $readApi
Assert-Denied { Assert-AutoMergeProtection -Repo owner/repo -Branch main -ReadApi { @() } } 'no rules'
Assert-Denied { Assert-AutoMergeProtection -Repo owner/repo -Branch main -ReadApi { throw 'HTTP 403' } } 'API failure'
foreach ($property in @('enforcement', 'target', 'source', 'id')) {
    $ruleset = New-Ruleset
    $ruleset.$property = 'wrong'
    Assert-Denied { Assert-AutoMergeProtection -Repo owner/repo -Branch release/next -ReadApi $readApi } $property
}
foreach ($actors in @(
    @{ value = $null },
    @{ value = @([pscustomobject]@{ actor_type = 'RepositoryRole'; actor_id = 5; bypass_mode = 'always' }) },
    @{ value = @([pscustomobject]@{ actor_type = 'RepositoryRole'; actor_id = 5; bypass_mode = 'pull_request' }) }
)) {
    $ruleset = New-Ruleset
    $ruleset.bypass_actors = $actors.value
    $ruleset.current_user_can_bypass = 'always'
    # The maintainer is a bypass actor; Test-ImmediateMergeSafe guards the early merge instead.
    Assert-AutoMergeProtection -Repo owner/repo -Branch release/next -ReadApi $readApi
}
foreach ($surface in @('effective', 'detail')) {
    foreach ($property in @('context', 'integration_id')) {
        $rule = New-Rule
        $ruleset = New-Ruleset
        $check = if ($surface -eq 'effective') { $rule.parameters.required_status_checks[0] } else { $ruleset.rules[0].parameters.required_status_checks[0] }
        $check.$property = 'wrong'
        Assert-Denied { Assert-AutoMergeProtection -Repo owner/repo -Branch release/next -ReadApi $readApi } "$surface $property"
    }
}

function New-Check([string]$Name, [string]$Status, [string]$Conclusion, [string]$Started) {
    [pscustomobject]@{ __typename = 'CheckRun'; workflowName = 'CI'; name = $Name; status = $Status; conclusion = $Conclusion; startedAt = $Started }
}
$passed = New-Check 'CI Passed' 'COMPLETED' 'SUCCESS' '2026-01-01T00:00:00Z'
$pending = New-Check 'CI Passed' 'IN_PROGRESS' '' '2026-01-01T00:01:00Z'
foreach ($case in @(
    @{ state = 'BLOCKED'; checks = @(); safe = $true },
    @{ state = 'CLEAN'; checks = @($passed); safe = $true },
    @{ state = 'UNSTABLE'; checks = @($passed); safe = $true },
    @{ state = 'CLEAN'; checks = @(); safe = $false },
    @{ state = 'HAS_HOOKS'; checks = @($pending); safe = $false },
    # A newer run on the same head replaces an earlier success.
    @{ state = 'CLEAN'; checks = @($passed, $pending); safe = $false },
    @{ state = 'CLEAN'; checks = @((New-Check 'CI Passed' 'COMPLETED' 'FAILURE' '')); safe = $false },
    @{ state = 'CLEAN'; checks = @((New-Check 'CI Passed (incomplete)' 'COMPLETED' 'SUCCESS' '')); safe = $false }
)) {
    if ((Test-ImmediateMergeSafe -State $case.state -Checks $case.checks) -ne $case.safe) {
        throw "Immediate merge case $($case.state) with $($case.checks.Count) check(s): expected safe=$($case.safe)"
    }
}

# Run the real wrapper in a child process. Mock only gh and the unrelated review gate;
# assert the mutation log so a denial cannot accidentally promote or merge a PR.
$artifactsRoot = [IO.Path]::GetFullPath((Join-Path (Split-Path $PSScriptRoot -Parent) '.artifacts'))
$testRoot = Join-Path $artifactsRoot "auto-merge-tests-$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Path $testRoot -Force | Out-Null
try {
    Copy-Item (Join-Path $PSScriptRoot 'Enable-AutoMerge.ps1') $testRoot
    Copy-Item (Join-Path $PSScriptRoot 'AutoMergeProtection.ps1') $testRoot
    Copy-Item (Join-Path $PSScriptRoot 'AssertPrGreenChecks.ps1') $testRoot
    Copy-Item (Join-Path $PSScriptRoot 'GateScriptSource.ps1') $testRoot
    Set-Content (Join-Path $testRoot 'Assert-PrGreen.ps1') 'exit 0'
    $runner = @'
param([string]$Scenario, [switch]$AlreadyReady)
$ErrorActionPreference = 'Stop'
# Run the copied wrapper in place; Test-GateScriptSource.ps1 covers the re-run from the base.
$env:RELAYER_GATE_SCRIPTS_COMMIT = 'pinned-by-test'
$global:AutoMergeTestState = @{ protectionReads = 0; targetReads = 0; draft = -not $AlreadyReady }
function gh {
    $global:LASTEXITCODE = 0
    $command = ($args | ForEach-Object { if ($_ -is [array]) { $_ -join ',' } else { $_ } }) -join ' '
    if ($args[0] -eq 'api') {
        if ($args[1] -like '*/rules/branches/*') {
            $global:AutoMergeTestState.protectionReads++
            if ($Scenario -eq 'api-failure') { $global:LASTEXITCODE = 1; return }
            if ($Scenario -eq 'malformed') { return 'not json' }
            if ($Scenario -eq 'missing' -or ($Scenario -eq 'removed' -and $global:AutoMergeTestState.protectionReads -eq 2)) { return '[[]]' }
            # The valid rule is on page two: the production reader must flatten every page.
            return '[[],[{"type":"required_status_checks","ruleset_source_type":"Repository","ruleset_source":"owner/repo","ruleset_id":42,"parameters":{"required_status_checks":[{"context":"CI Passed","integration_id":15368}]}}]]'
        }
        if ($args[1] -eq 'repos/owner/repo/rulesets/42') {
            return '[{"id":42,"source":"owner/repo","target":"branch","enforcement":"active","current_user_can_bypass":"never","bypass_actors":[],"rules":[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"CI Passed","integration_id":15368}]}}]}]'
        }
    }
    if ($command -eq 'repo view owner/repo --json nameWithOwner --jq .nameWithOwner') { return 'owner/repo' }
    if ($command -like 'pr view*baseRefName*') {
        $global:AutoMergeTestState.targetReads++
        $branch = if ($Scenario -eq 'retargeted' -and $global:AutoMergeTestState.targetReads -eq 2) { 'other' } else { 'main' }
        return (@{ baseRefName = $branch } | ConvertTo-Json -Compress)
    }
    if ($command -like 'pr view*mergeStateStatus*') {
        # 'early': the caller can bypass, so GitHub reports CLEAN while CI Passed still runs.
        if ($Scenario -eq 'early') { return '{"mergeStateStatus":"CLEAN","statusCheckRollup":[{"__typename":"CheckRun","workflowName":"CI","name":"CI Passed","status":"IN_PROGRESS","conclusion":""}]}' }
        return '{"mergeStateStatus":"BLOCKED","statusCheckRollup":[]}'
    }
    if ($command -like 'pr view*isDraft,headRefOid*') { return (@{ isDraft = $global:AutoMergeTestState.draft; headRefOid = 'abc' } | ConvertTo-Json -Compress) }
    if ($command -like 'pr view*statusCheckRollup*') { return '1' }
    if ($command -like 'pr view*headRefOid*') { return 'abc' }
    if ($command -like 'pr ready*') { Add-Content (Join-Path $PSScriptRoot 'mutations.log') 'ready'; $global:AutoMergeTestState.draft = $false; return }
    if ($command -eq 'pr merge 123 --repo owner/repo --auto --squash --match-head-commit abc') {
        Add-Content (Join-Path $PSScriptRoot 'mutations.log') 'merge'; return
    }
    throw "Unexpected gh call: $command"
}
& (Join-Path $PSScriptRoot 'Enable-AutoMerge.ps1') -Pr 123 -Repo owner/repo -MarkReady:(-not $AlreadyReady)
exit $LASTEXITCODE
'@
    $runnerPath = Join-Path $testRoot 'Run-Wrapper.ps1'
    Set-Content $runnerPath $runner
    foreach ($case in @(
        @{ scenario = 'missing' }, @{ scenario = 'api-failure' }, @{ scenario = 'malformed' },
        @{ scenario = 'removed' }, @{ scenario = 'retargeted' }, @{ scenario = 'valid' }, @{ scenario = 'early' },
        @{ scenario = 'missing'; alreadyReady = $true }, @{ scenario = 'valid'; alreadyReady = $true }
    )) {
        $scenario = $case.scenario
        $mutationLog = Join-Path $testRoot 'mutations.log'
        if (Test-Path $mutationLog) { Remove-Item -LiteralPath $mutationLog }
        $output = & pwsh -NoProfile -File $runnerPath -Scenario $scenario -AlreadyReady:([bool]$case.alreadyReady) 2>&1
        $code = $LASTEXITCODE
        $mutations = if (Test-Path $mutationLog) { (Get-Content $mutationLog) -join ',' } else { '' }
        $expected = switch ($scenario) {
            valid { 'ready,merge' }
            removed { 'ready' }
            early { 'ready' }
            retargeted { 'ready' }
            default { '' }
        }
        if ($case.alreadyReady -and $scenario -eq 'valid') { $expected = 'merge' }
        if ($mutations -ne $expected -or (($scenario -eq 'valid') -ne ($code -eq 0))) {
            throw "Wrapper case ${scenario}: exit=$code mutations=$mutations; $output"
        }
    }
}
finally {
    $resolvedTestRoot = (Resolve-Path -LiteralPath $testRoot).Path
    if ([IO.Path]::GetDirectoryName($resolvedTestRoot) -ne $artifactsRoot) { throw 'Test cleanup escaped its artifacts directory' }
    Remove-Item -LiteralPath $resolvedTestRoot -Recurse -Force
}
Write-Output 'OK auto-merge protection and wrapper mutation guards passed.'
