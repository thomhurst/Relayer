#!/usr/bin/env pwsh
[CmdletBinding()]
param([Parameter(Mandatory)][string]$ResultsDirectory)

$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
$results = [IO.Path]::GetFullPath($ResultsDirectory)
New-Item -ItemType Directory -Path $results -Force | Out-Null
$ledger = Join-Path $results 'fixture-ledger.json'
$report = Join-Path $results 'probe.trx'
Remove-Item $ledger, $report -ErrorAction SilentlyContinue
$previous = $env:RELAYER_PARALLELISM_PROBE_RESULT
try {
    $env:RELAYER_PARALLELISM_PROBE_RESULT = $ledger
    # The two failed setups are intentional. Check structured results, not log text.
    dotnet test -c Release --project (Join-Path $repo 'tools/CoreParallelismProbe') `
        --results-directory $results --report-trx --report-trx-filename probe.trx
    $testExit = $LASTEXITCODE
    if ($testExit -ne 2) { throw "Expected test-failure exit code 2, got $testExit." }
    if (!(Test-Path $ledger) -or !(Test-Path $report)) { throw 'Probe did not finish and write both reports.' }
    $state = Get-Content $ledger -Raw | ConvertFrom-Json
    if ($state.Entered -ne 8 -or $state.Exited -ne 8 -or $state.Active -ne 0 -or
        $state.Peak -ne 2 -or $state.Bodies -ne 6 -or $state.UnexpectedBodies -ne 0) {
        throw "Unexpected fixture state: $(ConvertTo-Json $state -Compress)"
    }
    [xml]$trx = Get-Content $report -Raw
    $outcomes = @($trx.TestRun.Results.UnitTestResult)
    $failed = @($outcomes | Where-Object outcome -eq 'Failed')
    $passed = @($outcomes | Where-Object outcome -eq 'Passed')
    if ($outcomes.Count -ne 8 -or $passed.Count -ne 6 -or $failed.Count -ne 2 -or
        @($failed | Where-Object testName -notmatch 'Setup_(fails|is_cancelled)$').Count -ne 0) {
        throw 'Expected six passing fixture users and only the two intentional setup failures.'
    }
    Write-Output 'PASS: two concurrent fixtures across classes; all eight teardown paths completed, including failure and cancellation.'
} finally {
    $env:RELAYER_PARALLELISM_PROBE_RESULT = $previous
}
# Do not propagate the intentional native test-failure status to the CI shell.
exit 0
