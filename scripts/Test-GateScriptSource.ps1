#!/usr/bin/env pwsh
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'GateScriptSource.ps1')

$testRoot = Join-Path ([IO.Path]::GetTempPath()) "relayer-gate-source-tests-$([Guid]::NewGuid().ToString('N'))"
$origin = Join-Path $testRoot 'origin.git'
$publisher = Join-Path $testRoot 'publisher'
$checkout = Join-Path $testRoot 'checkout'
$report = Join-Path $testRoot 'report.txt'
$previous = $env:RELAYER_GATE_SCRIPTS_COMMIT

function Invoke-Git {
    git @args 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "git $args failed (exit $LASTEXITCODE)" }
}

# The probe records which copy ran, the pinned commit it saw, and the argument it was given.
function Publish-Probe([string]$Version) {
    $probe = "param([string]`$Report)`nSet-Content -LiteralPath `$Report -Value ('$Version|' + `$env:RELAYER_GATE_SCRIPTS_COMMIT)`nexit 7`n"
    Set-Content -LiteralPath (Join-Path $publisher 'scripts/Probe.ps1') -Value $probe
    Invoke-Git -C $publisher add -A
    Invoke-Git -C $publisher commit -q -m $Version
    Invoke-Git -C $publisher push -q origin HEAD:main
    return (git -C $publisher rev-parse HEAD).Trim()
}

function Invoke-Probe([string]$Branch = 'main') {
    if (Test-Path -LiteralPath $report) { Remove-Item -LiteralPath $report }
    Invoke-GateFromBase -ScriptPath (Join-Path $checkout 'scripts/Probe.ps1') -Branch $Branch -Arguments @('-Report', $report)
}

function Assert-Equal($Expected, $Actual, [string]$Case) {
    if ($Expected -ne $Actual) { throw "${Case}: expected '$Expected', got '$Actual'" }
}

function Assert-Throws([scriptblock]$Action, [string]$Case) {
    $threw = $false
    try { & $Action | Out-Null } catch { $threw = $true }
    if (-not $threw) { throw "${Case}: expected a failure" }
}

try {
    $env:RELAYER_GATE_SCRIPTS_COMMIT = $null
    New-Item -ItemType Directory -Path (Join-Path $publisher 'scripts') -Force | Out-Null
    Invoke-Git init -q --bare --initial-branch=main $origin
    Invoke-Git init -q --initial-branch=main $publisher
    Invoke-Git -C $publisher config user.email test@example.com
    Invoke-Git -C $publisher config user.name Test
    Invoke-Git -C $publisher config core.autocrlf false
    Invoke-Git -C $publisher remote add origin $origin
    $v1 = Publish-Probe 'v1'
    Invoke-Git clone -q --config core.autocrlf=false $origin $checkout

    Assert-Equal $null (Invoke-Probe) 'scripts matching origin run in place'
    Assert-Equal $false (Test-Path -LiteralPath $report) 'in-place run must not start a child'

    Add-Content -LiteralPath (Join-Path $checkout 'scripts/Probe.ps1') -Value '# local edit'
    Assert-Equal 7 (Invoke-Probe) 'locally edited scripts re-run from origin'
    Assert-Equal "v1|$v1" (Get-Content -LiteralPath $report) 'locally edited scripts'
    Assert-Equal $null $env:RELAYER_GATE_SCRIPTS_COMMIT 'pinned commit must not leak to the caller'
    Invoke-Git -C $checkout checkout -q -- scripts

    # The checkout has never fetched v2, as with a shared checkout that lags main.
    $v2 = Publish-Probe 'v2'
    Assert-Equal 7 (Invoke-Probe) 'stale checkout re-runs from origin'
    Assert-Equal "v2|$v2" (Get-Content -LiteralPath $report) 'stale checkout'
    Assert-Equal $v1 (git -C $checkout rev-parse refs/remotes/origin/main).Trim() 'verification must not move remote refs'

    # A branch whose name ends in the base ref sorts first in ls-remote; it must not be used.
    Invoke-Git -C $publisher push -q origin "$v1`:refs/heads/a/refs/heads/main"
    Assert-Equal 7 (Invoke-Probe) 'tail-matching branch'
    Assert-Equal "v2|$v2" (Get-Content -LiteralPath $report) 'only the exact base ref is used'

    $env:RELAYER_GATE_SCRIPTS_COMMIT = $v2
    Assert-Equal $null (Invoke-Probe) 'a pinned child runs in place'
    $env:RELAYER_GATE_SCRIPTS_COMMIT = $null

    Assert-Throws { Invoke-Probe -Branch 'missing' } 'unknown base branch'
    $outside = Join-Path $testRoot 'outside'
    New-Item -ItemType Directory -Path $outside | Out-Null
    $env:GIT_CEILING_DIRECTORIES = $testRoot
    try {
        Assert-Throws { Invoke-GateFromBase -ScriptPath (Join-Path $outside 'Probe.ps1') -Branch main } 'scripts outside a checkout'
    }
    finally { $env:GIT_CEILING_DIRECTORIES = $null }
}
finally {
    $env:RELAYER_GATE_SCRIPTS_COMMIT = $previous
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Output 'OK gate scripts run from the base branch.'
