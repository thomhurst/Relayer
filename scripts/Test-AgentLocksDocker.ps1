#!/usr/bin/env pwsh
# Deterministic subprocess tests: no Docker daemon or shared Redis is contacted.
$ErrorActionPreference = 'Stop'
$testRoot = Join-Path ([IO.Path]::GetTempPath()) "relayer-lock-docker-$([Guid]::NewGuid().ToString('N'))"
$owner = "docker-deadline-tests-$([Guid]::NewGuid().ToString('N'))"
$ownerHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($owner))).ToLowerInvariant()
$cache = Join-Path ([IO.Path]::GetTempPath()) "relayer-agent-locks/$ownerHash"
$lockName = 'issue-docker-deadline-test'
$tokenFile = Join-Path $cache "$lockName.token"
$originalFixture = $env:RELAYER_DOCKER_FIXTURE
$originalCalls = $env:RELAYER_DOCKER_CALLS
$originalMode = $env:RELAYER_DOCKER_MODE
$originalHang = $env:RELAYER_DOCKER_HANG
try {
    New-Item -ItemType Directory -Force $testRoot, $cache | Out-Null
    . (Join-Path $PSScriptRoot 'BoundedProcess.ps1')
    # Both pipes exceed typical OS buffers, so sequential drainage would deadlock.
    $pipes = Invoke-BoundedProcess -FilePath (Get-Process -Id $PID).Path -Arguments @('-NoProfile', '-Command', '[Console]::Out.Write("o" * 262144); [Console]::Error.Write("e" * 262144); exit 17') -TimeoutSeconds 10
    if ($pipes.ExitCode -ne 17 -or $pipes.Output.Length -ne 262144 -or $pipes.ErrorOutput.Length -ne 262144) {
        throw 'Concurrent pipe drainage or exit-code capture failed'
    }
    $argumentFixture = Join-Path $testRoot 'arguments.ps1'
    Set-Content $argumentFixture '"$($args[0].Length)|$($args[1])|$($args[2])"'
    $arguments = Invoke-BoundedProcess -FilePath (Get-Process -Id $PID).Path -Arguments @('-NoProfile', '-File', $argumentFixture, '', 'with spaces', '"quoted"') -TimeoutSeconds 10
    if ($arguments.ExitCode -ne 0 -or $arguments.Output.Trim() -ne '0|with spaces|"quoted"') { throw 'Argument preservation failed' }
    Copy-Item (Join-Path $PSScriptRoot 'AgentLocks.ps1') $testRoot
    # Substitute the external executable at the process boundary in an isolated
    # script copy; all AgentLocks behavior and the real process runner stay intact.
    $helper = (Join-Path $PSScriptRoot 'BoundedProcess.ps1').Replace("'", "''")
    Set-Content (Join-Path $testRoot 'BoundedProcess.ps1') @"
. '$helper'
`$script:realProcessRunner = `${function:Invoke-BoundedProcess}
function Invoke-BoundedProcess {
    param(`$FilePath, `$Arguments, `$TimeoutSeconds)
    & `$script:realProcessRunner -FilePath (Get-Process -Id `$PID).Path -Arguments (@('-NoProfile', '-File', `$env:RELAYER_DOCKER_FIXTURE) + `$Arguments) -TimeoutSeconds `$TimeoutSeconds
}
"@
    $env:RELAYER_DOCKER_FIXTURE = Join-Path $testRoot 'docker.ps1'
    $env:RELAYER_DOCKER_CALLS = Join-Path $testRoot 'calls.txt'
    Set-Content $env:RELAYER_DOCKER_FIXTURE @'
$verb = $args[0]
Add-Content $env:RELAYER_DOCKER_CALLS $verb
$calls = @(Get-Content $env:RELAYER_DOCKER_CALLS)
if ($calls.Count -eq [int]$env:RELAYER_DOCKER_HANG) {
    Set-Content ($env:RELAYER_DOCKER_CALLS + '.pid') $PID
    Start-Sleep -Seconds 120
    exit 99
}
if ($env:RELAYER_DOCKER_MODE -eq 'nonzero' -and $verb -eq 'exec') { Write-Error 'fixture failure'; exit 17 }
switch ($verb) {
    'inspect' {
        switch ($env:RELAYER_DOCKER_MODE) {
            'missing' { exit 1 }
            'legacy' { if ($calls.Count -eq 1) { 'false redis-server'; exit 0 }; exit 1 }
            'stopped' { 'false redis-server --appendonly yes'; exit 0 }
            default { 'true redis-server --appendonly yes'; exit 0 }
        }
    }
    'exec' {
        if ($args[3] -eq 'PING') { 'PONG' }
        elseif ($args[3] -eq 'GET') { 'private-fixture-token' }
        elseif ($args[4] -like '*return redis.call*GET*') { '{"worktree":""}' }
        else { '1' }
    }
}
'@
    $agentLocks = Join-Path $testRoot 'AgentLocks.ps1'
    function Invoke-Fixture([string]$Mode, [int]$HangAt, [string]$Verb = 'release') {
        $env:RELAYER_DOCKER_MODE = $Mode
        $env:RELAYER_DOCKER_HANG = "$HangAt"
        Remove-Item $env:RELAYER_DOCKER_CALLS, ($env:RELAYER_DOCKER_CALLS + '.pid') -ErrorAction SilentlyContinue
        Set-Content $tokenFile 'private-fixture-token' -NoNewline
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $deadline = if ($HangAt) { '2' } else { '10' }
        $processResult = Invoke-BoundedProcess -FilePath (Get-Process -Id $PID).Path -Arguments @('-NoProfile', '-File', $agentLocks, $Verb, '-LockName', $lockName, '-OwnerId', $owner, '-DockerTimeoutSeconds', $deadline) -TimeoutSeconds 15
        $output = $processResult.Output + $processResult.ErrorOutput
        $code = $processResult.ExitCode
        $watch.Stop()
        if ($watch.Elapsed.TotalSeconds -gt 15) { throw "Fixture exceeded bounded runtime: $($watch.Elapsed)" }
        [pscustomobject]@{ Code = $code; Output = $output; Calls = @(Get-Content $env:RELAYER_DOCKER_CALLS) }
    }
    function Assert-Failure($Result, [string]$Action) {
        if ($Result.Code -ne 1 -or $Result.Output -notlike "*docker $Action during release for $lockName*failed*") {
            throw "Expected actionable $Action failure, got $($Result.Code): $($Result.Output)"
        }
        if ($Result.Output -match 'private-fixture-token|STALE|HELD-BY-ME') { throw 'Failure leaked a token or claimed ownership outcome' }
        if ((Get-Content $tokenFile -Raw) -ne 'private-fixture-token') { throw 'Failure changed ownership cache' }
    }
    # Every backend branch, including inspect after removal, setup, and cold PING.
    foreach ($case in @(
        @('running', 1, 'inspect'), @('running', 2, 'exec'), @('running', 3, 'exec'),
        @('missing', 2, 'run'), @('missing', 3, 'exec'),
        @('legacy', 2, 'rm'), @('legacy', 3, 'inspect'),
        @('stopped', 2, 'start')
    )) {
        $result = Invoke-Fixture $case[0] $case[1]
        Assert-Failure $result $case[2]
        if ($result.Output -notlike '*deadline*') { throw 'Missing timeout diagnostic' }
        $hungPid = [int](Get-Content ($env:RELAYER_DOCKER_CALLS + '.pid'))
        if (Get-Process -Id $hungPid -ErrorAction SilentlyContinue) { throw 'Timed-out Docker subprocess survived termination' }
    }
    Assert-Failure (Invoke-Fixture 'nonzero' 0) 'exec'
    # A retained cache can recover through a later authoritative Redis release.
    $result = Invoke-Fixture 'running' 0
    if ($result.Code -ne 0 -or (Test-Path $tokenFile)) { throw "Release recovery failed: $($result.Output)" }
    foreach ($mode in @('missing', 'legacy', 'stopped')) {
        $result = Invoke-Fixture $mode 0 'ping'
        if ($result.Code -ne 0) { throw "Cold setup failed: $($result.Output)" }
    }
    $result = Invoke-Fixture 'running' 0 'status'
    if ($result.Code -ne 0 -or $result.Output.Trim() -ne 'HELD-BY-ME') { throw "Status failed: $($result.Output)" }
    Write-Host 'OK AgentLocks bounded Docker tests passed.'
}
finally {
    $env:RELAYER_DOCKER_FIXTURE = $originalFixture
    $env:RELAYER_DOCKER_CALLS = $originalCalls
    $env:RELAYER_DOCKER_MODE = $originalMode
    $env:RELAYER_DOCKER_HANG = $originalHang
    Remove-Item $testRoot, $cache -Recurse -Force -ErrorAction SilentlyContinue
}
