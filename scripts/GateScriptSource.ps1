# Runs the merge gate as committed on the PR's base branch.
#
# Agents invoke the gate from a shared checkout that can lag the base branch, or from a PR
# worktree whose scripts are unreviewed. A stale copy enforces old rules: after #145 made the
# Performance workflow advisory, a checkout from before it still denied arming on every
# in-progress `Performance regression gate`. The gate that decides a merge must be the one on
# the base branch, so Invoke-GateFromBase re-runs the calling script from that commit when the
# local scripts directory differs from it.

# Returns $null when the caller should continue in place: its scripts directory matches
# origin/<Branch>, or it already runs from a pinned copy. Otherwise runs the base branch's copy
# of the calling script with -Arguments and returns that run's exit code. The child (and its
# nested gate calls) sees the pinned commit in RELAYER_GATE_SCRIPTS_COMMIT and runs in place.
# Throws when the base branch cannot be read, so callers fail closed.
function Invoke-GateFromBase {
    param(
        [Parameter(Mandatory)][string]$ScriptPath,
        [Parameter(Mandatory)][string]$Branch,
        [string[]]$Arguments = @()
    )

    if ($env:RELAYER_GATE_SCRIPTS_COMMIT) { return $null }

    $directory = Split-Path -Parent $ScriptPath
    $prefix = git -C $directory rev-parse --show-prefix 2>$null
    if ($LASTEXITCODE -ne 0) { throw "$directory is not in a git checkout, so the gate scripts cannot be verified against origin/$Branch" }
    $prefix = ([string]$prefix).Trim().TrimEnd('/')
    $top = git -C $directory rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $top) { throw "could not resolve the checkout of $directory" }
    $top = ([string]$top).Trim()
    $pathspec = if ($prefix) { $prefix } else { '.' }

    # Resolve and fetch by commit: no ref is written, so agents sharing the checkout cannot
    # contend on a ref lock.
    $remote = git -C $directory ls-remote --exit-code origin "refs/heads/$Branch" 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $remote) { throw "could not resolve origin/$Branch to verify the gate scripts" }
    $commit = ([string]@($remote)[0] -split '\s+')[0]
    git -C $directory cat-file -e "$commit^{commit}" 2>$null
    if ($LASTEXITCODE -ne 0) {
        git -C $directory fetch --quiet --no-write-fetch-head origin $commit 2>$null
        if ($LASTEXITCODE -ne 0) { throw "could not fetch origin/$Branch ($commit) to verify the gate scripts" }
    }

    git -C $top diff --quiet $commit -- $pathspec 2>$null
    if ($LASTEXITCODE -eq 0) { return $null }
    if ($LASTEXITCODE -ne 1) { throw "could not compare the gate scripts with origin/$Branch ($commit)" }

    $root = Join-Path ([IO.Path]::GetTempPath()) "relayer-gate-$([Guid]::NewGuid().ToString('N'))"
    $scripts = Join-Path $root 'scripts'
    New-Item -ItemType Directory -Path $root | Out-Null
    try {
        $archive = Join-Path $root 'scripts.zip'
        $tree = if ($prefix) { "${commit}:$prefix" } else { $commit }
        git -C $top archive --format=zip -o $archive $tree 2>$null
        if ($LASTEXITCODE -ne 0) { throw "could not export the gate scripts from origin/$Branch ($commit)" }
        Expand-Archive -LiteralPath $archive -DestinationPath $scripts
        $child = Join-Path $scripts (Split-Path -Leaf $ScriptPath)
        if (-not (Test-Path -LiteralPath $child)) { throw "origin/$Branch ($commit) has no $(Split-Path -Leaf $ScriptPath)" }

        [Console]::Error.WriteLine("Gate scripts in $directory differ from origin/$Branch; running the copy at $commit.")
        $env:RELAYER_GATE_SCRIPTS_COMMIT = $commit
        & pwsh -NoProfile -File $child @Arguments | Out-Host
        return $LASTEXITCODE
    }
    finally {
        $env:RELAYER_GATE_SCRIPTS_COMMIT = $null
        Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
    }
}
