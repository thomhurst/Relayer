# Docker cleanup for the worktree sweep. Dot-source this file.
#
# Test fixtures and agent experiments leave Docker data behind: an image that declares
# VOLUME (postgres, docker:dind) gets an anonymous volume per container, and
# `docker rm --force` without `--volumes` keeps it. Nothing can reach such a volume by
# name once its container is gone, but the data stays in Docker's disk image.
#
# The Docker daemon is shared with every other repository on the machine, so these
# functions only remove what nobody can still use:
#   - anonymous volumes (label com.docker.volume.anonymous) that no container, running
#     or stopped, references, and that are older than the grace period. Named volumes,
#     such as the agent-lock Redis data, are never touched.
#   - stopped or never-started containers whose name starts with this repository's
#     prefix, once they have been idle longer than the retention period. The agent-lock
#     Redis container is excluded by name.

function Test-DockerAvailable {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { return $false }
    docker version --format '{{.Server.Version}}' 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

function ConvertFrom-DockerTime([string]$Value) {
    $parsed = [DateTimeOffset]::MinValue
    if ([DateTimeOffset]::TryParse($Value, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$parsed)) {
        return $parsed.UtcDateTime
    }
    return [DateTime]::MinValue
}

# Returns the number of volumes removed (or that would be removed with -WhatIf).
function Remove-DanglingAnonymousVolumes {
    param(
        [TimeSpan]$MinimumAge = [TimeSpan]::FromHours(1),
        [switch]$WhatIf
    )
    $names = @(docker volume ls --quiet --filter dangling=true --filter label=com.docker.volume.anonymous 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "docker volume ls failed (exit $LASTEXITCODE)" }
    $names = @($names | Where-Object { $_ })
    if ($names.Count -eq 0) { return 0 }

    # One inspect per chunk instead of per volume: a leak can leave thousands.
    $cutoff = [DateTime]::UtcNow - $MinimumAge
    $old = [Collections.Generic.List[string]]::new()
    for ($i = 0; $i -lt $names.Count; $i += 200) {
        $chunk = $names[$i..([Math]::Min($i + 199, $names.Count - 1))]
        foreach ($line in (docker volume inspect --format '{{.Name}} {{.CreatedAt}}' @chunk 2>$null)) {
            $name, $created = ([string]$line) -split ' ', 2
            # A volume removed since the listing is simply absent from the output.
            if ($name -and (ConvertFrom-DockerTime $created) -lt $cutoff) { $old.Add($name) }
        }
    }
    if ($WhatIf) { return $old.Count }

    $removed = 0
    for ($i = 0; $i -lt $old.Count; $i += 200) {
        $chunk = $old.GetRange($i, [Math]::Min(200, $old.Count - $i)).ToArray()
        # A volume a new container attached since the listing fails to remove; the
        # others in the chunk still go. Count what actually went.
        $removed += @(docker volume rm @chunk 2>$null | Where-Object { $_ }).Count
    }
    return $removed
}

# Returns the number of containers removed (or that would be removed with -WhatIf).
function Remove-StaleRepositoryContainers {
    param(
        [Parameter(Mandatory)][string]$NamePrefix,
        [string[]]$ExcludeNames = @(),
        [TimeSpan]$MinimumIdle = [TimeSpan]::FromHours(24),
        [switch]$WhatIf
    )
    $ids = @(docker ps --all --quiet --no-trunc --filter status=exited --filter status=created --filter status=dead --filter "name=^$NamePrefix" 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "docker ps failed (exit $LASTEXITCODE)" }
    $ids = @($ids | Where-Object { $_ })
    if ($ids.Count -eq 0) { return 0 }

    $cutoff = [DateTime]::UtcNow - $MinimumIdle
    $stale = [Collections.Generic.List[string]]::new()
    foreach ($line in (docker inspect --format '{{.Id}} {{.Name}} {{.State.Status}} {{.Created}} {{.State.FinishedAt}}' @ids 2>$null)) {
        $id, $name, $status, $created, $finished = ([string]$line) -split ' '
        $name = $name.TrimStart('/')
        # The name filter is a substring match on some engines; check the prefix exactly.
        if (-not $name.StartsWith($NamePrefix, [StringComparison]::Ordinal)) { continue }
        if ($name -in $ExcludeNames) { continue }
        # Re-check: the container may have been started since the listing.
        if ($status -notin @('exited', 'created', 'dead')) { continue }
        $idleSince = ConvertFrom-DockerTime $created
        $finishedAt = ConvertFrom-DockerTime $finished
        if ($finishedAt -gt $idleSince) { $idleSince = $finishedAt }
        if ($idleSince -lt $cutoff) { $stale.Add($id) }
    }
    if ($WhatIf -or $stale.Count -eq 0) { return $stale.Count }

    # Without -f, Docker refuses a container someone started since the inspect.
    return @(docker rm --volumes @stale 2>$null | Where-Object { $_ }).Count
}
