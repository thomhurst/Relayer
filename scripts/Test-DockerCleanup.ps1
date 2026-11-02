#!/usr/bin/env pwsh
# Regression tests for the sweep's Docker cleanup. Needs PowerShell 7 and Docker.
# Fixtures use a unique name prefix, so the container cleanup only sees its own containers.
# The volume cleanup is daemon-wide by design, so these tests drive it through a short
# grace period and assert only on their own volumes.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'DockerCleanup.ps1')

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Test-VolumeExists([string]$Name) {
    docker volume inspect $Name 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

function Test-ContainerExists([string]$Name) {
    docker inspect $Name 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-DockerAvailable)) { throw 'Docker is required for these tests.' }

$prefix = "relayer-docker-cleanup-test-$([Guid]::NewGuid().ToString('N').Substring(0, 8))-"
$image = 'alpine:3.22'
$created = [Collections.Generic.List[string]]::new()
try {
    docker pull --quiet $image | Out-Null

    # A container removed without --volumes leaves its anonymous volume dangling.
    $leaked = "${prefix}leaked"
    docker create --name $leaked --volume /data $image | Out-Null
    $created.Add($leaked)
    $leakedVolume = docker inspect --format '{{range .Mounts}}{{.Name}}{{end}}' $leaked
    docker rm $leaked | Out-Null
    Assert-True (Test-VolumeExists $leakedVolume) 'Fixture did not leak its anonymous volume.'

    # A stopped container still references its anonymous volume.
    $stopped = "${prefix}stopped"
    docker create --name $stopped --volume /data $image | Out-Null
    $created.Add($stopped)
    $stoppedVolume = docker inspect --format '{{range .Mounts}}{{.Name}}{{end}}' $stopped

    # A named volume is never removed, even when nothing references it.
    $named = "${prefix}named"
    docker volume create $named | Out-Null

    # Inside the grace period nothing is removed.
    Remove-DanglingAnonymousVolumes -MinimumAge ([TimeSpan]::FromHours(1)) | Out-Null
    Assert-True (Test-VolumeExists $leakedVolume) 'A volume inside the grace period was removed.'

    Assert-True ((Remove-DanglingAnonymousVolumes -MinimumAge ([TimeSpan]::Zero) -WhatIf) -ge 1) 'Dry run did not count the leaked volume.'
    Assert-True (Test-VolumeExists $leakedVolume) 'Dry run removed a volume.'

    Start-Sleep -Seconds 1
    $removedVolumes = Remove-DanglingAnonymousVolumes -MinimumAge ([TimeSpan]::Zero)
    Assert-True ($removedVolumes -ge 1) "Removed $removedVolumes volumes."
    Assert-True (-not (Test-VolumeExists $leakedVolume)) 'The leaked anonymous volume survived.'
    Assert-True (Test-VolumeExists $stoppedVolume) "A stopped container's volume was removed."
    Assert-True (Test-VolumeExists $named) 'A named volume was removed.'

    # Containers: only stopped ones with the prefix, outside the exclusions, past retention.
    $excluded = "${prefix}excluded"
    docker create --name $excluded $image | Out-Null
    $created.Add($excluded)
    $running = "${prefix}running"
    docker run --detach --name $running $image sleep 600 | Out-Null
    $created.Add($running)
    $foreign = "other-$prefix"
    docker create --name $foreign $image | Out-Null
    $created.Add($foreign)

    $arguments = @{ NamePrefix = $prefix; ExcludeNames = @($excluded) }
    Assert-True ((Remove-StaleRepositoryContainers @arguments -MinimumIdle ([TimeSpan]::FromHours(1))) -eq 0) 'A recently stopped container was removed.'
    Assert-True ((Remove-StaleRepositoryContainers @arguments -MinimumIdle ([TimeSpan]::Zero) -WhatIf) -eq 1) 'Dry run counted the wrong containers.'
    Assert-True (Test-ContainerExists $stopped) 'Dry run removed a container.'

    Start-Sleep -Seconds 1
    $removedContainers = Remove-StaleRepositoryContainers @arguments -MinimumIdle ([TimeSpan]::Zero)
    Assert-True ($removedContainers -eq 1) "Removed $removedContainers containers."
    Assert-True (-not (Test-ContainerExists $stopped)) 'The stale container survived.'
    Assert-True (-not (Test-VolumeExists $stoppedVolume)) "The stale container's anonymous volume survived."
    Assert-True (Test-ContainerExists $excluded) 'An excluded container was removed.'
    Assert-True (Test-ContainerExists $running) 'A running container was removed.'
    Assert-True (Test-ContainerExists $foreign) 'A container without the prefix was removed.'

    Write-Host 'Docker cleanup tests passed.'
}
finally {
    foreach ($name in $created) { docker rm --force --volumes $name 2>$null | Out-Null }
    docker volume rm "${prefix}named" 2>$null | Out-Null
}
