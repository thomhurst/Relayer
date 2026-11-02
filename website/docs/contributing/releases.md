---
title: Packages and releases
description: "Build, inspect and publish Relayer packages from version tags."
---

Relayer has packaging and release infrastructure. Packages are not released yet, and some transport
packages still contain scaffolding. Check the [roadmap](../roadmap.md) before choosing a transport.

## Package contents

Every packable project under `src/` includes the repository README, the Relayer icon, MIT license
metadata and the source repository commit. The SDK supplies GitHub SourceLink, and each package has
a matching `.snupkg` containing portable PDBs. Debuggers can retrieve the source for that exact commit.

The packages are `Relayer`, `Relayer.Testing`, `Relayer.HealthChecks`, `Relayer.Kafka`,
`Relayer.Kafka.HealthChecks`, `Relayer.AzureServiceBus`, `Relayer.AmazonSqs`, `Relayer.RabbitMQ`,
`Relayer.Outbox.PostgreSql`, `Relayer.Outbox.EntityFrameworkCore`, `Relayer.Sagas`,
`Relayer.Sagas.EntityFrameworkCore` and the `dotnet-relayer` tool.
`Relayer.Kafka.SchemaRegistry`, `Relayer.Serialization.Protobuf` and `Relayer.Serialization.Avro` are optional integration packages.
`Relayer.Sagas` provides [process diagram declarations and export](../guides/saga-diagrams.md) and
typed execution. [Transactional state transitions](../guides/sagas.md) use the optional EF Core
integration; durable process timeout handling remains planned.
The source generator and code fixes ship inside `Relayer`; they are not separate packages.

## Versions and local verification

MinVer derives all package and assembly versions from Git tags. Tags have the form `v1.2.3` or
`v1.2.3-preview.1`. Untagged commits get a preview version with a commit height, starting at `0.1.0`
before the first release. Use a full checkout with tags; shallow history can produce a different version.
Do not edit a project version to make a release. Build metadata (`+...`) and leading zeros in numeric
version identifiers are not accepted by the release workflow.

Before the first publish, pack and verify locally with the SDK selected by `global.json`:

```powershell
dotnet pack -c Release -o .artifacts/packages -p:ContinuousIntegrationBuild=true
python .github/scripts/verify_packages.py .artifacts/packages --commit (git rev-parse HEAD)
```

Use an empty output directory so an earlier version cannot be mistaken for the current build.
`ContinuousIntegrationBuild=true` normalizes source paths for reproducible builds; GitHub Actions
sets this automatically. Deterministic compilation is enabled for all builds.

The verification checks the full package inventory, matching versions and internal dependencies,
README and PNG icon assets, repository commits, portable symbols, SourceLink and bundled compiler
components. CI runs this check after building and testing.

## One-time publishing setup

The publishing job uses the GitHub environment `release`, `id-token: write` and `NuGet/login@v1`
to exchange GitHub OIDC credentials for a short-lived NuGet API key. No permanent NuGet API key is needed.

A maintainer must configure these before publishing:

1. Create the `release` GitHub environment. Restrict its deployment tags and add required reviewers
   if releases need a separate approval.
2. Add the environment secret `NUGET_USER` containing the nuget.org profile name, not an email address.
3. Create a [NuGet trusted publishing policy](https://www.nuget.org/account/trustedpublishing) with
   repository owner `thomhurst`, repository `Relayer`, workflow file **`release.yml`** and environment
   `release`. The policy uses the exact filename, not the workflow display name or its full path.
4. Ensure that the policy account owns the existing package IDs and that its scopes permit the
   intended publishing operations, including new packages for an initial release.

See [NuGet's trusted publishing documentation](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing)
for account and policy requirements. Confirm the policy is configured before triggering a release.

## Release behavior

After publishing setup is confirmed, a maintainer selects a commit on `main` whose `CI Passed` check
has succeeded, creates a version tag on that commit and pushes that tag. Annotated and lightweight
tags both work. No release tags are created automatically.

The `Release NuGet packages` workflow rejects malformed tags, commits outside `main`, and commits
without a successful `CI Passed` check from GitHub Actions. It packs with full Git history, verifies
that every artifact version matches the tag and uploads the `.nupkg` and `.snupkg` artifacts before
requesting publishing credentials. It then pushes packages and symbols to nuget.org.

The workflow uses `--skip-duplicate` and explicitly pushes symbols as well as packages. A rerun can
finish a partially published version without overwriting existing packages. Never move a release
tag to change an existing NuGet version: use a new version instead. Releases are serialized and an
active release is not cancelled by another tag push.
