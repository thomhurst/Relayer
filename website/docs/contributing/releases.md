---
title: Packages and releases
description: "Build, inspect and publish Relayer packages from version tags."
---

Relayer publishes pre-1.0 packages to [NuGet](https://www.nuget.org/packages/Relayer) from published GitHub releases. Some transport
packages still contain scaffolding. Check the [roadmap](../roadmap.md) before choosing a transport.

## Dependency updates

Renovate opens dependency updates as draft pull requests. It cannot merge updates itself or arm
GitHub auto-merge. The repository configuration applies this policy to all packages, overriding
inherited package rules that enable auto-merge.

Dependency updates require the same review as other changes: Claude and Codex must each approve
the exact current head, and every review finding must have a disposition. Claude reviews drafts
automatically, including Renovate branches and dependency bot actors. Request Codex on a draft
with `@codex review`, following the repository's issue/PR loop instructions. After both reviews
approve that head, promote and arm the PR with `scripts/Enable-AutoMerge.ps1 -Pr <N> -MarkReady`.
GitHub merges only after the protected `CI Passed` check succeeds. A new push requires a new review
cycle before auto-merge can be armed again.

This configuration does not convert existing PRs to drafts or cancel an auto-merge request already
registered with GitHub. When taking over an older dependency PR, disable any existing auto-merge
request and restore the draft under its PR lock before applying this review lifecycle.

## Package contents

Every packable project under `src/` includes the repository README, the Relayer icon, MIT license
metadata and the source repository commit. The SDK supplies GitHub SourceLink, and each package has
a matching `.snupkg` containing portable PDBs. Debuggers can retrieve the source for that exact commit.

The packages are `Relayer`, `Relayer.Testing`, `Relayer.AspNetCore`, `Relayer.Aspire`, `Relayer.HealthChecks`,
`Relayer.Kafka`, `Relayer.Kafka.HealthChecks`, `Relayer.AzureServiceBus`, `Relayer.AzureServiceBus.HealthChecks`,
`Relayer.AmazonSqs`, `Relayer.AmazonSqs.HealthChecks`, `Relayer.RabbitMQ`, `Relayer.RabbitMQ.HealthChecks`,
`Relayer.Nats`, `Relayer.GooglePubSub`, `Relayer.Outbox.PostgreSql`, `Relayer.Outbox.SqlServer`, `Relayer.Outbox.MySql`,
`Relayer.Outbox.EntityFrameworkCore`, `Relayer.Outbox.PostgreSql.EntityFrameworkCore`, `Relayer.Sagas`,
`Relayer.Sagas.EntityFrameworkCore` and `Relayer.Cli`, the tool that installs the `dotnet-relayer` command.
Every package ID starts with `Relayer`, because the nuget.org trusted publishing policy only accepts that pattern.
`Relayer.Kafka.SchemaRegistry`, `Relayer.Serialization.Protobuf`, `Relayer.Serialization.MessagePack` and
`Relayer.Kafka.Serialization.Avro` are optional integration packages.
`Relayer.Sagas` provides [process diagram declarations and export](../guides/saga-diagrams.md) and
typed execution. [Transactional state transitions](../guides/sagas.md) use the optional EF Core
integration; [durable process timeouts](../guides/saga-timeouts.md) reuse transactional scheduled sends.
The source generator and code fixes ship inside `Relayer`; they are not separate packages.
`Relayer.RabbitMQ.Streams` is not packaged or published until native send and receive are supported;
its source and tests remain in the repository.

## Versions and local verification

MinVer derives package and assembly versions from Git tags for every build except a release, which
takes its version from the release's tag. Tags have the form `v1.2.3` or `v1.2.3-preview.1`. Untagged
commits get a preview version with a commit height, starting at `0.1.0` before the first release. Use a
full checkout with tags; shallow history can produce a different version. Do not edit a project version
to make a release. Build metadata (`+...`), a missing `v` prefix, leading zeros in numeric version
identifiers, empty prerelease identifiers and version parts above 65534 (the file version limit) are not
accepted by the release workflow.

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

The `Release Draft` workflow (`release-draft.yml`) keeps one draft GitHub release for `main`, rewritten
on every push. Its tag is one patch above the latest release (`v0.2.0` suggests `v0.2.1`), however many
commits were merged; after a prerelease such as `v1.0.0-rc.1` it suggests `v1.0.0`. The latest release
is the highest version tag in `main`'s history, so tags pushed before drafts were introduced count. Its
target is the newest commit on `main`, and its notes are GitHub's generated notes since that release.
Commit messages do not choose the version. The workflow recognizes its draft by a hidden marker
(`<!-- relayer-release-draft -->`) at the end of the notes and never changes other drafts or releases.
Keep the marker when editing the notes: without it, the next push creates a second draft. The workflow
also leaves the draft alone when a tag or release already exists for the suggested version.

To release, wait for `CI Passed` on the draft's commit, then edit the draft on GitHub and publish it.
Change the draft's tag to override the version (for example `v0.3.0` or `v1.0.0`, or a prerelease such as
`v1.0.0-rc.1`, marked as a pre-release); later pushes keep a higher version you set. Set it back to the
suggestion (or lower) to return to the default. A push to `main` before publishing retargets the draft at
the new commit and regenerates its notes, discarding edits to the notes. Publishing creates the tag on the
draft's commit and starts the `Release NuGet packages` workflow (`release.yml`).

The notes are grouped by PR label, as configured in `.github/release.yml`: Breaking Changes, New Features,
Bug Fixes, Performance, Documentation, Other Changes and Dependencies. Label a PR `breaking` for a breaking
change, so it is listed first, or `ignore-for-release` to leave it out. Dependency PRs carry the
`dependencies` label from Renovate and are grouped together automatically, last.

That workflow rejects malformed tags, commits outside `main`, and commits without a successful
`CI Passed` check from GitHub Actions. It packs with full Git history and the tag's version, verifies
that every artifact version matches the tag and uploads the `.nupkg` and `.snupkg` artifacts before
requesting publishing credentials. It then pushes packages and symbols to nuget.org. Once the packages
are pushed, it renames the release after its tag and points the generated **Full Changelog** link at that
tag if the tag was edited; other text in the notes is left as written.

The workflow uses `--skip-duplicate` and explicitly pushes symbols as well as packages. To finish a
failed or partial release, re-run that workflow run: packages already on nuget.org are skipped and
nothing is overwritten. Never move a release tag to change an existing NuGet version: use a new version instead.

Releases run one at a time. New releases leave the active release running and can queue up to
100 pending releases. GitHub Actions cancels additional runs if the queue is full.
