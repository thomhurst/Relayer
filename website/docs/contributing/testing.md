---
title: Running repository tests
description: Release validation and the core suite's broker concurrency bound.
---

Run the core suite from the repository root with .NET SDK selected by `global.json`
and Docker running:

```bash
dotnet test -c Release --project tests/Relayer.Tests
```

The core assembly uses TUnit's `ParallelLimiter<CoreTestParallelLimit>` to run at
most **two tests concurrently across all classes**. This includes Service Bus
emulators and their SQL dependencies, RabbitMQ and LocalStack fixtures. The permit
covers setup, assertions and teardown, so containers still serving assertions count
toward the bound. Nonbroker core tests also share this conservative limit. Keep
broker fixtures within the test lifecycle and avoid overriding the assembly limiter
on individual classes or methods.

This is a limit per test process, not a Docker host budget. Other test assemblies,
concurrent commands and other workers have independent limits. Each fixture retains
its own isolated infrastructure and cleans up only its own resources. Provisioning
and operation deadlines are unchanged; the bound does not retry failed tests.

To verify the scheduler without starting Docker containers, run:

```powershell
pwsh scripts/Test-CoreBrokerParallelism.ps1 -ResultsDirectory /path/to/results
```

The separate probe executable links the core assembly's actual limiter declaration.
Paired fixture users synchronize without sleeps, across independent classes, and
record peak concurrency and teardown. Two setups deliberately fail (one through
cancellation); the script checks their structured test results and verifies that
all other users finish and every fixture releases its slot. The probe is kept outside
the normal suite because these expected failures must not count as suite failures.
