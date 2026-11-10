using System.Text.Json;

namespace Relayer.CoreParallelismProbe;

// This executable deliberately fails two setups. Run only through
// scripts/Test-CoreBrokerParallelism.ps1, which verifies the exact outcomes.
[ClassDataSource<FixtureLedger>(Shared = SharedType.PerTestSession)]
[DependsOn<FailedSetupTests>(ProceedOnFailure = true)]
[DependsOn<CancelledSetupTests>(ProceedOnFailure = true)]
public sealed class FirstFixtureTests(FixtureLedger ledger)
{
    [Before(Test)] public Task SetUp() => ledger.EnterAsync();
    [After(Test)] public void TearDown() => ledger.Exit();

    [Test, Arguments(0), Arguments(1), Arguments(2)]
    public void Broker_user(int id) => ledger.CompleteBody();
}

[ClassDataSource<FixtureLedger>(Shared = SharedType.PerTestSession)]
[DependsOn<FailedSetupTests>(ProceedOnFailure = true)]
[DependsOn<CancelledSetupTests>(ProceedOnFailure = true)]
public sealed class SecondFixtureTests(FixtureLedger ledger)
{
    [Before(Test)] public Task SetUp() => ledger.EnterAsync();
    [After(Test)] public void TearDown() => ledger.Exit();

    [Test, Arguments(0), Arguments(1), Arguments(2)]
    public void Broker_user(int id) => ledger.CompleteBody();
}

[ClassDataSource<FixtureLedger>(Shared = SharedType.PerTestSession)]
public sealed class FailedSetupTests(FixtureLedger ledger)
{
    [Before(Test)]
    public async Task SetUp()
    {
        await ledger.EnterAsync();
        throw new InvalidOperationException("Intentional fixture setup failure");
    }

    [After(Test)] public void TearDown() => ledger.Exit();
    [Test] public void Setup_fails() => ledger.UnexpectedBody();
}

[ClassDataSource<FixtureLedger>(Shared = SharedType.PerTestSession)]
public sealed class CancelledSetupTests(FixtureLedger ledger)
{
    [Before(Test)]
    public async Task SetUp()
    {
        await ledger.EnterAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        cancellation.Token.ThrowIfCancellationRequested();
    }

    [After(Test)] public void TearDown() => ledger.Exit();
    [Test] public void Setup_is_cancelled() => ledger.UnexpectedBody();
}

public sealed class FixtureLedger : IDisposable
{
    private readonly TaskCompletionSource[] _pairs = Enumerable.Range(0, 4)
        .Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
    private int _entered;
    private int _active;
    private int _peak;
    private int _exited;
    private int _bodies;
    private int _unexpectedBodies;

    public async Task EnterAsync()
    {
        var active = Interlocked.Increment(ref _active);
        int peak;
        do { peak = Volatile.Read(ref _peak); }
        while (active > peak && Interlocked.CompareExchange(ref _peak, active, peak) != peak);
        if (active > 2) throw new InvalidOperationException("More than two fixtures entered concurrently");

        var entry = Interlocked.Increment(ref _entered) - 1;
        var pair = _pairs[entry / 2];
        if (entry % 2 == 1) pair.TrySetResult();
        // Each pair must overlap. No sleeps or timing-based sampling. A leaked
        // framework permit leaves the next pair unable to enter and fails here.
        await pair.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

    public void CompleteBody()
    {
        if (Volatile.Read(ref _exited) < 2)
            throw new InvalidOperationException("Failed setups must finish before subsequent fixture users");
        Interlocked.Increment(ref _bodies);
    }

    public void Exit()
    {
        Interlocked.Decrement(ref _active);
        Interlocked.Increment(ref _exited);
    }

    public void UnexpectedBody()
    {
        Interlocked.Increment(ref _unexpectedBodies);
        throw new InvalidOperationException("Body must not run after setup failure");
    }

    public void Dispose()
    {
        var output = Environment.GetEnvironmentVariable("RELAYER_PARALLELISM_PROBE_RESULT")
            ?? throw new InvalidOperationException("Run scripts/Test-CoreBrokerParallelism.ps1");
        File.WriteAllText(output, JsonSerializer.Serialize(new
        {
            Entered = _entered, Exited = _exited, Active = _active, Peak = _peak,
            Bodies = _bodies, UnexpectedBodies = _unexpectedBodies
        }));
    }
}
