namespace Relayer.Kafka.Tests;

public class NativeLifecycleProxyTests
{
    [Test]
    public async Task Topic_facets_share_disposal_boundary_and_forward_real_calls()
    {
        var native = new Calls();
        var probe = new NativeLifecycleProbe();
        var client = NativeLifecycleProxy.Wrap<ICalls>(native, probe);
        var facet = client.ForTopic();
        await client.DisposeAsync();
        await facet.CommitAsync();
        await Assert.That(native.Disposed).IsTrue();
        await Assert.That(native.Commits).IsEqualTo(1);
        await Assert.That(() => probe.AssertClosed()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Uninstrumented_disposal_and_failed_commits_cannot_supply_evidence()
    {
        var probe = new NativeLifecycleProbe();
        await Assert.That(() => probe.AssertClosed()).Throws<InvalidOperationException>();
        var failure = new IOException("broker rejected commit");
        var native = new Calls { Failure = failure };
        var client = NativeLifecycleProxy.Wrap<ICalls>(native, probe);
        var error = await Assert.That(async () => await client.CommitAsync()).Throws<IOException>();
        await Assert.That(error).IsSameReferenceAs(failure);
        await Assert.That(probe.Commits).IsEqualTo(0);
    }

    // Public for DispatchProxy's generated forwarding type; these are probe tests, not broker conformance.
    public interface ICalls : IAsyncDisposable
    {
        ICalls ForTopic();
        ValueTask CommitAsync();
    }

    private sealed class Calls : ICalls
    {
        internal bool Disposed;
        internal int Commits;
        internal Exception? Failure;
        public ICalls ForTopic() => this;
        public ValueTask CommitAsync()
        {
            if (Failure is not null) return ValueTask.FromException(Failure);
            Commits++;
            return default;
        }
        public ValueTask DisposeAsync() { Disposed = true; return default; }
    }
}
