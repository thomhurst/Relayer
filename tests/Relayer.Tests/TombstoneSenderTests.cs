using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Tests;

public class TombstoneSenderTests
{
    [Test]
    public async Task Tombstones_reuse_routes_preserve_keys_in_buffer_and_reject_outbox_enlistment()
    {
        var sink = new Sink();
        var outbox = new Outbox();
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(sink);
        services.AddSingleton<IOutboxEnlistment>(outbox);
        services.AddRelayer(r =>
        {
            r.Infrastructure().MessageTypes.Add<Payload>("tests.tombstone-send", 7);
            r.AddTransport<Options, Options, Options>("sink", t => t.Send<Payload>("orders"));
            r.AddMessageTypesFromThisAssembly();
        });
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var sender = (Sender)scope.ServiceProvider.GetRequiredService<ISender>();
        var tombstone = Tombstone.For<Payload>("customer-1");
        sender.AttemptBuffer = new AttemptSendBuffer();
        await sender.SendAsync(tombstone);
        await Assert.That(sink.Sends).IsEqualTo(0);
        await sender.AttemptBuffer.FlushAsync(default);
        sender.AttemptBuffer.Clear();
        sender.AttemptBuffer = null;
        await Assert.That(sink.Sends).IsEqualTo(1);
        await Assert.That(sink.Key).IsEqualTo("customer-1");
        await Assert.That(sink.WireType).IsEqualTo("tests.tombstone-send;v=7");
        outbox.IsActive = true;
        await Assert.That(async () => await sender.SendAsync(tombstone)).Throws<InvalidOperationException>().WithMessageContaining("tombstones");
        await Assert.That(async () => await sender.SendViaOutboxAsync(tombstone)).Throws<InvalidOperationException>();
        await sender.SendNowAsync(tombstone);
        await Assert.That(sink.Sends).IsEqualTo(2);
        await Assert.That(outbox.Enlisted).IsFalse();
        // An empty byte key remains a non-null native key.
        await sender.SendNowAsync(Tombstone.For<Payload>(ReadOnlyMemory<byte>.Empty));
        await Assert.That(sink.Key).IsEqualTo("");
        await Assert.That(() => Tombstone.For<Payload>((string)null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Explicit_tombstone_routes_fail_during_composition()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new Sink());
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.AddTransport<Options, Options, Options>("sink", t => t.Send<Tombstone<Payload>>("orders"));
        });
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        await Assert.That(() => scope.ServiceProvider.GetRequiredService<ISender>())
            .Throws<InvalidOperationException>().WithMessageContaining("automatically reuse");
    }

    [Test]
    public async Task Unsupported_transport_has_no_implicit_tombstone_route()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new Sink { SupportsTombstones = false });
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.AddTransport<Options, Options, Options>("sink", t => t.Send<Payload>("orders"));
        });
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await Assert.That(async () => await sender.SendAsync(Tombstone.For<Payload>("key")))
            .Throws<InvalidOperationException>().WithMessageContaining("No send route");
    }

    [Message("tests.tombstone-send", Version = 7)]
    public sealed record Payload(int Id);
    public sealed class Options;
    private sealed class Outbox : IOutboxEnlistment
    {
        public bool IsActive { get; set; }
        internal bool Enlisted;
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken token) { Enlisted = true; return default; }
    }
    private sealed class Sink : ITransport
    {
        internal bool SupportsTombstones = true;
        internal int Sends;
        internal string? Key;
        internal string? WireType;
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { Tombstones = SupportsTombstones, OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            if (!message.IsTombstone || !message.Body.IsEmpty || message.OrderingKey is null)
                throw new InvalidOperationException("Typed tombstone lost native state.");
            Sends++;
            Key = message.OrderingKey.Value.ToString();
            foreach (var header in message.Headers.Span)
                if (header.Key == "ce_type") WireType = header.Value.ToString();
            return default;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
