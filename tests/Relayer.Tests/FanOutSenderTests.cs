using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Tests;

public class FanOutSenderTests
{
    [Test]
    public async Task Outbox_send_enlists_one_row_per_route_with_a_shared_envelope()
    {
        var (provider, broker, local, outbox) = Build(active: true);
        using var _ = provider;
        using var scope = provider.CreateScope();
        var order = new OrderPlaced(Guid.NewGuid());

        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(order);

        await Assert.That(outbox.Rows.Count).IsEqualTo(2);
        var (first, second) = (outbox.Rows[0], outbox.Rows[1]);
        await Assert.That((first.Transport, first.Destination)).IsEqualTo(("broker", "orders"));
        await Assert.That((second.Transport, second.Destination)).IsEqualTo(("local", "order-handlers"));
        await Assert.That(second.Headers["ce_id"]).IsEqualTo(first.Headers["ce_id"]);
        await Assert.That(second.Headers["ce_time"]).IsEqualTo(first.Headers["ce_time"]);
        await Assert.That(first.Id.ToString("D")).IsEqualTo(first.Headers["ce_id"]);
        await Assert.That(second.Id).IsNotEqualTo(first.Id);
        await Assert.That(second.Id.Version).IsEqualTo(7);
        await Assert.That(first.OrderingKey).IsEqualTo(order.OrderId.ToString());
        await Assert.That(second.OrderingKey).IsNull();
        await Assert.That(broker.Count + local.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Direct_send_to_several_routes_is_rejected_before_any_send()
    {
        var (provider, broker, local, outbox) = Build(active: false);
        using var _ = provider;
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();

        await Assert.That(async () => await sender.SendAsync(new OrderPlaced(Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("requires an active outbox transaction");
        await Assert.That(async () => await sender.SendNowAsync(new OrderPlaced(Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("requires an active outbox transaction");
        await Assert.That(broker.Count + local.Count + outbox.Rows.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Repeated_destination_is_rejected_when_the_sender_is_resolved_without_a_host()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(new CountingTransport("broker"));
        services.AddScoped<IOutboxEnlistment>(_ => new RecordingOutbox { IsActive = true });
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.DeclareRoute<OrderPlaced>("broker", "orders");
            builder.DeclareRoute<OrderPlaced>("broker", "orders");
        });
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();

        await Assert.That(() => scope.ServiceProvider.GetRequiredService<ISender>())
            .Throws<InvalidOperationException>().WithMessageContaining("'orders' is declared more than once");
    }

    private static (ServiceProvider, CountingTransport, CountingTransport, RecordingOutbox) Build(bool active)
    {
        var broker = new CountingTransport("broker");
        var local = new CountingTransport("local");
        var outbox = new RecordingOutbox { IsActive = active };
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(broker);
        services.AddSingleton<ITransport>(local);
        services.AddScoped<IOutboxEnlistment>(_ => outbox);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.DeclareRoute<OrderPlaced>("broker", "orders", route => route.OrderingKey(static order => order.OrderId.ToString()));
            builder.DeclareRoute<OrderPlaced>("local", "order-handlers");
        });
        return (services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }), broker, local, outbox);
    }

    private sealed class CountingTransport(string name) : ITransport
    {
        public int Count { get; private set; }
        public string Name => name;
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Count++;
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed record Row(Guid Id, string Transport, string Destination, string? OrderingKey, Dictionary<string, string> Headers);

    private sealed class RecordingOutbox : IOutboxEnlistment
    {
        public bool IsActive { get; init; }
        public List<Row> Rows { get; } = [];
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            var headers = new Dictionary<string, string>();
            foreach (var header in record.Message.Headers.Span) headers.Add(header.Key, header.Value.ToString());
            Rows.Add(new(record.Id, record.TransportName, record.Message.Destination, record.Message.OrderingKey?.ToString(), headers));
            return ValueTask.CompletedTask;
        }
    }
}
