using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
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
        await Assert.That(async () => await sender.SendWithoutOutboxAsync(new OrderPlaced(Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("requires an active outbox transaction");
        await Assert.That(broker.Count + local.Count + outbox.Rows.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Startup_rejects_fan_out_without_a_compatible_outbox(bool registerOutbox, bool nativeFirst)
    {
        var builder = Host.CreateApplicationBuilder();
        var broker = new CountingTransport("broker", transactions: true);
        var local = new CountingTransport("local");
        builder.Services.AddSingleton<ITransport>(broker);
        builder.Services.AddSingleton<ITransport>(local);

        if (registerOutbox)
        {
            builder.Services.AddScoped<IOutboxEnlistment>(_ => new RecordingOutbox { IsActive = true });
        }

        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();

            if (!nativeFirst)
            {
                AddSelectorRoute(relayer);
            }

            relayer.DeclareRoute<OrderPlaced>("broker", "orders", route => route.SendOptions = new()
            {
                TransactionRequirement = registerOutbox ? SendTransactionRequirement.Native : SendTransactionRequirement.None
            });

            if (nativeFirst)
            {
                AddSelectorRoute(relayer);
            }
        });
        using var host = builder.Build();
        var expected = registerOutbox ? "TransactionRequirement.Native is incompatible with outbox fan-out" : "Register an outbox integration with IOutboxEnlistment";

        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>().WithMessageContaining(expected);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Typed_tombstones_reject_fan_out_before_any_send(bool allRoutesSupportTombstones)
    {
        var (provider, broker, local, outbox) = Build(active: true, localSupportsTombstones: allRoutesSupportTombstones);
        using var _ = provider;
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();

        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
        await Assert.That(outbox.Rows.Count).IsEqualTo(2);

        var tombstone = Tombstone.For<OrderPlaced>("customer-1");
        await Assert.That(async () => await sender.SendWithoutOutboxAsync(tombstone))
            .Throws<InvalidOperationException>().WithMessageContaining("Typed tombstones require a single fixed send route");
        await Assert.That(async () => await sender.SendAsync(tombstone))
            .Throws<InvalidOperationException>().WithMessageContaining("Typed tombstones require a single fixed send route");
        await Assert.That(broker.Count + local.Count).IsEqualTo(0);
        await Assert.That(outbox.Rows.Count).IsEqualTo(2);
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

    private static void AddSelectorRoute(IRelayerBuilder relayer)
    {
        relayer.AddTransport<SenderTransportOptions, SenderTransportOptions>("local", transport =>
            transport.Send<OrderPlaced>(["local-a", "local-b"], static _ => "local-b"));
    }

    private static (ServiceProvider, CountingTransport, CountingTransport, RecordingOutbox) Build(bool active, bool localSupportsTombstones = false)
    {
        var broker = new CountingTransport("broker", tombstones: true);
        var local = new CountingTransport("local", tombstones: localSupportsTombstones);
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

    private sealed class CountingTransport(string name, bool transactions = false, bool tombstones = false) : ITransport
    {
        public int Count
        {
            get; private set;
        }

        public string Name => name;

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new()
            {
                OrderedByKey = true,
                Transactions = transactions,
                Tombstones = tombstones
            };
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Count++;
            return ValueTask.CompletedTask;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(new TopologyReport([]));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed record Row(Guid Id, string Transport, string Destination, string? OrderingKey, Dictionary<string, string> Headers);

    private sealed class RecordingOutbox : IOutboxEnlistment
    {
        public bool IsActive
        {
            get; init;
        }

        public List<Row> Rows
        {
            get;
        } = [];

        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            var headers = new Dictionary<string, string>();

            foreach (var header in record.Message.Headers.Span)
            {
                headers.Add(header.Key, header.Value.ToString());
            }

            Rows.Add(new(record.Id, record.TransportName, record.Message.Destination, record.Message.OrderingKey?.ToString(), headers));
            return ValueTask.CompletedTask;
        }
    }
}
