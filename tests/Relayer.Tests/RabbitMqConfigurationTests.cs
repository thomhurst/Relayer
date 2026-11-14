using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqConfigurationTests
{
    [Test]
    public async Task Named_registration_preserves_native_factory_and_does_not_connect_on_resolution()
    {
        var first = new ConnectionFactory { HostName = "does-not-exist.invalid", AutomaticRecoveryEnabled = false };
        var second = new ConnectionFactory { HostName = "also-does-not-exist.invalid" };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.RabbitMQ("first", rabbit => rabbit.Options.Configure(options => options.ConnectionFactory = first));
            builder.RabbitMQ("second", rabbit => rabbit.Options.Configure(options => options.ConnectionFactory = second));
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<RabbitMqTransportOptions>>();
        await Assert.That(options.Get("first").ConnectionFactory).IsSameReferenceAs(first);
        await Assert.That(options.Get("second").ConnectionFactory).IsSameReferenceAs(second);
        var transports = provider.GetServices<ITransport>().ToArray();
        await Assert.That(transports.Select(item => item.Name).ToArray()).IsEquivalentTo(new[] { "first", "second" });
        await Assert.That(first.AutomaticRecoveryEnabled).IsFalse();
        await Assert.That(second.AutomaticRecoveryEnabled).IsTrue();
        await Assert.That(transports[0].GetCapabilities("orders").MaxDeliveryDelay).IsNull();
        await Assert.That(transports[0].GetCapabilities("orders").DeadLetter).IsFalse();
    }

    [Test]
    public async Task None_and_cancelled_startup_do_not_connect_and_disposal_is_idempotent()
    {
        await using var provider = Provider(options => options.ConnectionFactory.HostName = "does-not-exist.invalid");
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count).IsEqualTo(0);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.StartAsync(cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Validate, cancelled.Token)).Throws<OperationCanceledException>();
        await transport.StopAsync(default);
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(async () => await transport.StartAsync(default)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.None, default)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Send_and_consume_declarations_resolve_without_connecting()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQ("rabbit", rabbit => rabbit.Send<Sample>("orders")));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetRequiredService<ITransport>().GetCapabilities("orders").MaxDeliveryDelay).IsNull();

        var consumers = new ServiceCollection();
        consumers.AddRelayer(builder => builder.RabbitMQ("rabbit", rabbit => rabbit.Consume("orders", consumer => consumer.Handler<SampleHandler>())));
        await using var consumerProvider = consumers.BuildServiceProvider();
        await Assert.That(consumerProvider.GetRequiredService<ITransport>().Name).IsEqualTo("rabbit");
    }

    [Test]
    public async Task Default_failure_destination_uses_the_default_exchange_for_queue_names_containing_colons()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQ("rabbit", rabbit =>
            rabbit.Consume("orders:eu", consumer => consumer.Handler<SampleHandler>())));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That(transport.GetCapabilities(":orders:eu.dead-letter").DeadLetter).IsFalse();
    }

    [Test]
    public async Task Direct_send_requires_a_declared_destination()
    {
        await using var provider = Provider(_ => { });
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), default)).Throws<InvalidOperationException>();
        await transport.SendBatchAsync(ReadOnlyMemory<OutgoingMessage>.Empty, default);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    [Arguments(6)]
    [Arguments(7)]
    [Arguments(8)]
    [Arguments(9)]
    public async Task Invalid_native_declarations_fail_before_connecting(int scenario)
    {
        await using var provider = Provider(options =>
        {
            switch (scenario)
            {
                case 0: options.ConnectionFactory = null!; break;
                case 1: options.Exchanges.Add(new("")); break;
                case 2: options.Queues.Add(new("")); break;
                case 3: options.Bindings.Add(new("orders", "", "")); break;
                case 4: options.Queues.Add(new("orders")); options.Queues.Add(new("orders")); break;
                case 5: options.Exchanges.Add(new("orders") { Type = "" }); break;
                case 6: options.PublisherCount = 0; break;
                case 7: options.PublisherChannelOptions = new(false, true); break;
                case 8: options.PublisherChannelOptions = new(true, false); break;
                case 9: options.DelayedExchanges.Add("", new("delayed") { Type = "direct" }); break;
            }
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Unsupported_consumer_declarations_fail_before_connecting(int scenario)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQ("rabbit", rabbit =>
        {
            rabbit.Options.Configure(options => options.ConnectionFactory.HostName = "does-not-exist.invalid");
            rabbit.Consume("orders", consumer =>
            {
                consumer.Handler<SampleHandler>();
                if (scenario == 0) consumer.Concurrency(2);
                if (scenario == 1) consumer.Batch(2, TimeSpan.FromSeconds(1));
                if (scenario == 2) consumer.Failures(new() { RedeliveryDelays = new[] { TimeSpan.Zero } });
            });
        }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<Exception>();
    }

    internal static ServiceProvider Provider(Action<RabbitMqTransportOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQ("rabbit", rabbit => rabbit.Options.Configure(configure)));
        return services.BuildServiceProvider();
    }

    [Message("rabbitmq.configuration.sample")]
    public sealed record Sample;

    public sealed class SampleHandler : IHandler<Sample>
    {
        public ValueTask HandleAsync(Sample message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
