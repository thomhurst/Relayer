using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Nats;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsConfigurationTests
{
    [Test]
    public async Task Named_registration_preserves_native_options_without_connecting()
    {
        var first = new NatsOpts { Url = "nats://does-not-exist.invalid:4222", MaxReconnectRetry = 0 };
        var jetStream = new NatsJSOpts(first, domain: "test");
        var second = new NatsOpts { Url = "nats://also-does-not-exist.invalid:4222" };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.Nats("first", nats => nats.Options.Configure(options =>
            {
                options.ConnectionOptions = first;
                options.JetStreamOptions = jetStream;
            }));
            builder.Nats("second", nats => nats.Options.Configure(options => options.ConnectionOptions = second));
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<NatsTransportOptions>>();
        await Assert.That(options.Get("first").ConnectionOptions).IsSameReferenceAs(first);
        await Assert.That(options.Get("first").JetStreamOptions).IsSameReferenceAs(jetStream);
        await Assert.That(options.Get("second").ConnectionOptions).IsSameReferenceAs(second);
        var transports = provider.GetServices<ITransport>().ToArray();
        await Assert.That(transports.Select(transport => transport.Name).ToArray()).IsEquivalentTo(new[] { "first", "second" });
        await Assert.That(transports[0].GetCapabilities("orders").DeadLetter).IsFalse();
        await Assert.That(transports[0].GetCapabilities("orders").MaxDeliveryDelay).IsNull();
    }

    [Test]
    public async Task None_and_cancelled_operations_do_not_connect_and_disposal_is_idempotent()
    {
        await using var provider = Provider(options => options.ConnectionOptions = new() { Url = "nats://does-not-exist.invalid:4222" });
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count).IsEqualTo(0);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.StartAsync(cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Validate, cancelled.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync((TopologyMode)99, default)).Throws<ArgumentOutOfRangeException>();
        await transport.StopAsync(default);
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(async () => await transport.StartAsync(default)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.None, default)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Send_and_consumer_declarations_fail_before_connecting()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Nats("nats", nats => nats.Send<Sample>("orders")));
        await using var provider = services.BuildServiceProvider();
        var error = await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        await Assert.That(error!.Message).Contains("send and receive execution are not implemented");

        var consumers = new ServiceCollection();
        consumers.AddRelayer(builder => builder.Nats("nats", nats => nats.Consume("orders", consumer => consumer.Handler<SampleHandler>())));
        await using var consumerProvider = consumers.BuildServiceProvider();
        await Assert.That(() => consumerProvider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();

        await using var native = Provider(_ => { });
        var transport = native.GetRequiredService<ITransport>();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendBatchAsync(ReadOnlyMemory<OutgoingMessage>.Empty, default)).Throws<NotSupportedException>();
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
    public async Task Invalid_native_declarations_fail_before_connecting(int scenario)
    {
        await using var provider = Provider(options =>
        {
            switch (scenario)
            {
                case 0: options.ConnectionOptions = null!; break;
                case 1: options.Streams.Add(new("")); break;
                case 2: options.Streams.Add(new("orders.*")); break;
                case 3: options.Streams.Add(new("orders")); options.Streams.Add(new("orders")); break;
                case 4: options.Streams.Add(new("orders") { Configuration = new("different", ["orders.>"]) }); break;
                case 5: options.Consumers.Add(new("orders", "handler") { Configuration = new("different") }); break;
                case 6: options.Consumers.Add(new("orders", "handler")); options.Consumers.Add(new("orders", "handler")); break;
                case 7: options.Consumers.Add(new("orders", "handler") { Configuration = new("handler") { DurableName = "different" } }); break;
            }
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    internal static ServiceProvider Provider(Action<NatsTransportOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Nats("nats", nats => nats.Options.Configure(configure)));
        return services.BuildServiceProvider();
    }

    [Message("nats.configuration.sample")]
    public sealed record Sample;

    public sealed class SampleHandler : IHandler<Sample>
    {
        public ValueTask HandleAsync(Sample message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
