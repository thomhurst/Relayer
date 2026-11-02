using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams.Tests;

public class ConfigurationTests
{
    [Test]
    public async Task Registration_preserves_named_native_configuration_without_connecting()
    {
        var first = new StreamSystemConfig { UserName = "first" };
        var second = new StreamSystemConfig { UserName = "second" };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.RabbitMQStreams("first", streams => streams.Options.Configure(options => options.Configuration = first));
            builder.RabbitMQStreams("second", streams => streams.Options.Configure(options => options.Configuration = second));
        });
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<RabbitMqStreamsOptions>>();
        await Assert.That(options.Get("first").Configuration).IsSameReferenceAs(first);
        await Assert.That(options.Get("second").Configuration).IsSameReferenceAs(second);
        var transports = provider.GetServices<ITransport>().ToArray();
        await Assert.That(transports.Select(transport => transport.Name).ToArray()).IsEquivalentTo(new[] { "first", "second" });
        var capabilities = transports[0].GetCapabilities("orders");
        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Log);
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.Replay).IsFalse();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.MaxRedeliveryDelay).IsNull();
    }

    [Test]
    public async Task None_cancellation_and_disposal_do_not_connect()
    {
        await using var provider = Provider(options => options.Streams.Add(new("orders")));
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
    public async Task Message_execution_fails_before_connecting()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Send<Sample>("orders")));
        await using var provider = services.BuildServiceProvider();
        var error = await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        await Assert.That(error!.Message).Contains("reliable consumer recovery");
        var consumers = new ServiceCollection();
        consumers.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Consume("orders", consumer => consumer.Handler<SampleHandler>())));
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
    public async Task Invalid_declarations_fail_before_connecting(int scenario)
    {
        await using var provider = Provider(options =>
        {
            switch (scenario)
            {
                case 0: options.Configuration = null!; break;
                case 1: options.Streams.Add(new("")); break;
                case 2: options.Streams.Add(new(" ")); break;
                case 3: options.Streams.Add(new("orders")); options.Streams.Add(new("orders")); break;
                case 4: options.Streams.Add(new("orders") { Configuration = new("different") }); break;
            }
        });
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
    }

    internal static ServiceProvider Provider(Action<RabbitMqStreamsOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams => streams.Options.Configure(configure)));
        return services.BuildServiceProvider();
    }

    [Message("streams.configuration.sample")]
    public sealed record Sample;
    public sealed class SampleHandler : IHandler<Sample>
    {
        public ValueTask HandleAsync(Sample message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
