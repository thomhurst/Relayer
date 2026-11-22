using System.Text.Json.Serialization;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusConsumerControlTests
{
    [Test]
    public async Task Deferred_consumers_start_and_stop_through_the_keyed_control()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, autoStart: false);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");

        await ServiceBusReceiveTests.Start(provider);
        var processor = client.Processors.Single();
        await Assert.That(processor.Starts).IsEqualTo(0);

        await control.StartAsync(default);
        await control.StartAsync(default);
        await Assert.That(processor.Starts).IsEqualTo(1);

        await control.StopAsync(default);
        await control.StopAsync(default);
        await Assert.That(processor.Stops).IsEqualTo(1);

        await control.StartAsync(default);
        await Assert.That(processor.Starts).IsEqualTo(2);
        await ServiceBusReceiveTests.Stop(provider);
        await Assert.That(processor.Stops).IsEqualTo(2);
    }

    [Test]
    public async Task Failed_start_stops_siblings_and_preserves_the_start_error_when_rollback_fails()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Channel.CreateUnbounded<int>());
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<DeferredPayload>("servicebus.deferred-start");
            builder.UseJsonSerializer(ServiceBusConsumerControlJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.AutoStartConsumers = false;
                });
                bus.Consume("first", consumer => consumer.Handler<DeferredHandler>());
                bus.Consume("second", consumer => consumer.Handler<DeferredHandler>());
                bus.Consume("third", consumer => consumer.Handler<DeferredHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        await ServiceBusReceiveTests.Start(provider);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");

        var first = client.Processors.Single(processor => processor.Source == "first");
        var second = client.Processors.Single(processor => processor.Source == "second");
        var third = client.Processors.Single(processor => processor.Source == "third");
        var startError = new InvalidOperationException("Third processor failed to start.");
        first.Drain = Task.FromException(new InvalidOperationException("First processor failed to stop."));
        third.StartWork = () => Task.FromException(startError);

        try
        {
            var observed = await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
            await Assert.That(observed).IsSameReferenceAs(startError);
        }
        finally
        {
            first.Drain = Task.CompletedTask;
        }

        await Assert.That(first.Starts).IsEqualTo(1);
        await Assert.That(first.Stops).IsEqualTo(1);
        await Assert.That(second.Starts).IsEqualTo(1);
        await Assert.That(second.Stops).IsEqualTo(1);
        await ServiceBusReceiveTests.Stop(provider);
    }

    [Test]
    public async Task Control_fails_before_the_host_starts_and_after_it_stops()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, autoStart: false);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");

        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
        await ServiceBusReceiveTests.Start(provider);
        await ServiceBusReceiveTests.Stop(provider);
        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Processors.Single().Starts).IsEqualTo(0);
    }

    [Test]
    public async Task Auto_started_consumers_can_be_paused_and_resumed()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");

        await ServiceBusReceiveTests.Start(provider);
        var processor = client.Processors.Single();
        await control.StartAsync(default);
        await Assert.That(processor.Starts).IsEqualTo(1);
        await control.StopAsync(default);
        await control.StartAsync(default);
        await Assert.That(processor.Starts).IsEqualTo(2);
        await ServiceBusReceiveTests.Stop(provider);
        await Assert.That(processor.Stops).IsEqualTo(2);
    }

    [Test]
    public async Task Host_stop_with_an_expired_token_still_stops_consumers_and_the_control()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");
        await ServiceBusReceiveTests.Start(provider);

        await ServiceBusReceiveTests.Stop(provider, new CancellationToken(true));
        await Assert.That(client.Processors.Single().Stops).IsEqualTo(1);
        await Assert.That(async () => await control.StartAsync(default)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Host_stop_reaches_consumers_while_a_control_start_holds_the_gate()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, autoStart: false);
        var control = provider.GetRequiredKeyedService<IConsumerControl>("bus");
        await ServiceBusReceiveTests.Start(provider);
        var processor = client.Processors.Single();
        var startBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        processor.StartWork = () => startBlocked.Task;

        var start = control.StartAsync(default);
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync()).IsEqualTo("start");
        using var deadline = new CancellationTokenSource();
        var stop = ServiceBusReceiveTests.Stop(provider, deadline.Token);
        await Assert.That(stop.IsCompleted).IsFalse();
        await deadline.CancelAsync();

        await stop;
        await Assert.That(await processor.Lifecycle.Reader.ReadAsync()).IsEqualTo("stop");
        startBlocked.SetResult();
        await start;
    }

    [Test]
    public async Task Emulator_processor_receives_only_after_the_deferred_start()
    {
        // The emulator is shared by the session, so the queue is named per test.
        var queue = TestContext.Current!.Isolation.GetIsolatedName("deferred-start");
        await using var broker = await ServiceBusReceiveBroker.StartAsync([queue]);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        var token = deadline.Token;
        await using var client = new ServiceBusClient(broker.ConnectionString);
        var received = Channel.CreateUnbounded<int>();
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<DeferredPayload>("servicebus.deferred-start");
            builder.UseJsonSerializer(ServiceBusConsumerControlJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.AutoStartConsumers = false;
                    options.Queues.Add(new(queue));
                });
                bus.Consume(queue, consumer => consumer.Handler<DeferredHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        await ServiceBusReceiveTests.Start(provider, token);

        await using var sender = client.CreateSender(queue);
        var message = new ServiceBusMessage("{\"Value\":7}") { ContentType = "application/json" };
        message.ApplicationProperties["cloudEvents:id"] = Guid.NewGuid().ToString("N");
        message.ApplicationProperties["cloudEvents:type"] = "servicebus.deferred-start;v=1";
        await sender.SendMessageAsync(message, token);

        await using (var receiver = client.CreateReceiver(queue))
        {
            await Assert.That((await receiver.PeekMessageAsync(cancellationToken: token))?.DeliveryCount).IsEqualTo(0);
        }

        await Assert.That(received.Reader.TryRead(out _)).IsFalse();

        await provider.GetRequiredKeyedService<IConsumerControl>("bus").StartAsync(token);
        await Assert.That(await received.Reader.ReadAsync(token)).IsEqualTo(7);
        await ServiceBusReceiveTests.Stop(provider, token);
    }

    [Message("servicebus.deferred-start")]
    public sealed record DeferredPayload(int Value);

    public sealed class DeferredHandler(Channel<int> received) : IHandler<DeferredPayload>
    {
        public ValueTask HandleAsync(DeferredPayload message, MessageContext context, CancellationToken cancellationToken)
        {
            received.Writer.TryWrite(message.Value);
            return ValueTask.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(ServiceBusConsumerControlTests.DeferredPayload))]
internal partial class ServiceBusConsumerControlJson : JsonSerializerContext;
