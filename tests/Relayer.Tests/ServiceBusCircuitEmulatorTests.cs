using System.Diagnostics.Metrics;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusCircuitEmulatorTests
{
    [Test]
    public async Task Queue_and_session_failures_reopen_and_redeliver_without_failure_pipeline_dead_lettering()
    {
        await using var broker = await ServiceBusReceiveBroker.StartAsync(["circuit-queue"], ["circuit-session"]);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        var token = deadline.Token;
        await using var client = new ServiceBusClient(broker.ConnectionString);
        foreach (var sessions in new[] { false, true })
        {
            var queue = sessions ? "circuit-session" : "circuit-queue";
            var clock = new ConsumerCircuitTests.Clock();
            var control = new Control();
            var services = new ServiceCollection();
            services.AddSingleton<TimeProvider>(clock);
            services.AddSingleton(control);
            services.AddRelayer(builder =>
            {
                builder.MessageTypes.Add<Payload>("servicebus.circuit-emulator");
                builder.UseJsonSerializer(ServiceBusCircuitEmulatorJson.Default);
                builder.ServiceBus("circuit", bus =>
                {
                    bus.TopologyMode = TopologyMode.None;
                    bus.Options.Configure(options => { options.Client = client; options.Queues.Add(new(queue) { RequiresSession = sessions }); });
                    bus.Consume(queue, consumer =>
                    {
                        consumer.Options.Failure = FailureOptions.None;
                        consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1, FailureRatio = 1 };
                        if (sessions) consumer.Options.SessionProcessorOptions = new() { AutoCompleteMessages = false, MaxConcurrentSessions = 1 };
                        consumer.Handler<Handler>();
                    });
                });
            });
            await using var provider = services.BuildServiceProvider();
            var metric = provider.GetRequiredService<RelayerInstrumentation>().CircuitState;
            var states = Channel.CreateUnbounded<int>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            { if (ReferenceEquals(instrument, metric)) meterListener.EnableMeasurementEvents(instrument); };
            listener.SetMeasurementEventCallback<int>((_, state, _, _) => states.Writer.TryWrite(state));
            listener.Start();
            await ServiceBusReceiveTests.Start(provider, token);
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(0);
            await using var sender = client.CreateSender(queue);
            var message = new ServiceBusMessage("{\"Value\":42}") { SessionId = sessions ? "customer" : null };
            message.ApplicationProperties["cloudEvents:id"] = "circuit-original";
            message.ApplicationProperties["cloudEvents:type"] = "servicebus.circuit-emulator;v=1";
            await sender.SendMessageAsync(message, token);
            var first = await control.Deliveries.Reader.ReadAsync(token);
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(1);
            clock.Recovery.Fire();
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(2);
            var failedProbe = await control.Deliveries.Reader.ReadAsync(token);
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(1);
            clock.Recovery.Fire();
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(2);
            var healthyProbe = await control.Deliveries.Reader.ReadAsync(token);
            await Assert.That(await states.Reader.ReadAsync(token)).IsEqualTo(0);
            await Assert.That(failedProbe > first).IsTrue();
            await Assert.That(healthyProbe > failedProbe).IsTrue();
            await ServiceBusReceiveTests.Stop(provider, token);
            await using var deadLetters = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
            await Assert.That(await deadLetters.PeekMessageAsync(cancellationToken: token)).IsNull();
        }
    }

    [Message("servicebus.circuit-emulator")]
    public sealed record Payload(int Value);
    public sealed class Control
    {
        internal Channel<int> Deliveries { get; } = Channel.CreateUnbounded<int>();
        internal int Calls;
    }
    public sealed class Handler(Control control) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            var native = context.ServiceBus().Message;
            if (!Equals(native.ApplicationProperties["cloudEvents:id"], "circuit-original") || message.Value != 42)
                throw new InvalidOperationException("Original message changed.");
            control.Deliveries.Writer.TryWrite(native.DeliveryCount);
            return Interlocked.Increment(ref control.Calls) <= 2
                ? ValueTask.FromException(new InvalidOperationException("dependency unavailable")) : ValueTask.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(ServiceBusCircuitEmulatorTests.Payload))]
internal partial class ServiceBusCircuitEmulatorJson : JsonSerializerContext;
