using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class SendCounterTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Typed_pipeline_counts_once_with_or_without_activity_listener(bool activities)
    {
        var client = new ServiceBusSendTests.NativeClient();
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => { options.Client = client; options.Queues.Add(new("orders")); });
                bus.Send<ServiceBusSendTests.First>("orders");
            });
        });
        await using var provider = services.BuildServiceProvider();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var tracing = provider.GetRequiredService<MessageTracing>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => activities && ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(activityListener);
        long sent = 0;
        using var metrics = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        metrics.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref sent, count));
        metrics.Start();
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new ServiceBusSendTests.First());
        await Assert.That(sent).IsEqualTo(1);
        await Assert.That(client.Senders[0].Sent.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false, "cancelled")]
    [Arguments(true, "cancelled")]
    [Arguments(false, "callback")]
    [Arguments(true, "callback")]
    [Arguments(false, "tombstone")]
    [Arguments(true, "tombstone")]
    [Arguments(true, "capacity")]
    [Arguments(true, "create")]
    public async Task ServiceBus_pre_native_failures_do_not_count(bool batch, string failure)
    {
        using var metrics = new Capture();
        var client = new ServiceBusSendTests.NativeClient();
        if (failure == "create") client.BatchFailure = new InvalidOperationException("create failed");
        await using var transport = Transport(client, metrics);
        using var cancellation = new CancellationTokenSource();
        if (failure == "cancelled") cancellation.Cancel();
        var options = failure == "callback" ? new SendOptions().ServiceBus(static _ => throw new InvalidOperationException("prepare failed")) : null;
        var message = new OutgoingMessage("orders", new byte[failure == "capacity" ? 129 : 1], options: options,
            isTombstone: failure == "tombstone");
        Exception? error = null;
        try
        {
            if (batch) await transport.SendBatchAsync(new[] { message }, cancellation.Token);
            else await transport.SendAsync(message, cancellation.Token);
        }
        catch (Exception caught) { error = caught; }
        await Assert.That(error).IsNotNull();
        await Assert.That(metrics.Sent).IsEmpty();
        await Assert.That(client.Senders.Sum(sender => sender.Sent.Count)).IsEqualTo(0);
    }

    [Test]
    [Arguments("size", false)]
    [Arguments("size", true)]
    [Arguments("session", false)]
    [Arguments("session", true)]
    [Arguments("partition", false)]
    [Arguments("partition", true)]
    [Arguments("transaction", false)]
    [Arguments("transaction", true)]
    public async Task ServiceBus_counts_only_attempted_native_splits(string split, bool fail)
    {
        using var metrics = new Capture();
        var client = new ServiceBusSendTests.NativeClient { BatchCapacity = split == "size" ? 2 : 100 };
        await using var transport = Transport(client, metrics);
        if (fail) client.Senders[0].Failure = new InvalidOperationException("send failed");
        var messages = new OutgoingMessage[5];
        for (var i = 0; i < messages.Length; i++)
        {
            var group = (i / 2).ToString();
            var options = new SendOptions().ServiceBus(native =>
            {
                if (split == "session") native.SessionId = group;
                if (split == "partition") native.PartitionKey = group;
                if (split == "transaction") native.TransactionPartitionKey = group;
            });
            messages[i] = new("orders", new byte[] { 1 }, options: options);
        }
        if (fail)
            await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<InvalidOperationException>();
        else
            await transport.SendBatchAsync(messages, default);
        await Assert.That(metrics.Sent.Select(item => item.Count)).IsEquivalentTo(fail ? new long[] { 2 } : new long[] { 2, 2, 1 });
        await Assert.That(metrics.Sent.All(item => item.Destination == "orders")).IsTrue();
    }

    [Test]
    public async Task ServiceBus_later_preparation_failure_excludes_unsent_batch_and_tail()
    {
        using var metrics = new Capture();
        var client = new ServiceBusSendTests.NativeClient { BatchCapacity = 1 };
        await using var transport = Transport(client, metrics);
        var messages = new[] { new OutgoingMessage("orders", new byte[] { 1 }), new("orders", new byte[129]), new("orders", new byte[] { 3 }) };
        await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<ServiceBusException>();
        await Assert.That(metrics.Sent.Sum(item => item.Count)).IsEqualTo(1);
        await Assert.That(client.Senders[0].Sent.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ServiceBus_mixed_destinations_exclude_unattempted_groups(bool fail)
    {
        using var metrics = new Capture();
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, metrics);
        if (fail) client.Senders[0].Failure = new InvalidOperationException("send failed");
        var messages = new[] { new OutgoingMessage("orders", new byte[] { 1 }), new("events", new byte[] { 2 }), new("orders", new byte[] { 3 }) };
        if (fail)
            await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<InvalidOperationException>();
        else
            await transport.SendBatchAsync(messages, default);
        await Assert.That(metrics.Sent.Select(item => item.Destination!)).IsEquivalentTo(fail ? new[] { "orders" } : new[] { "orders", "events", "orders" });
        await Assert.That(metrics.Sent.Sum(item => item.Count)).IsEqualTo(fail ? 1 : 3);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ServiceBus_single_attempts_count_once_even_on_native_failure(bool fail)
    {
        using var metrics = new Capture();
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, metrics);
        if (fail) client.Senders[0].Failure = new InvalidOperationException("send failed");
        if (fail)
            await Assert.That(async () => await transport.SendAsync(new("orders", default), default)).Throws<InvalidOperationException>();
        else
            await transport.SendAsync(new("orders", default), default);
        await Assert.That(metrics.Sent.Sum(item => item.Count)).IsEqualTo(1);
    }

    [Test]
    public async Task InMemory_partial_batch_and_precancelled_send_count_only_admitted_messages()
    {
        using var metrics = new Capture();
        await using var transport = new InMemoryTransport("local", new Dictionary<string, InMemoryEndpointOptions>
            { ["orders"] = new() { Capacity = 10 } }, tracing: metrics.Tracing);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), cancellation.Token)).Throws<OperationCanceledException>();
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(static _ => throw new InvalidOperationException("prepare failed"));
        await Assert.That(async () => await transport.SendBatchAsync(new[] {
            new OutgoingMessage("orders", default), new("orders", default, options: options), new("orders", default) }, default)).Throws<InvalidOperationException>();
        await Assert.That(metrics.Sent.Sum(item => item.Count)).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_counter_adds_zero_allocations_after_warmup(bool enabled)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, metrics) =>
            {
                if (enabled && ReferenceEquals(instrument.Meter.Scope, instrumentation)) metrics.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
        listener.Start();
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            static (_, _) => default, static (_, _) => default);
        for (var i = 0; i < 100; i++) send.RecordNativeSend(2);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) send.RecordNativeSend(2);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    private static ServiceBusSendTransport Transport(ServiceBusSendTests.NativeClient client, Capture metrics) =>
        new("bus", client, false, ServiceBusSendTests.Options(client), ["orders", "events"], TopologyMode.None, tracing: metrics.Tracing);

    private sealed class Capture : IDisposable
    {
        private readonly RelayerInstrumentation _instrumentation = new();
        private readonly MeterListener _listener = new();
        internal MessageTracing Tracing { get; }
        internal List<(string? Destination, long Count)> Sent { get; } = [];
        internal Capture()
        {
            Tracing = new(Options.Create(new TracingOptions()), [], instrumentation: _instrumentation);
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, _instrumentation) && instrument.Name == "messaging.client.sent.messages")
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((_, count, tags, _) =>
            {
                string? destination = null;
                foreach (var tag in tags)
                    if (tag.Key == "messaging.destination.name") destination = (string?)tag.Value;
                Sent.Add((destination, count));
            });
            _listener.Start();
        }
        public void Dispose() { _listener.Dispose(); Tracing.Dispose(); _instrumentation.Dispose(); }
    }
}
