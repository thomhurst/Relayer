using System.Diagnostics;
using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public class ServiceBusSendDurationTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Duration_waits_for_native_completion_and_tags_failures(bool batch, bool fail)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, tracing);
        var sender = client.Senders[0];
        sender.Pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var message = new OutgoingMessage("orders", new byte[] { 1 }).WithTracingStarted();
        var pending = batch ? transport.SendBatchAsync(new[] { message, message }, default) : transport.SendAsync(message, default);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(capture.Values.Count).IsEqualTo(0);
        if (fail) sender.Pending.SetException(new ServiceBusException("sensitive detail", ServiceBusFailureReason.ServiceBusy));
        else sender.Pending.SetResult();
        if (fail) await Assert.That(async () => await pending).Throws<ServiceBusException>();
        else await pending;
        var duration = capture.Values.Single();
        await Assert.That(duration.Seconds > 0).IsTrue();
        await Assert.That(duration.Tags["messaging.system"]).IsEqualTo("servicebus");
        await Assert.That(duration.Tags["messaging.operation.name"]).IsEqualTo("send");
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags["server.address"]).IsEqualTo(client.FullyQualifiedNamespace);
        await Assert.That(duration.Tags.ContainsKey("error.type")).IsEqualTo(fail);
        if (fail) await Assert.That(duration.Tags["error.type"]).IsEqualTo("_OTHER");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_cancellation_records_failed_duration(bool batch)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, tracing);
        var sender = client.Senders[0];
        sender.Pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();
        var message = new OutgoingMessage("orders", new byte[] { 1 });
        var pending = batch ? transport.SendBatchAsync(new[] { message }, stop.Token) : transport.SendAsync(message, stop.Token);
        await Assert.That(pending.IsCompleted).IsFalse();
        stop.Cancel();
        sender.Pending.SetCanceled(stop.Token);
        await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(capture.Values.Single().Tags["error.type"]).IsEqualTo("_OTHER");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Split_batches_and_outbox_dispatch_record_each_native_operation(bool outbox)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var client = new ServiceBusSendTests.NativeClient { BatchCapacity = 2 };
        await using var transport = Transport(client, tracing);
        var sessionA = new SendOptions().ServiceBus(message => message.SessionId = "a");
        var sessionB = new SendOptions().ServiceBus(message => message.SessionId = "b");
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 }, options: sessionA),
            new("orders", new byte[] { 2 }, options: sessionA), new("orders", new byte[] { 3 }, options: sessionA),
            new("orders", new byte[] { 4 }, options: sessionB), new("events", new byte[] { 5 })];
        if (outbox)
            for (var i = 0; i < messages.Length; i++) messages[i] = messages[i].ForOutboxDispatch();
        await transport.SendBatchAsync(messages, default);
        await Assert.That(capture.Values.Count).IsEqualTo(4);
        await Assert.That(capture.Values.Count(value => Equals(value.Tags["messaging.destination.name"], "orders"))).IsEqualTo(3);
        await Assert.That(capture.Values.All(value => !value.Tags.ContainsKey("error.type"))).IsTrue();
        capture.Values.Clear();
        await transport.SendAsync(messages[0], default);
        await Assert.That(capture.Values.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_mixed_batch_stops_after_one_native_attempt_including_dispatch(bool outbox)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, tracing);
        client.Senders[0].Failure = new ServiceBusException("sensitive detail", ServiceBusFailureReason.ServiceBusy);
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 }), new("events", new byte[] { 2 })];
        if (outbox)
            for (var i = 0; i < messages.Length; i++) messages[i] = messages[i].ForOutboxDispatch();
        await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<ServiceBusException>();
        var duration = capture.Values.Single();
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags["error.type"]).IsEqualTo("_OTHER");
        await Assert.That(client.Senders[1].Batches.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Empty_batches_and_preparation_failures_do_not_record_native_sends()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var client = new ServiceBusSendTests.NativeClient();
        await using var transport = Transport(client, tracing);
        await transport.SendBatchAsync(Array.Empty<OutgoingMessage>(), default);
        await Assert.That(async () => await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[129]) }, default))
            .Throws<ServiceBusException>();
        using var stop = new CancellationTokenSource();
        stop.Cancel();
        await Assert.That(async () => await transport.SendAsync(new("orders", default), stop.Token)).Throws<OperationCanceledException>();
        await Assert.That(capture.Values.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Sender_times_direct_send_and_dispatch_but_not_outbox_enqueue(bool trace)
    {
        var client = new ServiceBusSendTests.NativeClient();
        var outbox = new Outbox();
        var services = new ServiceCollection();
        services.AddScoped<IOutboxEnlistment>(_ => outbox);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.ServiceBus("bus", bus =>
            {
                bus.Options.Configure(options => { options.Client = client; options.Queues.Add(new("orders")); });
                bus.Send<ServiceBusSendTests.First>("orders");
            });
        });
        await using var provider = services.BuildServiceProvider();
        using var capture = new SendDurationCapture(provider.GetRequiredService<RelayerInstrumentation>());
        var tracing = provider.GetRequiredService<MessageTracing>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => trace && ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendNowAsync(new ServiceBusSendTests.First());
        await Assert.That(capture.Values.Count).IsEqualTo(1);
        capture.Values.Clear();
        await sender.SendAsync(new ServiceBusSendTests.First());
        await Assert.That(capture.Values.Count).IsEqualTo(0);
        var transport = provider.GetRequiredService<ITransport>();
        await transport.SendAsync(outbox.Message.ForOutboxDispatch(), default);
        await Assert.That(capture.Values.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Warm_native_timer_records_success_and_failure_without_allocations(bool listen)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, observer) =>
            {
                if (listen && ReferenceEquals(instrument.Meter.Scope, instrumentation)) observer.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
        listener.Start();
        var send = new SendTracing(tracing, new() { System = "servicebus", Destination = "orders", ServerAddress = "example.servicebus.windows.net" },
            static (_, _) => default, static (_, _) => default);
        await Assert.That(send.StartNativeSend().IsEnabled).IsEqualTo(listen);
        for (var i = 0; i < 1000; i++) send.StartNativeSend().Complete(failed: (i & 1) == 0);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) send.StartNativeSend().Complete(failed: (i & 1) == 0);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    private sealed class Outbox : IOutboxEnlistment
    {
        public bool IsActive => true;
        internal OutgoingMessage Message;
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            // Own the borrowed payload before the send pipeline returns its buffers.
            var headers = record.Message.Headers.ToArray();
            for (var i = 0; i < headers.Length; i++) headers[i] = new(headers[i].Key, new WireString(headers[i].Value.ToString()));
            Message = new(record.Message.Destination, record.Message.Body.ToArray(), headers);
            return default;
        }
    }

    private static ServiceBusSendTransport Transport(ServiceBusSendTests.NativeClient client, MessageTracing tracing)
    {
        var options = new AzureServiceBusTransportOptions { Client = client };
        options.Queues.Add(new("orders"));
        options.Topics.Add(new("events"));
        return new("bus", client, false, options, ["orders", "events"], TopologyMode.None, tracing: tracing);
    }
}
