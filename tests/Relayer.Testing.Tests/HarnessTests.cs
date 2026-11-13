using Relayer.Pipeline;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Testing.Tests;

public class HarnessTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Test]
    public async Task Typed_queries_filter_mixed_identities_and_keep_owned_results_after_reset()
    {
        await using var fixture = await Fixture.Start();
        var sent = new Probe(42);
        await fixture.Send(sent);
        using (var scope = fixture.Provider.CreateScope())
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OtherProbe(7));
        await fixture.Idle();
        await Assert.That(fixture.Harness.GetSent<Probe>().Single()).IsSameReferenceAs(sent);
        await Assert.That(fixture.Harness.GetSent<OtherProbe>().Single().Value).IsEqualTo(7);
        var consumed = fixture.Harness.GetConsumed<Probe>();
        var snapshot = fixture.Harness.Consumed.Single(item => item.WireType == "harness.probe;v=1");
        await Assert.That(consumed.Single().Value).IsEqualTo(42);
        await Assert.That(fixture.Harness.GetConsumed<OtherProbe>().Single().Value).IsEqualTo(7);
        await Assert.That(fixture.Harness.GetDispatched<Probe>().Single().Value).IsEqualTo(42);
        fixture.Harness.Reset();
        await fixture.Send(new Probe(99));
        await fixture.Idle();
        await Assert.That(consumed.Single().Value).IsEqualTo(42);
        await Assert.That(fixture.Harness.Read<Probe>(snapshot).Value).IsEqualTo(42);
        await Assert.That(fixture.Harness.GetConsumed<Probe>().Single().Value).IsEqualTo(99);
        await Assert.That(() => fixture.Harness.Read<OtherProbe>(snapshot)).Throws<InvalidOperationException>();
        await Assert.That(() => fixture.Harness.GetSent<string>()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Typed_queries_skip_raw_and_other_version_bodies_before_deserialization()
    {
        await using var fixture = await Fixture.Create();
        var transport = fixture.Provider.GetServices<ITransport>().Single(item => item.Name == "first");
        await transport.SendAsync(new("probes", "invalid JSON"u8.ToArray()), default);
        await transport.SendAsync(new("probes", "invalid JSON"u8.ToArray(),
            new KeyValuePair<string, WireString>[] { new("ce_type", new("harness.probe;v=2")) }), default);
        await fixture.Send(new Probe(5));
        await Assert.That(fixture.Harness.GetDispatched<Probe>().Single().Value).IsEqualTo(5);
        await Assert.That(fixture.Harness.GetDispatched<OtherProbe>().Count).IsEqualTo(0);
    }

    [Test]
    public async Task Typed_reads_use_named_serializers_and_keep_providers_isolated()
    {
        await using var first = await Fixture.Start(serializerMarker: 11);
        await using var second = await Fixture.Start(serializerMarker: 22);
        await Task.WhenAll(first.Send(new Probe(1)), second.Send(new Probe(2)));
        using (var scope = first.Provider.CreateScope())
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OtherProbe(3));
        await Task.WhenAll(first.Idle(), second.Idle());
        var serializer = (MarkedSerializer)first.Provider.GetMessageSerializer("first");
        var reads = serializer.Reads;
        await Assert.That(first.Harness.GetSent<Probe>().Single().Value).IsEqualTo(1);
        await Assert.That(serializer.Reads).IsEqualTo(reads);
        await Assert.That(first.Harness.GetConsumed<Probe>().Single().Value).IsEqualTo(1);
        await Assert.That(first.Harness.GetDispatched<Probe>().Single().Value).IsEqualTo(1);
        await Assert.That(first.Harness.Read<Probe>(first.Harness.Consumed.Single(item => item.TransportName == "first")).Value).IsEqualTo(1);
        await Assert.That(first.Harness.GetConsumed<OtherProbe>().Single().Value).IsEqualTo(3);
        await Assert.That(second.Harness.GetConsumed<Probe>().Single().Value).IsEqualTo(2);
        await Assert.That(second.Harness.GetDispatched<Probe>().Single().Value).IsEqualTo(2);
    }

    [Test]
    public async Task Round_trip_records_owned_snapshots_and_reset_clears_all_state()
    {
        await using var fixture = await Fixture.Start();
        await fixture.Send(new Probe(42));
        await fixture.Idle();
        await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(1);
        var snapshot = fixture.Harness.Consumed[0];
        await Assert.That(snapshot.Read<Probe>(fixture.Serializer).Value).IsEqualTo(42);
        await Assert.That(snapshot.TransportName).IsEqualTo("first");
        await Assert.That(snapshot.MessageId).IsEqualTo(fixture.Harness.Sent[0].MessageId);
        fixture.Harness.Reset();
        await fixture.Send(new Probe(99));
        await fixture.Idle();
        await Assert.That(snapshot.Read<Probe>(fixture.Serializer).Value).IsEqualTo(42);
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(0);
        await Assert.That(fixture.Harness.DeadLettered.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Idle_waits_for_handler_and_nested_send_without_sleeping()
    {
        var entered = Signal();
        var release = Signal();
        var control = new ProbeControl { Handle = async (message, context) =>
        {
            if (message.Value != 1) return;
            entered.TrySetResult();
            await release.Task;
            await context.Services.GetRequiredService<ISender>().SendAsync(new Probe(2));
        } };
        await using var fixture = await Fixture.Start(control);
        try
        {
            await fixture.Send(new Probe(1));
            await entered.Task.WaitAsync(Limit);
            var idle = fixture.Harness.WaitForIdleAsync();
            await Assert.That(idle.IsCompleted).IsFalse();
            await Assert.That(() => fixture.Harness.Reset()).Throws<InvalidOperationException>();
            release.TrySetResult();
            await idle.WaitAsync(Limit);
            await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(2);
        }
        finally { release.TrySetResult(); }
    }

    [Test]
    public async Task Idle_tracks_message_before_receivers_start()
    {
        await using var fixture = await Fixture.Create();
        await fixture.Send(new Probe(1));
        var idle = fixture.Harness.WaitForIdleAsync();
        await Assert.That(idle.IsCompleted).IsFalse();
        await fixture.StartServices();
        await idle.WaitAsync(Limit);
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Poison_failure_and_native_dead_letter_are_recorded_before_idle()
    {
        // Observe one failed attempt; retry policy behavior is covered separately.
        var error = new InvalidOperationException("poison");
        await using var fixture = await Fixture.Start(new() { Handle = (_, _) => ValueTask.FromException(error) }, failures: new() { RetryDelays = [] });
        await fixture.Send(new Probe(1));
        await fixture.Idle();
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Failed[0].Error).IsSameReferenceAs(error);
        await Assert.That(fixture.Harness.DeadLettered.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.DeadLettered[0].Error.Reason).IsEqualTo("Processing failed");
        await Assert.That(fixture.Harness.Read<Probe>(fixture.Harness.DeadLettered[0].Message).Value).IsEqualTo(1);
        fixture.Harness.Reset();
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(0);
        await Assert.That(fixture.Harness.DeadLettered.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Manual_native_dead_letter_is_observed()
    {
        await using var fixture = await Fixture.Start(new() { Handle = (_, context) =>
        {
            var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
            native.Message.DeadLetter(new("manual"));
            native.Disposition = Disposition.SettledByHandler;
            return default;
        } });
        await fixture.Send(new Probe(1));
        await fixture.Idle();
        await Assert.That(fixture.Harness.DeadLettered.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.DeadLettered[0].Error.Reason).IsEqualTo("manual");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Manual_native_settlement_failure_is_observed_once_with_or_without_logging(bool loggingEnabled)
    {
        var logger = new FailureLogger();
        await using var fixture = await Fixture.Start(new() { Handle = (_, context) =>
        {
            context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Message.Redeliver(TimeSpan.FromSeconds(1));
            return default;
        } }, logging: loggingEnabled ? logger : null, maxRedeliveryDelay: TimeSpan.Zero, failures: new() { RetryDelays = [] });
        await fixture.Send(new Probe(1));
        await fixture.Idle();
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(1);
        var failure = fixture.Harness.Failed[0];
        await Assert.That(failure.Operation).IsEqualTo(HarnessOperation.Consume);
        await Assert.That(failure.Error.Message).Contains("MaxRedeliveryDelay");
        await Assert.That(failure.Message.MessageId).IsEqualTo(fixture.Harness.Sent[0].MessageId);
        await Assert.That(logger.Errors.Count).IsEqualTo(loggingEnabled ? 1 : 0);
        if (loggingEnabled) await Assert.That(logger.Errors.Single()).IsSameReferenceAs(failure.Error);
    }

    [Test]
    public async Task Traced_settlement_failure_is_not_observed_twice()
    {
        var error = new InvalidOperationException("settlement");
        await using var fixture = await Fixture.Create();
        var provider = fixture.Provider;
        var endpoint = new MessagingEndpoint { System = "test", Destination = "probes" };
        var tracing = provider.GetRequiredService<MessageTracing>();
        var settlement = new MessageOperationTracing(tracing, endpoint, MessagingOperation.Settle, "complete");
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), fixture.Serializer, static (_, _, _) => default)
            .Handler(provider.GetRequiredService<HandlerRegistry>().Descriptors.Single(item => item.HandlerType == typeof(ProbeHandler)))
            .Tracing(tracing, endpoint)
            .Settle(settlement.WrapSettlement(_ => ValueTask.FromException(error))).Build(1);
        var body = new ArrayBufferWriter<byte>();
        fixture.Serializer.Serialize(new Probe(1), body);
        using var context = pipeline.RentContext(new ConsumeMessage
        {
            MessageId = "one", MessageType = new("harness.probe", 1), Body = new(body.WrittenMemory),
        });
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<InvalidOperationException>();
        await fixture.Idle();
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Failed[0].Error).IsSameReferenceAs(error);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Shared_destination_preserves_each_routes_envelope_identity(bool outboxBatch)
    {
        var services = new ServiceCollection();
        var outbox = new TestOutbox();
        if (outboxBatch) services.AddScoped<IOutboxEnlistment>(_ => outbox);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("shared", transport =>
            {
                transport.Send<Probe>("probes");
                transport.Send<OtherProbe>("probes", route => route.SendOptions = new() { HeaderNames = new("custom_") });
            });
        });
        services.AddRelayerHarness();
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new Probe(1));
        var first = outbox.Message;
        await sender.SendAsync(new OtherProbe(2));
        if (outboxBatch)
            await provider.GetServices<ITransport>().Single().SendBatchAsync(new[] { first, outbox.Message }, default);
        var harness = provider.GetRequiredService<RelayerHarness>();
        await Assert.That(harness.Dispatched.Count).IsEqualTo(2);
        for (var i = 0; i < 2; i++)
        {
            await Assert.That(harness.Dispatched[i].MessageId).IsEqualTo(harness.Sent[i].MessageId);
            await Assert.That(harness.Dispatched[i].WireType).IsEqualTo(harness.Sent[i].WireType);
        }
        await Assert.That(harness.Dispatched[0].Read<Probe>(provider.GetRequiredService<IMessageSerializer>()).Value).IsEqualTo(1);
        await Assert.That(harness.Dispatched[1].Read<OtherProbe>(provider.GetRequiredService<IMessageSerializer>()).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Publish_subscribe_route_waits_for_every_group()
    {
        var both = Signal();
        var release = Signal();
        var count = 0;
        await using var fixture = await Fixture.Start(new() { Handle = async (_, _) =>
        {
            if (Interlocked.Increment(ref count) == 2) both.TrySetResult();
            await release.Task;
        } }, log: true);
        try
        {
            await fixture.Send(new Probe(1));
            await both.Task.WaitAsync(Limit);
            var idle = fixture.Harness.WaitForIdleAsync();
            await Assert.That(idle.IsCompleted).IsFalse();
            release.TrySetResult();
            await idle.WaitAsync(Limit);
            await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(1);
            await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(1);
            await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(2);
        }
        finally { release.TrySetResult(); }
    }

    [Test]
    public async Task Multiple_named_transports_and_parallel_hosts_are_isolated()
    {
        await using var first = await Fixture.Start();
        await using var second = await Fixture.Start();
        await Task.WhenAll(first.Send(new Probe(1)), second.Send(new Probe(2)));
        using (var scope = first.Provider.CreateScope())
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OtherProbe(3));
        await Task.WhenAll(first.Idle(), second.Idle());
        await Assert.That(first.Harness.Sent.Count).IsEqualTo(2);
        await Assert.That(first.Harness.Dispatched.Select(item => item.TransportName!).Order().ToArray()).IsEquivalentTo(new[] { "first", "second" });
        await Assert.That(first.Harness.Consumed.Count).IsEqualTo(2);
        await Assert.That(second.Harness.Sent.Count).IsEqualTo(1);
        first.Harness.Reset();
        await Assert.That(second.Harness.Consumed.Count).IsEqualTo(1);
        await Assert.That(second.Harness.Consumed[0].Read<Probe>(second.Serializer).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Outbox_enlistment_and_later_native_dispatch_remain_distinct()
    {
        var outbox = new TestOutbox();
        await using var fixture = await Fixture.Start(outbox: outbox);
        using var cycle = fixture.Harness.TrackWork();
        await fixture.Send(new Probe(7));
        await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Sent[0].UsesOutbox).IsTrue();
        await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(0);
        var idle = fixture.Harness.WaitForIdleAsync();
        await Assert.That(idle.IsCompleted).IsFalse();
        var transport = fixture.Provider.GetServices<ITransport>().Single(item => item.Name == "first");
        await transport.SendAsync(outbox.Message, default);
        cycle.Dispose();
        await idle.WaitAsync(Limit);
        await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Dispatched[0].MessageId).IsEqualTo(fixture.Harness.Sent[0].MessageId);
    }

    [Test]
    public async Task Cancellation_of_idle_wait_does_not_cancel_shared_work()
    {
        var harness = new RelayerHarness();
        using var work = harness.TrackWork();
        using var cancellation = new CancellationTokenSource();
        var cancelled = harness.WaitForIdleAsync(cancellation.Token);
        var survivor = harness.WaitForIdleAsync();
        cancellation.Cancel();
        await Assert.That(async () => await cancelled).Throws<OperationCanceledException>();
        await Assert.That(survivor.IsCompleted).IsFalse();
        work.Dispose();
        work.Dispose();
        await survivor.WaitAsync(Limit);
        harness.Reset();
    }

    [Test]
    public async Task Delayed_native_message_keeps_harness_busy()
    {
        var clock = new Relayer.Tests.InMemoryTestClock();
        await using var fixture = await Fixture.Start(clock: clock);
        using (var scope = fixture.Provider.CreateScope())
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(1), options => options.DeliverAt = clock.GetUtcNow().AddMinutes(1));
        await clock.NextTimer();
        var idle = fixture.Harness.WaitForIdleAsync();
        await Assert.That(idle.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMinutes(1));
        await idle.WaitAsync(Limit);
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Native_batch_dispatch_copies_borrowed_payloads()
    {
        await using var fixture = await Fixture.Start();
        var transport = fixture.Provider.GetServices<ITransport>().Single(item => item.Name == "first");
        var bytes = "{\"Value\":31}"u8.ToArray();
        var id = Guid.CreateVersion7().ToString("D");
        var headers = new KeyValuePair<string, WireString>[] { new("ce_id", new WireString(id)), new("ce_type", new WireString("harness.probe;v=1")) };
        await transport.SendBatchAsync(new OutgoingMessage[] { new("probes", bytes, headers) }, default);
        Array.Fill(bytes, (byte)0);
        Array.Clear(headers);
        await fixture.Idle();
        await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.GetDispatched<Probe>().Single().Value).IsEqualTo(31);
        await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Failed_send_releases_work_and_preserves_error()
    {
        await using var fixture = await Fixture.Start();
        using var scope = fixture.Provider.CreateScope();
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(1), options => options.OrderingKey = "unsupported")).Throws<InvalidOperationException>();
        await fixture.Idle();
        await Assert.That(fixture.Harness.Sent.Count).IsEqualTo(0);
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Native_redelivery_keeps_work_pending_until_success()
    {
        await using var fixture = await Fixture.Start(new() { Handle = (_, context) =>
        {
            if (context.Attempt == 1)
            {
                var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
                native.Message.Redeliver(TimeSpan.Zero);
                native.Disposition = Disposition.SettledByHandler;
            }
            return default;
        } });
        await fixture.Send(new Probe(1));
        await fixture.Idle();
        await Assert.That(fixture.Harness.Consumed.Count).IsEqualTo(2);
        await Assert.That(fixture.Harness.Consumed[0].Attempt).IsEqualTo(1);
        await Assert.That(fixture.Harness.Consumed[1].Attempt).IsEqualTo(2);
        await Assert.That(fixture.Harness.DeadLettered.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Test_context_supports_headers_services_features_and_handler_invocation()
    {
        var headers = new Dictionary<string, string> { ["tenant"] = "one" };
        var services = new ServiceCollection().AddSingleton(new ProbeControl()).BuildServiceProvider();
        await using var provider = services;
        using var cancellation = new CancellationTokenSource();
        var context = new TestMessageContext(new("harness.probe", 1), provider, headers, "fixed-id", 3, cancellation.Token);
        headers["tenant"] = "two";
        context.Features.Set(new Marker(42));
        var invoked = false;
        var handler = new ProbeHandler(new() { Handle = (_, seen) =>
        {
            invoked = ReferenceEquals(context, seen);
            return default;
        } });
        await handler.HandleAsync(new Probe(1), context, context.CancellationToken);
        await Assert.That(invoked).IsTrue();
        await Assert.That(context.Headers.TryGetValue("tenant", out var tenant)).IsTrue();
        await Assert.That(tenant.ToString()).IsEqualTo("one");
        await Assert.That(context.Features.GetRequired<Marker>().Value).IsEqualTo(42);
        await Assert.That(context.MessageId).IsEqualTo("fixed-id");
        await Assert.That(context.Attempt).IsEqualTo(3);
        await Assert.That(context.Services.GetRequiredService<ProbeControl>()).IsNotNull();
        cancellation.Cancel();
        await Assert.That(context.CancellationToken.IsCancellationRequested).IsTrue();
        await Assert.That(() => new TestMessageContext(new("probe", 1), attempt: 0)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Registration_rejects_missing_relayer_and_duplicate_harness()
    {
        var services = new ServiceCollection();
        await Assert.That(() => services.AddRelayerHarness()).Throws<InvalidOperationException>();
        services.AddRelayer(_ => { });
        services.AddRelayerHarness();
        await Assert.That(() => services.AddRelayerHarness()).Throws<InvalidOperationException>();
    }

    private sealed record Marker(int Value);

    [Test]
    public async Task Batch_consumption_records_each_delivery_and_partial_failure()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<InMemoryTransportOptions, InMemoryRouteOptions, InMemoryConsumerOptions>("batch", transport =>
                transport.Consume("probes", consumer => consumer.Handler<BatchProbeHandler>().Batch(2, TimeSpan.FromSeconds(1))));
        });
        services.AddRelayerHarness();
        await using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        var descriptor = provider.GetRequiredService<HandlerRegistry>().Descriptors.Single();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), serializer,
            static (_, _, _) => default).BuildBatch(descriptor, declaration);
        var body = new ArrayBufferWriter<byte>();
        serializer.Serialize(new Probe(1), body);
        using var first = pipeline.RentContext(new ConsumeMessage
        {
            MessageId = "one", MessageType = new("harness.probe", 1), Body = new(body.WrittenMemory),
        });
        using var second = pipeline.RentContext(new ConsumeMessage
        {
            MessageId = "two", MessageType = new("harness.probe", 1), Body = new(body.WrittenMemory),
        });
        await pipeline.ProcessBatchAsync(new[] { first, second });
        var harness = provider.GetRequiredService<RelayerHarness>();
        await harness.WaitForIdleAsync().WaitAsync(Limit);
        await Assert.That(harness.Consumed.Count).IsEqualTo(2);
        await Assert.That(harness.GetConsumed<Probe>().Select(message => message.Value).ToArray()).IsEquivalentTo(new[] { 1, 1 });
        await Assert.That(harness.Read<Probe>(harness.Failed[0].Message).Value).IsEqualTo(1);
        await Assert.That(harness.Failed.Count).IsEqualTo(1);
        await Assert.That(harness.Failed[0].Message.MessageId).IsEqualTo("two");
    }

    [Test]
    public async Task Failed_native_dispatch_preserves_failure_and_releases_work()
    {
        await using var fixture = await Fixture.Start();
        var transport = fixture.Provider.GetServices<ITransport>().Single(item => item.Name == "first");
        await Assert.That(async () => await transport.SendAsync(new("missing", new byte[] { 1 }), default))
            .Throws<InvalidOperationException>();
        await fixture.Idle();
        await Assert.That(fixture.Harness.Failed.Count).IsEqualTo(1);
        await Assert.That(fixture.Harness.Failed[0].Operation).IsEqualTo(HarnessOperation.Dispatch);
        await Assert.That(fixture.Harness.Dispatched.Count).IsEqualTo(0);
    }

    private sealed class TestOutbox : IOutboxEnlistment
    {
        public bool IsActive => true;
        public OutgoingMessage Message { get; private set; }
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var message = record.Message;
            var headers = message.Headers.ToArray();
            for (var i = 0; i < headers.Length; i++) headers[i] = new(headers[i].Key, new WireString(headers[i].Value.ToString()));
            Message = new(message.Destination, message.Body.ToArray(), headers);
            return default;
        }
    }

    private sealed class Fixture(ServiceProvider provider) : IAsyncDisposable
    {
        internal ServiceProvider Provider => provider;
        internal RelayerHarness Harness => provider.GetRequiredService<RelayerHarness>();
        internal IMessageSerializer Serializer => provider.GetRequiredService<IMessageSerializer>();
        private bool _started;

        internal static Task<Fixture> Create(ProbeControl? control = null, bool log = false,
            TestOutbox? outbox = null, TimeProvider? clock = null, ILoggerFactory? logging = null, TimeSpan? maxRedeliveryDelay = null, FailureOptions? failures = null, byte? serializerMarker = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton(control ?? new ProbeControl());
            if (logging is not null) services.AddSingleton(logging);
            if (outbox is not null) services.AddScoped<IOutboxEnlistment>(_ => outbox);
            services.AddRelayer(builder =>
            {
                builder.AddMessageTypesFromThisAssembly();
                builder.InMemory("first", transport =>
                {
                    transport.Send<Probe>("probes");
                    if (serializerMarker is { } marker)
                        transport.UseSerializer(provider => new MarkedSerializer(provider.GetRequiredService<IMessageSerializer>(), marker));
                    transport.Options.Configure(options =>
                    {
                        options.Endpoints["probes"] = new() { Capabilities = new()
                        {
                            ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                            DeadLetter = true, MaxDeliveryDelay = TimeSpan.MaxValue, MaxRedeliveryDelay = maxRedeliveryDelay ?? TimeSpan.MaxValue,
                            MaxBatchSize = 100,
                        } };
                    });
                    transport.Consume("probes", consumer => { consumer.Handler<ProbeHandler>(); if (failures is not null) consumer.Failures(failures); consumer.Options.Group = log ? "one" : null; });
                    if (log) transport.Consume("probes", consumer => { consumer.Handler<ProbeHandler>(); if (failures is not null) consumer.Failures(failures); consumer.Options.Group = "two"; });
                });
                builder.InMemory("second", transport =>
                {
                    transport.Send<OtherProbe>("other");
                    transport.Consume("other", consumer => consumer.Handler<OtherProbeHandler>());
                });
            });
            if (clock is not null)
                services.AddSingleton<Microsoft.Extensions.Options.IOptionsMonitor<InMemoryTransportOptions>>(
                    new ClockOptions(clock));
            services.AddRelayerHarness();
            return Task.FromResult(new Fixture(services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true })));
        }

        internal static async Task<Fixture> Start(ProbeControl? control = null, bool log = false,
            TestOutbox? outbox = null, TimeProvider? clock = null, ILoggerFactory? logging = null, TimeSpan? maxRedeliveryDelay = null, FailureOptions? failures = null, byte? serializerMarker = null)
        {
            var fixture = await Create(control, log, outbox, clock, logging, maxRedeliveryDelay, failures, serializerMarker);
            await fixture.StartServices();
            return fixture;
        }

        internal async Task StartServices()
        {
            var hosted = provider.GetServices<IHostedService>().ToArray();
            foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
            foreach (var service in hosted) await service.StartAsync(default);
            _started = true;
        }

        internal async Task Send(Probe message)
        {
            using var scope = provider.CreateScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(message);
        }

        internal Task Idle() => Harness.WaitForIdleAsync().WaitAsync(Limit);

        public async ValueTask DisposeAsync()
        {
            if (_started)
                foreach (var service in provider.GetServices<IHostedService>().Reverse()) await service.StopAsync(default);
            await provider.DisposeAsync();
        }
    }

    private sealed class MarkedSerializer(IMessageSerializer inner, byte marker) : IMessageSerializer
    {
        public string ContentType => inner.ContentType;
        public int Reads { get; private set; }
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            destination.GetSpan(1)[0] = marker;
            destination.Advance(1);
            inner.Serialize(message, destination);
        }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            if (source.FirstSpan[0] != marker) throw new InvalidOperationException("Wrong provider's serializer.");
            Reads++;
            return inner.Deserialize<TMessage>(source.Slice(1));
        }
    }

    private sealed class FailureLogger : ILoggerFactory, ILogger
    {
        internal System.Collections.Concurrent.ConcurrentQueue<Exception> Errors { get; } = new();
        public ILogger CreateLogger(string categoryName) => this;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public bool IsEnabled(LogLevel level) => level >= LogLevel.Error;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> formatter)
        {
            if (error is not null && IsEnabled(level)) Errors.Enqueue(error);
        }
        public void Dispose() { }
    }

    private sealed class ClockOptions(TimeProvider clock) : Microsoft.Extensions.Options.IOptionsMonitor<InMemoryTransportOptions>
    {
        public InMemoryTransportOptions CurrentValue => Get(null);
        public InMemoryTransportOptions Get(string? name) => new() { TimeProvider = clock };
        public IDisposable? OnChange(Action<InMemoryTransportOptions, string?> listener) => null;
    }
}

[Message("harness.probe")]
public sealed record Probe(int Value);
[Message("harness.other")]
public sealed record OtherProbe(int Value);

public sealed class ProbeControl
{
    public Func<Probe, MessageContext, ValueTask> Handle { get; init; } = static (_, _) => default;
}

public sealed class ProbeHandler(ProbeControl control) : IHandler<Probe>
{
    public ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken) => control.Handle(message, context);
}

public sealed class OtherProbeHandler : IHandler<OtherProbe>
{
    public ValueTask HandleAsync(OtherProbe message, MessageContext context, CancellationToken cancellationToken) => default;
}

public sealed class BatchProbeHandler : IBatchHandler<Probe>
{
    public ValueTask<BatchResult> HandleAsync(Batch<Probe> batch, CancellationToken cancellationToken)
    {
        batch.Items[1].Fail(new InvalidOperationException("partial"));
        return new(BatchResult.Success);
    }
}
