using System.Diagnostics;
using Azure;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public class ServiceBusSendTests
{
    [Test]
    public async Task Named_registration_preserves_native_options_and_caller_ownership()
    {
        var first = new NativeClient();
        var second = new NativeClient();
        var nativeOptions = new ServiceBusClientOptions { TransportType = ServiceBusTransportType.AmqpWebSockets };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.ServiceBus("first", bus =>
            {
                bus.Options.Configure(options => { options.Client = first; options.ClientOptions = nativeOptions; options.Queues.Add(new("orders")); });
                bus.Send<First>("orders");
            });
            builder.ServiceBus("second", bus =>
            {
                bus.Options.Configure(options => { options.Client = second; options.Topics.Add(new("events")); });
                bus.Send<Second>("events");
            });
        });
        await using (var provider = services.BuildServiceProvider())
        {
            var options = provider.GetRequiredService<IOptionsMonitor<ServiceBusTransportOptions>>();
            await Assert.That(options.Get("first").ClientOptions).IsSameReferenceAs(nativeOptions);
            await Assert.That(options.Get("second").Client).IsSameReferenceAs(second);
            var transports = provider.GetServices<ITransport>().ToArray();
            var routes = provider.GetRequiredService<RelayerConfiguration>().Transports;
            var route = (RouteDefinition<First, ServiceBusRouteOptions>)routes[0].Routes.Single();
            await Assert.That(route.SendOptions.HeaderNames.Id).IsEqualTo("cloudEvents:id");
            await transports[0].SendAsync(new("orders", new byte[] { 1 }), default);
            await transports[0].SendAsync(new("orders", new byte[] { 2 }), default);
            await Assert.That(first.Senders.Count).IsEqualTo(1);
            await Assert.That(second.Senders.Count).IsEqualTo(1);
        }
        await Assert.That(first.Disposals).IsEqualTo(0);
        await Assert.That(second.Disposals).IsEqualTo(0);
        await Assert.That(first.Senders[0].Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Only_owned_clients_are_disposed_and_disposal_is_idempotent()
    {
        var client = new NativeClient();
        var transport = Transport(client, ownsClient: true);
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(client.Disposals).IsEqualTo(1);
        await Assert.That(client.Senders[0].Disposals).IsEqualTo(1);
        await Assert.That(async () => await transport.SendAsync(new("orders", default), default)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Disposal_closes_remaining_senders_and_owned_client_after_native_close_failure()
    {
        var client = new NativeClient();
        var transport = Transport(client, ownsClient: true);
        client.Senders[0].DisposeFailure = new ServiceBusException("close", ServiceBusFailureReason.ServiceCommunicationProblem);
        await Assert.That(async () => await transport.DisposeAsync()).Throws<ServiceBusException>();
        await Assert.That(client.Senders.All(sender => sender.Disposals == 1)).IsTrue();
        await Assert.That(client.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Sending_preserves_headers_native_callbacks_sessions_and_schedule_without_leaking_state()
    {
        var client = new NativeClient();
        await using var transport = Transport(client, sessions: true);
        var scheduled = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds());
        KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new("id-1")), new("traceparent", new("creation"u8.ToArray()))];
        var calls = 0;
        var options = new SendOptions().ServiceBus(message =>
        {
            calls++;
            message.Subject = "subject";
            message.ApplicationProperties["traceparent"] = "wrong";
            message.GetRawAmqpMessage().Footer["native"] = "value";
        });
        await transport.SendAsync(new("orders", new byte[] { 1, 2 }, headers, new WireString("customer"), scheduled, options), default);
        await transport.SendAsync(new("orders", new byte[] { 3 }), default);
        var sent = client.Senders[0].Sent;
        await Assert.That(sent[0].Body.ToMemory().Span.SequenceEqual(new byte[] { 1, 2 })).IsTrue();
        await Assert.That(sent[0].SessionId).IsEqualTo("customer");
        await Assert.That(sent[0].ScheduledEnqueueTime).IsEqualTo(scheduled);
        await Assert.That(sent[0].ApplicationProperties["cloudEvents:id"]).IsEqualTo("id-1");
        await Assert.That(sent[0].ApplicationProperties["traceparent"]).IsEqualTo("creation");
        await Assert.That(sent[0].Subject).IsEqualTo("subject");
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(sent[1].ApplicationProperties.Count).IsEqualTo(0);
        await Assert.That(sent[1].SessionId).IsNull();
        await Assert.That(sent[1].Subject).IsNull();
        await Assert.That(sent[1].ScheduledEnqueueTime).IsEqualTo(default(DateTimeOffset));
        await Assert.That(sent[1].GetRawAmqpMessage().Footer.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Serializer_content_type_uses_native_property_and_does_not_leak_between_sends(bool batch)
    {
        var client = new NativeClient { BatchCapacity = 1 };
        await using var transport = Transport(client);
        var native = new SendOptions().ServiceBus(message => message.ContentType = "application/native");
        KeyValuePair<string, WireString>[] utf8 = [new("content-type", new("application/x-protobuf"u8.ToArray()))];
        KeyValuePair<string, WireString>[] utf16 = [new("content-type", new("application/json"))];
        OutgoingMessage[] messages = [
            new("orders", new byte[] { 1 }, utf8, options: native),
            new("orders", new byte[] { 2 }),
            new("orders", new byte[] { 3 }, options: native),
            new("orders", new byte[] { 4 }, utf16)];
        if (batch) await transport.SendBatchAsync(messages, default);
        else foreach (var message in messages) await transport.SendAsync(message, default);

        var sent = client.Senders[0].Sent;
        await Assert.That(sent[0].ContentType).IsEqualTo("application/x-protobuf");
        await Assert.That(sent[1].ContentType).IsNull();
        await Assert.That(sent[2].ContentType).IsEqualTo("application/native");
        await Assert.That(sent[3].ContentType).IsEqualTo("application/json");
        await Assert.That(sent.All(message => !message.ApplicationProperties.ContainsKey("content-type"))).IsTrue();
    }

    [Test]
    public async Task Native_batches_split_by_bytes_destination_and_session_and_apply_callbacks_once()
    {
        var client = new NativeClient { BatchCapacity = 2 };
        await using var transport = Transport(client, sessions: true);
        var calls = 0;
        var options = new SendOptions().ServiceBus(_ => calls++);
        OutgoingMessage[] messages = [
            new("orders", new byte[] { 1 }, orderingKey: new WireString("a"), options: options),
            new("orders", new byte[] { 2 }, orderingKey: new WireString("a"), options: options),
            new("orders", new byte[] { 3 }, orderingKey: new WireString("a"), options: options),
            new("orders", new byte[] { 4 }, orderingKey: new WireString("b"), options: options),
            new("events", new byte[] { 5 }, options: options)];
        await transport.SendBatchAsync(messages, default);
        var orders = client.Senders.Single(sender => sender.Entity == "orders");
        var events = client.Senders.Single(sender => sender.Entity == "events");
        await Assert.That(orders.Batches.Select(batch => batch.Count).SequenceEqual(new[] { 2, 1, 1 })).IsTrue();
        await Assert.That(orders.Sent.Select(message => message.Body.ToMemory().Span[0]).SequenceEqual(new byte[] { 1, 2, 3, 4 })).IsTrue();
        await Assert.That(events.Sent.Single().Body.ToMemory().Span[0]).IsEqualTo((byte)5);
        await Assert.That(calls).IsEqualTo(5);
    }

    [Test]
    public async Task Mixed_destination_batches_trace_each_native_group_and_preserve_creation_context()
    {
        var services = new ServiceCollection();
        services.AddRelayer(_ => { });
        await using var provider = services.BuildServiceProvider();
        var tracing = provider.GetRequiredService<MessageTracing>();
        var spans = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var client = new NativeClient();
        await using var transport = new ServiceBusSendTransport("bus", client, false, Options(client),
            ["orders", "events"], TopologyMode.None, tracing: tracing);
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new("original")), new("traceparent", new(parent))];
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 1 }, headers),
            new OutgoingMessage("events", new byte[] { 2 }, headers) }, default);
        await Assert.That(spans.Count).IsEqualTo(2);
        await Assert.That(spans.Select(span => span.DisplayName).SequenceEqual(new[] { "send orders", "send events" })).IsTrue();
        await Assert.That(spans.All(span => Equals(span.GetTagItem("messaging.system"), "servicebus"))).IsTrue();
        await Assert.That(client.Senders.All(sender => Equals(sender.Sent.Single().ApplicationProperties["traceparent"], parent))).IsTrue();
    }

    [Test]
    public async Task Native_failures_cancellation_and_oversize_do_not_leak_or_send_empty_batches()
    {
        var client = new NativeClient();
        await using var transport = Transport(client);
        var sender = client.Senders[0];
        var failure = new ServiceBusException("unavailable", ServiceBusFailureReason.ServiceBusy);
        sender.Failure = failure;
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[] { 1 }), default)).Throws<ServiceBusException>();
        sender.Failure = null;
        await transport.SendAsync(new("orders", new byte[] { 2 }), default);
        await Assert.That(sender.Sent.Single().Body.ToMemory().Span[0]).IsEqualTo((byte)2);
        using var stop = new CancellationTokenSource();
        stop.Cancel();
        await Assert.That(async () => await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", default) }, stop.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[129]) }, default)).Throws<ServiceBusException>();
        await Assert.That(sender.Batches.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Pending_native_send_keeps_body_until_completion_and_forwards_cancellation()
    {
        var client = new NativeClient();
        await using var transport = Transport(client);
        var sender = client.Senders[0];
        sender.Pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();
        var send = transport.SendAsync(new("orders", new byte[] { 42 }), stop.Token);
        await Assert.That(send.IsCompleted).IsFalse();
        await Assert.That(sender.Borrowed!.Body.ToMemory().Span[0]).IsEqualTo((byte)42);
        await Assert.That(sender.Token).IsEqualTo(stop.Token);
        stop.Cancel();
        sender.Pending.SetCanceled(stop.Token);
        await Assert.That(async () => await send).Throws<OperationCanceledException>();
        await Assert.That(sender.Borrowed.Body.ToMemory().Length).IsEqualTo(0);
    }

    [Test]
    public async Task Native_batch_keeps_each_admitted_body_and_header_until_failure_then_releases_all_leases()
    {
        var client = new NativeClient();
        await using var transport = Transport(client);
        var sender = client.Senders[0];
        sender.Pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        KeyValuePair<string, WireString>[] firstHeaders = [new("id", new("first"))];
        KeyValuePair<string, WireString>[] secondHeaders = [new("id", new("second"))];
        var send = transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 1 }, firstHeaders),
            new OutgoingMessage("orders", new byte[] { 2 }, secondHeaders) }, default);
        await Assert.That(send.IsCompleted).IsFalse();
        var admitted = sender.BorrowedBatch!;
        await Assert.That(admitted[0].Body.ToMemory().Span[0]).IsEqualTo((byte)1);
        await Assert.That(admitted[1].Body.ToMemory().Span[0]).IsEqualTo((byte)2);
        await Assert.That(admitted[0].ApplicationProperties["id"]).IsEqualTo("first");
        await Assert.That(admitted[1].ApplicationProperties["id"]).IsEqualTo("second");
        sender.Pending.SetException(new ServiceBusException("lost", ServiceBusFailureReason.ServiceCommunicationProblem));
        await Assert.That(async () => await send).Throws<ServiceBusException>();
        await Assert.That(admitted.All(message => message.Body.ToMemory().IsEmpty && message.ApplicationProperties.Count == 0)).IsTrue();
        sender.Pending = null;
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 3 }) }, default);
        await Assert.That(sender.Sent.Last().Body.ToMemory().Span[0]).IsEqualTo((byte)3);
        await Assert.That(sender.Sent.Last().ApplicationProperties.ContainsKey("id")).IsFalse();
    }

    [Test]
    public async Task Validate_is_read_only_and_never_calls_administration_or_sends_probe_messages()
    {
        var client = new NativeClient();
        var admin = new NativeAdministration();
        var options = Options(client);
        options.Subscriptions.Add(new("events", "audit"));
        var topology = new ServiceBusTopology("bus", client, options, admin);
        var report = await topology.ApplyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(admin.Calls).IsEqualTo(0);
        await Assert.That(client.Senders.Sum(sender => sender.Sent.Count)).IsEqualTo(0);
        await Assert.That(client.Senders.All(sender => sender.BatchCreations == 1 && sender.Disposals == 1)).IsTrue();
        await Assert.That(client.Receiver.Peeks).IsEqualTo(1);
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Validate_reports_missing_entities_and_unresolved_sessions_without_advertising_them()
    {
        var client = new NativeClient { BatchFailure = new ServiceBusException("missing", ServiceBusFailureReason.MessagingEntityNotFound) };
        var options = Options(client, true);
        await using var transport = new ServiceBusSendTransport("bus", client, false, options, ["orders"], TopologyMode.Validate);
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Any(diff => diff.Contains("Missing send entity 'orders'"))).IsTrue();
        await Assert.That(report.Differences.Any(diff => diff.Contains("RequiresSession=true"))).IsTrue();
        await Assert.That(transport.GetCapabilities("orders").OrderedByKey).IsFalse();
        await Assert.That(transport.GetCapabilities("orders").Transactions).IsFalse();
        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsNull();
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.None)]
    public async Task Trusted_session_queue_supports_typed_ordered_sends_and_outbox_lanes(TopologyMode mode)
    {
        var client = new NativeClient();
        var admin = new NativeAdministration();
        var enlistment = new LaneEnlistment();
        var services = new ServiceCollection();
        services.AddScoped<IOutboxEnlistment>(_ => enlistment);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = mode;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.AdministrationClient = admin;
                    options.Queues.Add(new("orders") { RequiresSession = true });
                    options.TrustedSessionEntities.Add("orders");
                });
                bus.Send<First>("orders", route => route.OrderingKey(static _ => "customer"));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetServices<ITransport>().Single();
        var report = await transport.ApplyTopologyAsync(mode, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(admin.Calls).IsEqualTo(0);
        await Assert.That(transport.GetCapabilities("orders").OrderedByKey).IsTrue();
        await using var scope = provider.CreateAsyncScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendNowAsync(new First());
        await Assert.That(client.Senders[0].Sent.Single().SessionId).IsEqualTo("customer");
        await sender.SendViaOutboxAsync(new First());
        await Assert.That(enlistment.Message.OrderingKey!.Value.Span.SequenceEqual("customer"u8)).IsTrue();
        await Assert.That(enlistment.Message.Lane).IsNotNull();
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.None)]
    public async Task Topic_ordering_requires_trust_for_every_session_subscription(TopologyMode mode)
    {
        var client = new NativeClient();
        var options = Options(client);
        options.Subscriptions.Add(new("events", "audit") { RequiresSession = true });
        options.Subscriptions.Add(new("events", "billing") { RequiresSession = true });
        options.TrustedSessionEntities.Add("events/Subscriptions/audit");
        await using (var partial = new ServiceBusSendTransport("bus", client, false, options, ["events"], mode))
            await Assert.That(partial.GetCapabilities("events").OrderedByKey).IsFalse();
        options.TrustedSessionEntities.Add("events/Subscriptions/billing");
        await using var trusted = new ServiceBusSendTransport("bus", client, false, options, ["events"], mode);
        await Assert.That(trusted.GetCapabilities("events").OrderedByKey).IsTrue();
        var report = await trusted.ApplyTopologyAsync(mode, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(client.Receiver.Peeks).IsEqualTo(0); // Send-only startup never requests Listen.
        options.Subscriptions[1].RequiresSession = false;
        await using var mixed = new ServiceBusSendTransport("bus", client, false, options, ["events"], mode);
        await Assert.That(mixed.GetCapabilities("events").OrderedByKey).IsFalse();
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.None)]
    public async Task Untrusted_session_send_keeps_error_and_names_trust_option(TopologyMode mode)
    {
        var client = new NativeClient();
        await using var transport = new ServiceBusSendTransport("bus", client, false, Options(client, true), ["orders"], mode);
        await Assert.That(async () => await transport.SendAsync(new("orders", default, orderingKey: new WireString("customer")), default))
            .Throws<InvalidOperationException>().WithMessageContaining("TrustedSessionEntities");
    }

    [Test]
    [Arguments("missing")]
    [Arguments("orders")]
    [Arguments("events")]
    public async Task Trust_requires_a_session_enabled_native_entity_declaration(string entity)
    {
        var options = Options(new NativeClient());
        options.TrustedSessionEntities.Add(entity);
        await Assert.That(() => ServiceBusSendTransport.ValidateDeclarations("bus", options, ["orders"]))
            .Throws<InvalidOperationException>().WithMessageContaining("RequiresSession=true");
    }

    private sealed class LaneEnlistment : IOutboxEnlistment
    {
        public bool IsActive => true;
        internal BufferedOutboxMessage Message { get; } = new();
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            BufferedOutboxMessage.Validate(record);
            Message.Copy(record, 8);
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Provision_creates_only_missing_entities_reports_drift_and_propagates_sdk_failures()
    {
        var client = new NativeClient();
        var admin = new NativeAdministration();
        var options = Options(client, true);
        options.Subscriptions.Add(new("events", "audit") { RequiresSession = true });
        options.TrustedSessionEntities.Add("orders");
        var topology = new ServiceBusTopology("bus", client, options, admin);
        var report = await topology.ApplyAsync(TopologyMode.Provision, default);
        await Assert.That(admin.Created.SequenceEqual(new[] { "orders", "events", "events/Subscriptions/audit" })).IsTrue();
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        options.Queues[0].RequiresSession = false;
        report = await topology.ApplyAsync(TopologyMode.Provision, default);
        await Assert.That(report.Differences.Single()).Contains("RequiresSession expected 'False', actual 'True'");
        admin.Failure = new RequestFailedException(403, "denied");
        await Assert.That(async () => await topology.ApplyAsync(TopologyMode.Provision, default)).Throws<RequestFailedException>();
        using var stop = new CancellationTokenSource();
        stop.Cancel();
        await Assert.That(async () => await topology.ApplyAsync(TopologyMode.Provision, stop.Token)).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Provision_reports_a_false_trusted_session_assertion()
    {
        var client = new NativeClient();
        var admin = new NativeAdministration();
        var options = Options(client);
        var topology = new ServiceBusTopology("bus", client, options, admin);
        await topology.ApplyAsync(TopologyMode.Provision, default);
        options.Queues[0].RequiresSession = true;
        options.TrustedSessionEntities.Add("orders");
        var report = await topology.ApplyAsync(TopologyMode.Provision, default);
        await Assert.That(report.Differences.Single()).Contains("RequiresSession expected 'True', actual 'False'");
    }

    [Test]
    public async Task Invalid_ownership_undeclared_entities_and_consumers_fail_before_client_creation()
    {
        var client = new NativeClient();
        foreach (var invalid in new[] { "both", "neither", "entity", "consumer" })
        {
            var services = new ServiceCollection();
            services.AddRelayer(builder => builder.ServiceBus("bus", bus =>
            {
                bus.Options.Configure(options =>
                {
                    if (invalid != "neither") options.Client = client;
                    if (invalid == "both") options.ConnectionString = "invalid";
                });
#pragma warning disable RLY004 // Deliberately invalid consumer tests fail-fast adapter construction.
                if (invalid == "consumer") bus.Consume("orders", _ => { });
#pragma warning restore RLY004
                else bus.Send<First>("orders");
            }));
            await using var provider = services.BuildServiceProvider();
            await Assert.That(() => provider.GetServices<ITransport>().ToArray()).ThrowsException();
        }
        await Assert.That(client.Senders.Count).IsEqualTo(0);
    }

    internal static ServiceBusTransportOptions Options(ServiceBusClient client, bool sessions = false)
    {
        var options = new ServiceBusTransportOptions { Client = client };
        options.Queues.Add(new("orders") { RequiresSession = sessions });
        options.Topics.Add(new("events"));
        return options;
    }

    private static ServiceBusSendTransport Transport(NativeClient client, bool sessions = false, bool ownsClient = false) =>
        new("bus", client, ownsClient, Options(client, sessions), ["orders", "events"], TopologyMode.Provision);

    internal sealed class NativeClient : ServiceBusClient
    {
        internal List<NativeSender> Senders { get; } = [];
        internal NativeReceiver Receiver { get; } = new();
        internal int Disposals;
        internal int BatchCapacity = 2;
        internal Exception? BatchFailure;
        public override string FullyQualifiedNamespace => "test.servicebus.windows.net";
        public override ServiceBusSender CreateSender(string queueOrTopicName)
        {
            var sender = new NativeSender(queueOrTopicName) { Capacity = BatchCapacity, BatchFailure = BatchFailure };
            Senders.Add(sender);
            return sender;
        }
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions? options = null) => Receiver;
        public override ValueTask DisposeAsync() { Disposals++; return ValueTask.CompletedTask; }
    }

    internal sealed class NativeSender(string entity) : ServiceBusSender
    {
        internal string Entity => entity;
        internal readonly List<ServiceBusMessage> Sent = [];
        internal readonly List<List<ServiceBusMessage>> Batches = [];
        private List<ServiceBusMessage> _batch = [];
        internal int Capacity;
        internal int Disposals;
        internal int BatchCreations;
        internal Exception? Failure;
        internal Exception? DisposeFailure;
        internal Exception? BatchFailure;
        internal TaskCompletionSource? Pending;
        internal ServiceBusMessage? Borrowed;
        internal List<ServiceBusMessage>? BorrowedBatch;
        internal CancellationToken Token;

        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        {
            Token = cancellationToken;
            Borrowed = message;
            if (Failure is { } failure) return Task.FromException(failure);
            Sent.Add(Snapshot(message));
            return Pending?.Task ?? Task.CompletedTask;
        }
        public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default)
        {
            Token = cancellationToken;
            BatchCreations++;
            if (BatchFailure is { } failure) return ValueTask.FromException<ServiceBusMessageBatch>(failure);
            var store = new List<ServiceBusMessage>();
            _batch = store;
            return new(ServiceBusModelFactory.ServiceBusMessageBatch(128, store, new() { MaxSizeInBytes = 128 }, message =>
            {
                if (store.Count >= Capacity || message.Body.ToMemory().Length > 128) return false;
                return true;
            }));
        }
        public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default)
        {
            Token = cancellationToken;
            if (Failure is { } failure) return Task.FromException(failure);
            BorrowedBatch = _batch;
            var snapshots = _batch.Select(Snapshot).ToList();
            Batches.Add(snapshots);
            Sent.AddRange(snapshots);
            return Pending?.Task ?? Task.CompletedTask;
        }
        public override ValueTask DisposeAsync()
        {
            Disposals++;
            return DisposeFailure is { } failure ? ValueTask.FromException(failure) : ValueTask.CompletedTask;
        }
        private static ServiceBusMessage Snapshot(ServiceBusMessage message) =>
            new(AmqpAnnotatedMessage.FromBytes(message.GetRawAmqpMessage().ToBytes()));
    }

    internal sealed class NativeReceiver : ServiceBusReceiver
    {
        internal int Peeks;
        internal int Disposals;
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default)
        { Peeks++; return Task.FromResult<ServiceBusReceivedMessage>(null!); }
        public override ValueTask DisposeAsync() { Disposals++; return ValueTask.CompletedTask; }
    }

    private sealed class NativeAdministration : ServiceBusAdministrationClient
    {
        internal int Calls;
        internal List<string> Created { get; } = [];
        internal QueueProperties? Queue;
        internal TopicProperties? Topic;
        internal SubscriptionProperties? Subscription;
        internal Exception? Failure;
        public override Task<Response<QueueProperties>> GetQueueAsync(string name, CancellationToken cancellationToken = default)
        { Calls++; return Read(Queue); }
        public override Task<Response<TopicProperties>> GetTopicAsync(string name, CancellationToken cancellationToken = default)
        { Calls++; return Read(Topic); }
        public override Task<Response<SubscriptionProperties>> GetSubscriptionAsync(string topicName, string subscriptionName, CancellationToken cancellationToken = default)
        { Calls++; return Read(Subscription); }
        private Task<Response<T>> Read<T>(T? value) where T : class => Failure is { } failure ? Task.FromException<Response<T>>(failure)
            : value is null ? Task.FromException<Response<T>>(new ServiceBusException("missing", ServiceBusFailureReason.MessagingEntityNotFound))
            : Task.FromResult(Response.FromValue(value, null!));
        public override Task<Response<QueueProperties>> CreateQueueAsync(CreateQueueOptions o, CancellationToken cancellationToken = default)
        {
            Calls++; Created.Add(o.Name);
            Queue = ServiceBusModelFactory.QueueProperties(o.Name, o.LockDuration, o.MaxSizeInMegabytes, o.RequiresDuplicateDetection, o.RequiresSession,
                o.DefaultMessageTimeToLive, o.AutoDeleteOnIdle, o.DeadLetteringOnMessageExpiration, o.DuplicateDetectionHistoryTimeWindow,
                o.MaxDeliveryCount, o.EnableBatchedOperations, o.Status, o.ForwardTo, o.ForwardDeadLetteredMessagesTo, o.UserMetadata ?? "", o.EnablePartitioning);
            return Task.FromResult(Response.FromValue(Queue, null!));
        }
        public override Task<Response<TopicProperties>> CreateTopicAsync(CreateTopicOptions o, CancellationToken cancellationToken = default)
        {
            Calls++; Created.Add(o.Name);
            Topic = ServiceBusModelFactory.TopicProperties(o.Name, o.MaxSizeInMegabytes, o.RequiresDuplicateDetection, o.DefaultMessageTimeToLive,
                o.AutoDeleteOnIdle, o.DuplicateDetectionHistoryTimeWindow, o.EnableBatchedOperations, o.Status, o.EnablePartitioning);
            return Task.FromResult(Response.FromValue(Topic, null!));
        }
        public override Task<Response<SubscriptionProperties>> CreateSubscriptionAsync(CreateSubscriptionOptions o, CancellationToken cancellationToken = default)
        {
            Calls++; Created.Add(o.TopicName + "/Subscriptions/" + o.SubscriptionName);
            Subscription = ServiceBusModelFactory.SubscriptionProperties(o.TopicName, o.SubscriptionName, o.LockDuration, o.RequiresSession,
                o.DefaultMessageTimeToLive, o.AutoDeleteOnIdle, o.DeadLetteringOnMessageExpiration, o.MaxDeliveryCount,
                o.EnableBatchedOperations, o.Status, o.ForwardTo, o.ForwardDeadLetteredMessagesTo, o.UserMetadata ?? "");
            return Task.FromResult(Response.FromValue(Subscription, null!));
        }
    }

    [Message("servicebus.first")]
    public sealed record First;
    [Message("servicebus.second")]
    public sealed record Second;
}
