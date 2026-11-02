using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusReceiveTests
{
    [Test]
    public async Task Queue_and_subscription_processors_keep_native_options_and_complete_after_handler_work()
    {
        var client = new NativeClient();
        var native = new ServiceBusProcessorOptions { AutoCompleteMessages = false, PrefetchCount = 17 };
        await using var provider = Provider(client, consumer => consumer.Options.ProcessorOptions = native, subscription: "audit");
        await Start(provider);
        var processor = client.Processors.Single();
        await Assert.That(processor.Source).IsEqualTo("events");
        await Assert.That(processor.Subscription).IsEqualTo("audit");
        await Assert.That(processor.Options).IsSameReferenceAs(native);
        var control = provider.GetRequiredService<Control>();
        var receiver = new NativeReceiver();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Handle = async (_, context, _) =>
        {
            await Assert.That(context.ServiceBus().Message).IsSameReferenceAs(receiver.Incoming);
            entered.SetResult();
            await release.Task;
        };
        var processing = processor.Emit(Args(receiver));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(receiver.Operations.Count).IsEqualTo(0);
        release.SetResult();
        await processing;
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "complete" });
        await Stop(provider);
    }

    [Test]
    public async Task Session_processor_preserves_native_callback_options_and_manual_completion()
    {
        var client = new NativeClient();
        var options = new ServiceBusSessionProcessorOptions { AutoCompleteMessages = false, MaxConcurrentSessions = 1, PrefetchCount = 13 };
        await using var provider = Provider(client, consumer => consumer.Options.SessionProcessorOptions = options, sessions: true);
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("{\"Value\":42}"), sessionId: "customer", deliveryCount: 1,
            properties: new Dictionary<string, object> { ["cloudEvents:id"] = "id", ["cloudEvents:type"] = "servicebus.receive-test;v=1" });
        var receiver = new NativeSessionReceiver();
        var callback = new ProcessSessionMessageEventArgs(message, receiver, "session-test", default);
        provider.GetRequiredService<Control>().Handle = async (_, context, _) =>
        {
            await Assert.That(context.ServiceBus().SessionCallback).IsSameReferenceAs(callback);
            await Assert.That(context.ServiceBus().Message).IsSameReferenceAs(message);
            await Assert.That(() => context.ServiceBus().Callback).Throws<InvalidOperationException>();
            await context.ServiceBus().CompleteAsync();
        };
        await Start(provider);
        var processor = client.SessionProcessors.Single();
        await Assert.That(processor.Options).IsSameReferenceAs(options);
        await processor.Emit(callback);
        await Assert.That(receiver.Completions).IsEqualTo(1);
        await Stop(provider);
    }

    [Test]
    public async Task Failure_pipeline_dead_letters_poison_and_continues_with_healthy_delivery()
    {
        var client = new NativeClient();
        await using var provider = Provider(client);
        var control = provider.GetRequiredService<Control>();
        control.Handle = (message, _, _) => message.Value == 1
            ? ValueTask.FromException(new InvalidOperationException("poison")) : ValueTask.CompletedTask;
        await Start(provider);
        var processor = client.Processors.Single();
        var receiver = new NativeReceiver();
        await processor.Emit(Args(receiver, 1));
        await Assert.That(receiver.Operations.Single()).IsEqualTo("dead_letter");
        await Assert.That(receiver.Reason).IsEqualTo("Processing failed");
        await Assert.That(receiver.Description).IsEqualTo("poison");
        await Assert.That(receiver.Properties!.Count > 0).IsTrue();
        await processor.Emit(Args(receiver, 2));
        await Assert.That(receiver.Operations.Last()).IsEqualTo("complete");
        await Stop(provider);
    }

    [Test]
    [Arguments("complete")]
    [Arguments("abandon")]
    [Arguments("defer")]
    [Arguments("dead_letter")]
    public async Task Manual_settlement_runs_once_and_has_native_settlement_span(string operation)
    {
        var client = new NativeClient();
        await using var provider = Provider(client);
        var tracing = provider.GetRequiredService<MessageTracing>();
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        provider.GetRequiredService<Control>().Handle = async (_, context, _) =>
        {
            var native = context.ServiceBus();
            if (operation == "complete") await native.CompleteAsync();
            else if (operation == "abandon") await native.AbandonAsync();
            else if (operation == "defer") await native.DeferAsync();
            else await native.DeadLetterAsync("manual", "details");
        };
        await Start(provider);
        var receiver = new NativeReceiver();
        await client.Processors.Single().Emit(Args(receiver));
        await Assert.That(receiver.Operations.Single()).IsEqualTo(operation);
        var process = spans.Single(span => Equals(span.GetTagItem("messaging.operation.type"), "process"));
        var settle = spans.Single(span => Equals(span.GetTagItem("messaging.operation.type"), "settle"));
        await Assert.That(settle.DisplayName).IsEqualTo(operation + " orders");
        await Assert.That(settle.ParentSpanId).IsEqualTo(process.SpanId);
        await Assert.That(settle.GetTagItem("messaging.servicebus.disposition_status")).IsNotNull();
        await Assert.That(process.GetTagItem("messaging.servicebus.message.delivery_count")).IsEqualTo(1);
        await Assert.That(process.GetTagItem("messaging.servicebus.message.enqueued_time")).IsNotNull();
        await Stop(provider);
    }

    [Test]
    public async Task Native_settlement_failure_never_completes_or_reenters_handler()
    {
        var client = new NativeClient();
        await using var provider = Provider(client);
        var calls = 0;
        provider.GetRequiredService<Control>().Handle = (_, _, _) => { calls++; return default; };
        await Start(provider);
        var receiver = new NativeReceiver { Failure = new ServiceBusException("lost", ServiceBusFailureReason.MessageLockLost) };
        await Assert.That(async () => await client.Processors.Single().Emit(Args(receiver))).Throws<ServiceBusException>();
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "complete" });
        await Stop(provider);
    }

    [Test]
    public async Task Successful_native_settlement_then_handler_error_does_not_retry_or_settle_again()
    {
        var client = new NativeClient();
        await using var provider = Provider(client, consumer => consumer.Options.Failure = new() { RetryDelays = new[] { TimeSpan.Zero } });
        var calls = 0;
        provider.GetRequiredService<Control>().Handle = async (_, context, _) =>
        {
            calls++;
            await context.ServiceBus().CompleteAsync();
            throw new InvalidOperationException("Failure after native completion");
        };
        await Start(provider);
        var receiver = new NativeReceiver();
        await Assert.That(async () => await client.Processors.Single().Emit(Args(receiver))).Throws<InvalidOperationException>();
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "complete" });
        await Stop(provider);
    }

    [Test]
    public async Task Portable_request_rejects_manual_settlement_before_touching_the_broker()
    {
        var client = new NativeClient();
        await using var provider = Provider(client);
        provider.GetRequiredService<Control>().Handle = async (_, context, _) =>
        {
            context.Features.GetRequired<ISettlementFeature>().Complete();
            await Assert.That(async () => await context.ServiceBus().CompleteAsync()).Throws<InvalidOperationException>();
        };
        await Start(provider);
        var receiver = new NativeReceiver();
        await client.Processors.Single().Emit(Args(receiver));
        await Assert.That(receiver.Operations.ToArray()).IsEquivalentTo(new[] { "complete" });
        await Stop(provider);
    }

    [Test]
    public async Task Optional_health_registration_uses_exact_key_and_stopped_consumers_are_unavailable()
    {
        var client = new NativeClient();
        await using var provider = Provider(client, health: true);
        await Assert.That(provider.GetKeyedService<IConsumerHealthProvider>("BUS")).IsNull();
        await Assert.That(provider.GetKeyedService<ITransportHealthProbe>("BUS")).IsNull();
        var health = provider.GetRequiredKeyedService<IConsumerHealthProvider>("bus");
        var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers.Single();
        await Assert.That(await health.GetStateAsync(declaration, default)).IsNull();
        await Start(provider);
        await Assert.That((await health.GetStateAsync(declaration, default))!.Value.IsIdle).IsTrue();
        await Stop(provider);
        await Assert.That(await health.GetStateAsync(declaration, default)).IsNull();
    }

    [Test]
    public async Task Liveness_reports_oldest_active_worker_until_that_worker_finishes()
    {
        var first = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var declaration = new ConsumerDefinition<ServiceBusConsumerOptions>("orders", new(), [], 2, 1, TimeSpan.FromMilliseconds(100));
        var clock = new TestTimeProvider();
        var activated = clock.Now;
        var calls = 0;
        var processor = new NativeProcessor("orders", null, new() { MaxConcurrentCalls = 2, AutoCompleteMessages = false });
        await using var consumer = new ServiceBusConsumer(declaration, processor,
            (_, _) => new(++calls == 1 ? first.Task : second.Task), clock) { TrackHealth = true };
        await consumer.StartAsync(default);
        var one = processor.Emit(Args(new()));
        clock.Now = clock.Now.AddMinutes(1);
        var two = processor.Emit(Args(new()));
        var state = consumer.GetHealthState()!.Value;
        await Assert.That(state.IsIdle).IsFalse();
        await Assert.That(state.LastProgressAt).IsEqualTo(activated);
        first.SetResult(Disposition.Complete);
        await one;
        await Assert.That(consumer.GetHealthState()!.Value.LastProgressAt).IsEqualTo(clock.Now);
        second.SetResult(Disposition.Complete);
        await two;
        await Assert.That(consumer.GetHealthState()!.Value.IsIdle).IsTrue();
        await consumer.StopAsync(default);
        await Assert.That(consumer.GetHealthState()).IsNull();
    }

    [Test]
    [Arguments("auto-complete")]
    [Arguments("receive-delete")]
    [Arguments("batch")]
    [Arguments("undeclared")]
    public async Task Unsupported_declarations_fail_before_starting_processors(string invalid)
    {
        var client = new NativeClient();
        await using var provider = Provider(client, consumer =>
        {
            if (invalid == "auto-complete") consumer.Options.ProcessorOptions.AutoCompleteMessages = true;
            if (invalid == "receive-delete") consumer.Options.ProcessorOptions.ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete;
            if (invalid == "batch") consumer.Batch(2, TimeSpan.FromMilliseconds(10));
        }, declare: invalid != "undeclared");
        await Assert.That(async () => await Start(provider)).ThrowsException();
        await Assert.That(client.Processors.All(processor => processor.Starts == 0)).IsTrue();
    }

    internal static ServiceProvider Provider(NativeClient client, Action<ConsumerBuilder<ServiceBusConsumerOptions>>? configure = null,
        string? subscription = null, bool declare = true, bool health = false, bool sessions = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Control>();
        if (health) services.AddRelayerServiceBusHealthProviders("bus");
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    if (!declare) return;
                    if (subscription is null) options.Queues.Add(new("orders") { RequiresSession = sessions });
                    else
                    {
                        options.Topics.Add(new("events"));
                        options.Subscriptions.Add(new("events", subscription));
                    }
                });
                bus.Consume(subscription is null ? "orders" : "events", consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.Subscription = subscription;
                    configure?.Invoke(consumer);
                });
            });
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    internal static async Task Start(ServiceProvider provider, CancellationToken token = default)
    {
        var services = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in services.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(token);
        foreach (var service in services) await service.StartAsync(token);
    }

    internal static async Task Stop(ServiceProvider provider, CancellationToken token = default)
    {
        foreach (var service in provider.GetServices<IHostedService>().Reverse()) await service.StopAsync(token);
    }

    internal static ProcessMessageEventArgs Args(NativeReceiver receiver, int value = 42, int delivery = 1)
    {
        receiver.Incoming = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData($"{{\"Value\":{value}}}"),
            messageId: "native-id", deliveryCount: delivery, enqueuedTime: DateTimeOffset.UtcNow,
            properties: new Dictionary<string, object>
            {
                ["cloudEvents:id"] = "creation-id", ["cloudEvents:type"] = "servicebus.receive-test;v=1",
                ["traceparent"] = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            });
        return new(receiver.Incoming, receiver, "test", default);
    }

    [Message("servicebus.receive-test")]
    public sealed record Payload(int Value);

    public sealed class Control
    {
        public Func<Payload, MessageContext, CancellationToken, ValueTask> Handle { get; set; } = static (_, _, _) => default;
    }

    public sealed class Handler(Control control) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) =>
            control.Handle(message, context, cancellationToken);
    }

    internal sealed class NativeClient : ServiceBusClient
    {
        internal List<NativeProcessor> Processors { get; } = [];
        internal List<NativeSessionProcessor> SessionProcessors { get; } = [];
        public override string FullyQualifiedNamespace => "test.servicebus.windows.net";
        public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options) => Add(queueName, null, options);
        public override ServiceBusProcessor CreateProcessor(string topicName, string subscriptionName, ServiceBusProcessorOptions options) =>
            Add(topicName, subscriptionName, options);
        public override ServiceBusReceiver CreateReceiver(string queueName) => new NativeReceiver();
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => new NativeReceiver();
        public override ServiceBusSessionProcessor CreateSessionProcessor(string queueName, ServiceBusSessionProcessorOptions options)
        {
            var processor = new NativeSessionProcessor(options);
            SessionProcessors.Add(processor);
            return processor;
        }
        private NativeProcessor Add(string source, string? subscription, ServiceBusProcessorOptions options)
        {
            var processor = new NativeProcessor(source, subscription, options);
            Processors.Add(processor);
            return processor;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    internal sealed class NativeProcessor(string source, string? subscription, ServiceBusProcessorOptions options) : ServiceBusProcessor
    {
        internal string Source => source;
        internal string? Subscription => subscription;
        internal ServiceBusProcessorOptions Options => options;
        internal int Starts;
        internal int Stops;
        internal int Disposals;
        internal Task Emit(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) { Starts++; return Task.CompletedTask; }
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) { Stops++; return Task.CompletedTask; }
        public override Task CloseAsync(CancellationToken cancellationToken = default) { Disposals++; return Task.CompletedTask; }
    }

    internal sealed class NativeReceiver : ServiceBusReceiver
    {
        internal readonly ConcurrentQueue<string> Operations = new();
        internal ServiceBusReceivedMessage? Incoming;
        internal IDictionary<string, object>? Properties;
        internal string? Reason;
        internal string? Description;
        internal Exception? Failure;
        private Task Record(string operation, IDictionary<string, object>? properties = null, string? reason = null, string? description = null)
        {
            Operations.Enqueue(operation);
            Properties = properties;
            Reason = reason;
            Description = description;
            return Failure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) => Record("complete");
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) => Record("abandon", propertiesToModify);
        public override Task DeferMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) => Record("defer", propertiesToModify);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify,
            string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) =>
            Record("dead_letter", propertiesToModify, deadLetterReason, deadLetterErrorDescription);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, string deadLetterReason,
            string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) =>
            Record("dead_letter", reason: deadLetterReason, description: deadLetterErrorDescription);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NativeSessionReceiver : ServiceBusSessionReceiver
    {
        internal int Completions;
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        { Completions++; return Task.CompletedTask; }
    }

    internal sealed class NativeSessionProcessor(ServiceBusSessionProcessorOptions options) : ServiceBusSessionProcessor
    {
        protected override ServiceBusProcessor InnerProcessor { get; } = new NativeProcessor("sessions", null, new() { AutoCompleteMessages = false });
        internal ServiceBusSessionProcessorOptions Options => options;
        internal Task Emit(ProcessSessionMessageEventArgs args) => OnProcessSessionMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class TestTimeProvider : TimeProvider
    {
        internal DateTimeOffset Now = new(2026, 11, 1, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }
}

[JsonSerializable(typeof(ServiceBusReceiveTests.Payload))]
internal partial class ServiceBusReceiveTestJson : JsonSerializerContext;
