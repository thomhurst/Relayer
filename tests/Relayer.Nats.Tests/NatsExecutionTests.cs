using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Serialization;

namespace Relayer.Nats.Tests;

public class NatsExecutionTests
{
    [Test]
    public async Task Repeated_start_during_outage_does_not_ping_or_close_the_running_transport()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        // This test changes server-wide connectivity and therefore owns its broker.
        await using var broker = await NatsTopologyTests.Broker.StartAsync(token);
        await using var provider = NatsConfigurationTests.Provider(options => options.ConnectionOptions = broker.Options());
        var transport = provider.GetRequiredService<Relayer.Transports.ITransport>();
        await transport.StartAsync(token);
        await broker.SetConnectedAsync(false, token);
        // Startup is idempotent; native reconnect owns the existing connection during an outage.
        using var secondStart = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await transport.StartAsync(secondStart.Token);
        await broker.SetConnectedAsync(true, token);
        await transport.StopAsync(token);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Hosted_pipeline_retries_and_confirms_poison_copy_before_acknowledging_original(bool multipleTraceHeaders)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("events");
        var orders = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("dead");
        var control = new ExecutionControl();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(control);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Handler<ExecutionHandler>();
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events) { Configuration = new(events, [orders, deadSubject]) });
                    options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { FilterSubject = orders } });
                });
                transport.Send<ExecutionMessage>(orders);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Options.Settings = new() { MaxMsgs = 1, Expires = TimeSpan.FromSeconds(1), DrainOnCancel = true };
                    consumer.Options.Failure = new() { RetryDelays = new[] { TimeSpan.Zero }, DeadLetterEndpoint = deadSubject };
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(events, new("dead_reader") { FilterSubject = deadSubject }, token);
        var poison = new byte[] { 1, 2, 3 };
        var poisonHeaders = new NatsPublishHeaders(false)
        {
            ["original"] = new Microsoft.Extensions.Primitives.StringValues(["first", "second"]),
            ["Nats-Msg-Id"] = "original-poison-id",
            TraceParent = "00-11111111111111111111111111111111-2222222222222222-01",
        };
        if (multipleTraceHeaders)
        {
            poisonHeaders.TraceParent = new Microsoft.Extensions.Primitives.StringValues([poisonHeaders.TraceParent.ToString(), "malformed-parent"]);
            poisonHeaders.TraceState = new Microsoft.Extensions.Primitives.StringValues(["first=value", "second=value"]);
            poisonHeaders["ce-id"] = new Microsoft.Extensions.Primitives.StringValues(["first-id", "second-id"]);
        }
        var ack = await js.PublishAsync(orders, new ReadOnlyMemory<byte>(poison), poisonHeaders,
            headers: poisonHeaders, cancellationToken: token);
        ack.EnsureSuccess();
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new ExecutionMessage(42),
            options => options.Nats(headers => headers["custom"] = "native"), token);
        await control.Completed.Task.WaitAsync(token);
        var dead = await js.GetConsumerAsync(events, "dead_reader", token);
        var copy = await dead.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(poison);
        await Assert.That(copy.Headers!["original"].ToArray()).IsEquivalentTo(new string?[] { "first", "second" });
        await Assert.That(copy.Headers.TryGetLastValue("original", out var last)).IsTrue();
        await Assert.That(last).IsEqualTo("second");
        await Assert.That(copy.Headers.ContainsKey("Nats-Msg-Id")).IsFalse();
        await Assert.That(copy.Headers["traceparent"].ToArray()).IsEquivalentTo(poisonHeaders.TraceParent.ToArray());
        if (multipleTraceHeaders)
        {
            await Assert.That(copy.Headers["tracestate"].ToArray()).IsEquivalentTo(poisonHeaders.TraceState.ToArray());
            await Assert.That(copy.Headers["ce-id"].ToArray()).IsEquivalentTo(new string?[] { "first-id", "second-id" });
        }
        await copy.AckAsync(new() { DoubleAck = true }, token);
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(2UL);
        await Assert.That(control.Attempts).IsEqualTo(2);
    }
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Poison_copy_drops_original_and_configured_publication_preconditions(bool separateDeadStream)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("precondition_events");
        var deadStream = separateDeadStream ? events + "_dead" : events;
        var orders = TestContext.Current!.Isolation.GetIsolatedName("precondition_orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("precondition_dead");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new ExecutionControl());
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Handler<ExecutionHandler>();
            relayer.Nats("preconditions", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    // These publication-only options are deliberately invalid for a failure copy.
                    options.PublishOptions = new()
                    {
                        ExpectedStream = events,
                        ExpectedLastSequence = 0,
                        ExpectedLastMsgId = "not-the-last-id",
                        ExpectedLastSubjectSequence = 0,
                        ExpectedLastSubjectSequenceSubject = orders,
                    };
                    options.Streams.Add(new(events) { Configuration = new(events, separateDeadStream ? [orders] : [orders, deadSubject]) });
                    if (separateDeadStream) options.Streams.Add(new(deadStream) { Configuration = new(deadStream, [deadSubject]) });
                    options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { FilterSubject = orders } });
                });
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Options.Failure = new() { DeadLetterEndpoint = deadSubject };
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(deadStream, new("dead_reader") { FilterSubject = deadSubject }, token);
        var headers = new NatsHeaders
        {
            ["Nats-Expected-Stream"] = events,
            ["Nats-Expected-Last-Sequence"] = "0",
            ["Nats-Expected-Last-Subject-Sequence"] = "0",
            ["nats-expected-last-msg-id"] = "noncanonical-original-check",
            ["Nats-Msg-Id"] = "original-id",
            ["ce-id"] = "preserved-id",
            ["application"] = "preserved-value",
        };
        byte[] poison = [1, 2, 3];
        (await js.PublishAsync(orders, poison, NatsRawSerializer<byte[]>.Default, headers: headers, cancellationToken: token)).EnsureSuccess();
        var reader = await js.GetConsumerAsync(deadStream, "dead_reader", token);
        var copy = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(poison);
        await Assert.That(copy.Headers!["ce-id"].ToString()).IsEqualTo("preserved-id");
        await Assert.That(copy.Headers["application"].ToString()).IsEqualTo("preserved-value");
        foreach (var header in copy.Headers)
            await Assert.That(header.Key.StartsWith("Nats-Expected-", StringComparison.OrdinalIgnoreCase)).IsFalse();
        await Assert.That(copy.Headers.ContainsKey("Nats-Msg-Id")).IsFalse();
        await copy.AckAsync(new() { DoubleAck = true }, token);
        // Graceful stop waits for the original delivery's confirmed ACK after the durable copy.
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(1UL);
    }

    [Test]
    [Arguments("sub")]
    [Arguments("all")]
    public async Task Poison_copy_drops_rollup_without_purging_previous_dead_letters(string rollup)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("rollup_events");
        var deadStream = events + "_dead";
        var orders = TestContext.Current!.Isolation.GetIsolatedName("rollup_orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("rollup_dead");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new ExecutionControl());
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Handler<ExecutionHandler>();
            relayer.Nats("rollup", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events) { Configuration = new(events, [orders]) { AllowRollupHdrs = true } });
                    options.Streams.Add(new(deadStream) { Configuration = new(deadStream, [deadSubject]) { AllowRollupHdrs = true } });
                    options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { FilterSubject = orders } });
                });
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Options.Failure = new() { DeadLetterEndpoint = deadSubject };
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(deadStream, new("dead_reader") { FilterSubject = deadSubject }, token);
        byte[] retained = [4, 5, 6];
        (await js.PublishAsync(deadSubject, retained, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
        var headers = new NatsHeaders
        {
            ["Nats-Rollup"] = rollup,
            ["application"] = "preserved-value",
        };
        byte[] poison = [1, 2, 3];
        (await js.PublishAsync(orders, poison, NatsRawSerializer<byte[]>.Default, headers: headers, cancellationToken: token)).EnsureSuccess();
        var reader = await js.GetConsumerAsync(deadStream, "dead_reader", token);
        var previous = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(previous).IsNotNull();
        await Assert.That(previous!.Data).IsEquivalentTo(retained);
        await previous.AckAsync(new() { DoubleAck = true }, token);
        var copy = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(poison);
        await Assert.That(copy.Headers!["application"].ToString()).IsEqualTo("preserved-value");
        await Assert.That((await js.GetStreamAsync(deadStream, cancellationToken: token)).Info.State.Messages).IsEqualTo(2L);
        await Assert.That(copy.Headers.ContainsKey("Nats-Rollup")).IsFalse();
        await copy.AckAsync(new() { DoubleAck = true }, token);
        // Graceful stop waits for the original delivery's confirmed ACK after the durable copy.
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(1UL);
    }

    [Test]
    public async Task Failed_confirmed_ack_leaves_native_redelivery_running()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("ack_events");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("ack_orders");
        var capture = new AckFailureCapture();
        using var logs = LoggerFactory.Create(logging => logging.SetMinimumLevel(LogLevel.Debug).AddProvider(capture));
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(logs);
        services.AddRelayer(relayer => relayer.Nats("ack_failure", transport =>
        {
            transport.Options.Configure(options =>
            {
                options.ConnectionOptions = broker.Options() with { LoggerFactory = logs };
                options.Streams.Add(new(events) { Configuration = new(events, [subject]) });
                options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { AckWait = TimeSpan.FromMilliseconds(200) } });
            });
            transport.Send<ExecutionMessage>(subject);
        }));
        await using var provider = services.BuildServiceProvider();
        var adapter = (NatsNativeTransport)provider.GetRequiredService<Relayer.Transports.ITransport>();
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        var worker = adapter.AddConsumer(events, "worker", new() { MaxMsgs = 1, DrainOnCancel = true, Expires = TimeSpan.FromSeconds(1) }, 2);
        var failedAckCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redeliveryReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        ulong originalSequence = 0;
        worker.Receive = async (delivery, processing) =>
        {
            try
            {
                var metadata = delivery.Message.Metadata!.Value;
                Interlocked.Increment(ref calls);
                if (metadata.NumDelivered == 1)
                {
                    originalSequence = metadata.Sequence.Stream;
                    // Callback scheduling is not broker delivery order. Keep the original active
                    // until the second worker receives native redelivery.
                    await redeliveryReceived.Task.WaitAsync(processing);
                    // A real native confirmed ACK request to an unsubscribed subject fails without
                    // acknowledging the original. Observe the failure, rather than assuming it happened.
                    var failed = new NatsJSMsg<ReadOnlyMemory<byte>>(new NatsMsg<ReadOnlyMemory<byte>>(
                        subject, subject + ".no_ack_receiver", 0, null, delivery.Message.Data, delivery.Message.Connection),
                        new NatsJSContext(delivery.Message.Connection!));
                    await worker.SettleAsync(new(failed, delivery.Consumer), Disposition.Complete, processing);
                    failedAckCompleted.TrySetResult();
                }
                else
                {
                    redeliveryReceived.TrySetResult();
                    // AckWait can expire while the other callback is still in its failed request.
                    // Fence the assertion behind that request, without delaying native redelivery.
                    await failedAckCompleted.Task.WaitAsync(token);
                    await Assert.That(metadata.Sequence.Stream).IsEqualTo(originalSequence);
                    await Assert.That(metadata.NumDelivered).IsGreaterThan(1UL);
                    await Assert.That(delivery.Message.Data.ToArray()).IsEquivalentTo(new byte[] { 42 });
                    await worker.SettleAsync(delivery, Disposition.Complete, processing);
                    redelivered.TrySetResult();
                }
                return Disposition.SettledByHandler;
            }
            catch (Exception error)
            {
                // The worker intentionally catches callback errors to keep consuming. Test assertions
                // must also reach the test, instead of turning into an unexplained redelivery timeout.
                callbackFailure.TrySetException(error);
                failedAckCompleted.TrySetException(error);
                throw;
            }
        };
        await adapter.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        (await js.PublishAsync(subject, new byte[] { 42 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
        try
        {
            await WaitForStageAsync(failedAckCompleted.Task, "failed ACK callback");
            await Assert.That(capture.Failure.Task.IsCompletedSuccessfully).IsTrue();
            await Assert.That(await capture.Failure.Task).IsTypeOf<NatsNoRespondersException>();
            await WaitForStageAsync(redelivered.Task, "confirmed native redelivery");
            await adapter.StopAsync(token);
            if (callbackFailure.Task.IsCompleted) await callbackFailure.Task;
            var consumer = await js.GetConsumerAsync(events, "worker", token);
            await Assert.That(consumer.Info.NumAckPending).IsEqualTo(0L);
            // At-least-once delivery permits more redeliveries when a runner is delayed past AckWait.
            await Assert.That(calls).IsGreaterThanOrEqualTo(2);
        }
        catch (Exception error)
        {
            // The test deadline may already be canceled: use a separate bounded diagnostic request.
            string consumerState;
            using var diagnostics = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                var info = (await js.GetConsumerAsync(events, "worker", diagnostics.Token)).Info;
                consumerState = $"ackPending={info.NumAckPending}, redelivered={info.NumRedelivered}, waiting={info.NumWaiting}, pending={info.NumPending}";
            }
            catch (Exception diagnosticError) { consumerState = diagnosticError.ToString(); }
            throw new InvalidOperationException(
                $"Failed ACK recovery: calls={Volatile.Read(ref calls)}, failedAck={failedAckCompleted.Task.Status}, " +
                $"ackError={capture.Failure.Task.Status}, redelivery={redelivered.Task.Status}, " +
                $"callback={callbackFailure.Task.Status}, worker={worker.Completion.Status}, " +
                $"adminConnection={native.ConnectionState}; {consumerState}\n{capture}", error);
        }

        async Task WaitForStageAsync(Task stage, string name)
        {
            var completed = await Task.WhenAny(stage, callbackFailure.Task, worker.Completion).WaitAsync(token);
            await completed;
            if (completed == worker.Completion)
                throw new InvalidOperationException($"Worker stopped before {name}.");
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_processing_leaves_native_redelivery_running(bool unsupportedSettlement)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("failure_events");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("failure_orders");
        var missing = TestContext.Current!.Isolation.GetIsolatedName("missing_dead_letter");
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.Nats("failure", transport =>
        {
            transport.Options.Configure(options =>
            {
                options.ConnectionOptions = broker.Options();
                options.Streams.Add(new(events) { Configuration = new(events, [subject]) });
                options.Consumers.Add(new(events, "worker") { Configuration = new("worker") { AckWait = TimeSpan.FromMilliseconds(200) } });
            });
            transport.Send<ExecutionMessage>(subject);
        }));
        await using var provider = services.BuildServiceProvider();
        var adapter = (NatsNativeTransport)provider.GetRequiredService<Relayer.Transports.ITransport>();
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var worker = adapter.AddConsumer(events, "worker", new() { MaxMsgs = 1, DrainOnCancel = true, Expires = TimeSpan.FromSeconds(1) }, 2);
        var redelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redeliveryReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialAttemptFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        worker.Receive = async (delivery, processing) =>
        {
            Interlocked.Increment(ref calls);
            if (delivery.Message.Metadata!.Value.NumDelivered == 1)
            {
                // Force the second worker to observe native redelivery before this attempt
                // finishes. Callback scheduling is not broker delivery order.
                await redeliveryReceived.Task.WaitAsync(processing);
                try
                {
                    if (unsupportedSettlement) return Disposition.Redeliver(TimeSpan.Zero);
                    // A real failure-copy publication to a subject outside every stream fails.
                    // Leave the original unacknowledged so native AckWait can redeliver it.
                    (await js.PublishAsync(missing, delivery.Message.Data,
                        NatsRawSerializer<ReadOnlyMemory<byte>>.Default, cancellationToken: processing)).EnsureSuccess();
                    throw new InvalidOperationException("Expected native publication to fail.");
                }
                finally { initialAttemptFinished.TrySetResult(); }
            }
            redeliveryReceived.TrySetResult();
            await initialAttemptFinished.Task.WaitAsync(processing);
            // Confirm settlement before signalling: stopping the pull loop must not race
            // the assertion, and native at-least-once delivery may include queued duplicates.
            await worker.SettleAsync(delivery, Disposition.Complete, processing);
            if (delivery.Message.Metadata!.Value.NumDelivered > 1) redelivered.TrySetResult();
            return Disposition.SettledByHandler;
        };
        await adapter.StartAsync(token);
        (await js.PublishAsync(subject, new byte[] { 42 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
        await Task.WhenAny(redelivered.Task, worker.Completion).WaitAsync(token);
        if (worker.Completion.IsCompleted) await worker.Completion;
        await redelivered.Task.WaitAsync(token);
        await adapter.StopAsync(token);
        var consumer = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(consumer.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(calls).IsGreaterThanOrEqualTo(2);
    }

    private sealed class AckFailureCapture : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<string> _entries = new();
        internal TaskCompletionSource<Exception> Failure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ILogger CreateLogger(string categoryName) => this;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Dispose() { }
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _entries.Enqueue($"{logLevel} {eventId.Name}: {formatter(state, exception)} {exception}");
            if (eventId.Name == "AckFailed" && exception is not null) Failure.TrySetResult(exception);
        }
        public override string ToString() => string.Join(Environment.NewLine, _entries);
    }

}

[Message("nats.execution")]
public sealed record ExecutionMessage(int Value);
public sealed class ExecutionControl
{
    public int Attempts;
    public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
public sealed class ExecutionHandler(ExecutionControl control) : IHandler<ExecutionMessage>
{
    public ValueTask HandleAsync(ExecutionMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var native = context.Nats();
        if (message.Value != 42 || native.Message.Headers!["custom"].ToString() != "native")
            throw new InvalidOperationException("Native payload/header mapping changed.");
        if (++control.Attempts == 1) throw new InvalidOperationException("Expected in-process retry.");
        control.Completed.TrySetResult();
        return default;
    }
}
[JsonSerializable(typeof(ExecutionMessage))]
internal partial class NatsExecutionJson : JsonSerializerContext;
