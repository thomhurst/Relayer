using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaReceiveTests
{
    [Test]
    public async Task Handled_observation_preserves_receiver_failure_and_releases_the_pending_read()
    {
        using var cancellation = new CancellationTokenSource();
        var control = new KafkaReceiveControl();
        var failure = new InvalidOperationException("receiver failed");
        try
        {
            var read = control.ReadHandledAsync(Task.FromException(failure), cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(await Assert.ThrowsAsync<InvalidOperationException>(() => read)).IsSameReferenceAs(failure);
            control.Handled.Writer.TryWrite(42);
            var next = control.Handled.Reader.ReadAsync(cancellation.Token);
            await Assert.That(next.IsCompleted).IsTrue();
            await Assert.That(await next).IsEqualTo(42);
        }
        finally { await cancellation.CancelAsync(); }
    }

    [Test]
    public async Task Handled_observation_preserves_receiver_cancellation()
    {
        using var cancellation = new CancellationTokenSource();
        using var receiver = new CancellationTokenSource();
        await receiver.CancelAsync();
        var control = new KafkaReceiveControl();
        try
        {
            var read = control.ReadHandledAsync(Task.FromCanceled(receiver.Token), cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(10));
            var error = await Assert.ThrowsAsync<OperationCanceledException>(() => read);
            await Assert.That(error!.CancellationToken).IsEqualTo(receiver.Token);
            await Assert.That(cancellation.IsCancellationRequested).IsFalse();
        }
        finally { await cancellation.CancelAsync(); }
    }

    [Test]
    public async Task Handled_observation_preserves_failure_during_a_pending_read()
    {
        var control = new KafkaReceiveControl();
        var receiver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("receiver failed during the read");
        var read = control.ReadHandledAsync(receiver.Task, CancellationToken.None);
        await Assert.That(read.IsCompleted).IsFalse();
        receiver.SetException(failure);
        // Give a broken observer a terminal channel too, so the regression cannot hang.
        control.Handled.Writer.TryComplete(new InvalidOperationException("channel closed after receiver failure"));
        await Assert.That(await Assert.ThrowsAsync<InvalidOperationException>(() => read)).IsSameReferenceAs(failure);
    }

    [Test]
    [Arguments(TaskStatus.Faulted)]
    [Arguments(TaskStatus.Canceled)]
    [Arguments(TaskStatus.RanToCompletion)]
    public async Task Handled_observation_observes_channel_fault_when_receiver_completes(TaskStatus receiverStatus)
    {
        var channelFailure = new InvalidOperationException("channel failed while receiver completed");
        var unobserved = new ConcurrentQueue<Exception>();
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            foreach (var error in args.Exception.InnerExceptions)
                if (error is ChannelClosedException && ReferenceEquals(error.InnerException, channelFailure))
                    unobserved.Enqueue(error);
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            await ExerciseFaultedHandledReadAsync(receiverStatus, channelFailure);
            // Finalize unreachable task exception holders while the scoped observer is registered.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Assert.That(unobserved.IsEmpty).IsTrue();
        }
        finally { TaskScheduler.UnobservedTaskException -= OnUnobserved; }
    }

    [Test]
    public async Task Handled_observation_rejects_receiver_completion_before_a_message()
    {
        using var cancellation = new CancellationTokenSource();
        var control = new KafkaReceiveControl();
        try
        {
            var read = control.ReadHandledAsync(Task.CompletedTask, cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(async () => await read).Throws<InvalidOperationException>()
                .WithMessageContaining("stopped before handling the expected message");
        }
        finally { await cancellation.CancelAsync(); }
    }

    [Test]
    public async Task Handled_observation_returns_messages_while_receiver_is_running()
    {
        var control = new KafkaReceiveControl();
        var receiver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Handled.Writer.TryWrite(42);
        await Assert.That(await control.ReadHandledAsync(receiver.Task, CancellationToken.None)).IsEqualTo(42);
        await Assert.That(receiver.Task.IsCompleted).IsFalse();
    }

    [Test]
    public async Task Handled_observation_cancels_its_pending_read_with_the_caller()
    {
        using var cancellation = new CancellationTokenSource();
        var control = new KafkaReceiveControl();
        var receiver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = control.ReadHandledAsync(receiver.Task, cancellation.Token);
        await cancellation.CancelAsync();
        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => read);
        await Assert.That(error!.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(receiver.Task.IsCompleted).IsFalse();
        control.Handled.Writer.TryWrite(42);
        var next = control.Handled.Reader.ReadAsync();
        await Assert.That(next.IsCompleted).IsTrue();
        await Assert.That(await next).IsEqualTo(42);
    }

    [Test]
    [Arguments(Dekaf.Protocol.ErrorCode.FencedMemberEpoch, true)]
    [Arguments(Dekaf.Protocol.ErrorCode.UnknownMemberId, true)]
    [Arguments(Dekaf.Protocol.ErrorCode.StaleMemberEpoch, true)]
    [Arguments(Dekaf.Protocol.ErrorCode.GroupAuthorizationFailed, false)]
    public async Task Rejected_commit_replays_only_membership_failures(Dekaf.Protocol.ErrorCode code, bool recover)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 0), token);
        var partition = new TopicPartition(fixture.Destination, 0);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        native.Assign(partition);
        var failure = new Dekaf.Errors.GroupException(code, "injected commit rejection");
        var probe = new NativeLifecycleProbe { CommitFailure = failure };
        var handled = 0;
        await using var adapter = new KafkaConsumer(NativeLifecycleProxy.Wrap(native, probe),
            (_, _) => { handled++; return ValueTask.FromResult(Disposition.Complete); }, tracing,
            new() { System = "kafka", Destination = fixture.Destination, ConsumerGroup = fixture.Destination }, new());
        await native.InitializeAsync(token);
        await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token);
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        if (!recover)
        {
            var error = await Assert.ThrowsAsync<Dekaf.Errors.GroupException>(async () =>
                await adapter.ProcessBatchAsync(batches.Current, token));
            await Assert.That(ReferenceEquals(error, failure)).IsTrue();
            return;
        }
        await Assert.That(await adapter.ProcessBatchAsync(batches.Current, token)).IsEqualTo(0);
        await Assert.That(native.GetPosition(partition)).IsEqualTo(0L);
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        await Assert.That(await adapter.ProcessBatchAsync(batches.Current, token)).IsEqualTo(1);
        await Assert.That(handled).IsEqualTo(2);
        await Assert.That(await native.GetCommittedOffsetAsync(partition, token)).IsEqualTo(1L);
    }

    [Test]
    [Arguments(1, false)]
    [Arguments(3, false)]
    [Arguments(1, true)]
    [Arguments(3, true)]
    public async Task Retained_partition_completes_despite_assignment_notifications(int invalidateAfter, bool circuitEnabled)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        for (var i = 0; i < 3; i++) await fixture.Transport.SendAsync(Message(fixture.Destination, i), token);
        var partition = new TopicPartition(fixture.Destination, 0);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        native.Assign(partition);
        var ownership = new KafkaOwnership();
        var handled = new List<long>();
        ConsumerCircuitDelivery? circuit = null;
        await using var adapter = new KafkaConsumer(native, Receive, tracing,
            new() { System = "kafka", Destination = fixture.Destination, ConsumerGroup = fixture.Destination }, ownership);
        using var instruments = new RelayerInstrumentation();
        circuit = circuitEnabled ? adapter.ConfigureCircuit(new() { FailureRatio = .01, MinimumThroughput = 1 },
            instruments.CreateEndpoint(new() { System = "kafka", Destination = fixture.Destination }), TimeProvider.System) : null;
        await native.InitializeAsync(token);
        await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token);
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        await Assert.That(await adapter.ProcessBatchAsync(batches.Current, token)).IsEqualTo(3);
        await Assert.That(handled.ToArray()).IsEquivalentTo(new long[] { 0, 1, 2 });
        await Assert.That(native.GetPosition(partition)).IsEqualTo(3L);
        await Assert.That(await native.GetCommittedOffsetAsync(partition, token)).IsEqualTo(3L);
        if (circuit is not null)
        {
            circuit.TryEnter();
            await Assert.That(circuit.Complete(true)).IsTrue();
            // Exercise the native assignment listener while open, including a retained partition.
            await ownership.OnPartitionsAssignedAsync([partition], token);
            await Assert.That(native.Paused.Contains(partition)).IsTrue();
            await Assert.That(await native.GetCommittedOffsetAsync(partition, token)).IsEqualTo(3L);
        }


        async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellationToken)
        {
            handled.Add(delivery.Result.Offset);
            circuit?.Complete(false);
            if (handled.Count == invalidateAfter && ownership.Version == 0)
                await ownership.OnPartitionsAssignedAsync([partition], cancellationToken);
            return Disposition.Complete;
        }
    }

    [Test]
    public async Task Partial_start_failure_disposes_consumers_already_started()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        fixture.RegisterConsumer(static (_, _) => new(Disposition.Complete));
        fixture.RegisterConsumer(static (_, _) => new(Disposition.Complete));
        var transport = (KafkaProducerTransport)fixture.Transport;
        // Simulate an unrestartable second consumer without relying on broker timing.
        await transport.Consumers[1].StopAsync(new CancellationToken(canceled: true));
        await Assert.That(async () => await transport.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("failed or aborted");
        await Assert.That(transport.Consumers[0].Completion.IsCompleted).IsTrue();
        await Assert.That(async () => await transport.StartAsync(token)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Core_retry_durable_dead_letter_and_native_commits_preserve_creation_context()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl();
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is "Relayer" or "Dekaf",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        using var host = BuildHost(fixture, control);
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        await fixture.Transport.StartAsync(token);
        var source = fixture.Destination;
        await fixture.Transport.SendAsync(Message(source, 1), token);
        await control.Entered.Task.WaitAsync(token);
        await using var offsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(source).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await offsets.InitializeAsync(token);
        var before = await offsets.GetCommittedOffsetAsync(new(source, 0), token);
        await Assert.That(before is null or < 0).IsTrue();
        control.Release.SetResult();
        await Assert.That(await control.ReadHandledAsync(transport.Consumers[0].Completion, token)).IsEqualTo(1);
        var poison = Message(source, 2);
        await fixture.Transport.SendAsync(new(source, poison.Body, poison.Headers, poison.OrderingKey,
            options: new SendOptions().ConfigureNative<Dekaf.Serialization.Headers>(static headers =>
                headers.Add(new Dekaf.Serialization.Header("nullable", (byte[]?)null)).Add("nullable", "second"))), token);
        var tombstone = Message(source, 4);
        await fixture.Transport.SendAsync(new(source, default, tombstone.Headers, tombstone.OrderingKey,
            isTombstone: true), token);
        // Bypass Relayer's send marker handling to simulate an untrusted source header.
        await using var rawProducer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await rawProducer.InitializeAsync(token);
        await rawProducer.ProduceAsync(new()
        {
            Topic = source, Key = "customer-1"u8.ToArray(), Value = [],
            Headers = new Dekaf.Serialization.Headers().Add("ce_id", "empty-with-marker")
                .Add("ce_type", "tests.kafka.receive;v=1").Add("traceparent", Creation)
                .Add(KafkaSendState.TombstoneHeader, "true")
                .Add(new Dekaf.Serialization.Header(KafkaSendState.TombstoneHeader, (byte[]?)null)),
        }, token);
        await fixture.Transport.SendAsync(Message(source, 3), token);
        await Assert.That(await control.ReadHandledAsync(transport.Consumers[0].Completion, token)).IsEqualTo(3);
        await host.StopAsync(token);
        var service = host.Services.GetServices<IHostedService>().OfType<KafkaHostedService>().Single();
        await Assert.That(async () => await service.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("only start once");
        await Assert.That(transport.Consumers.Count).IsEqualTo(1);
        await Assert.That(control.FirstAttempts).IsEqualTo(2);
        await Assert.That(control.LastFeature.ConsumeResult.Topic).IsNull();
        await Assert.That(await offsets.GetCommittedOffsetAsync(new(source, 0), token)).IsEqualTo(5L);

        await using var deadReader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
        deadReader.Assign(new TopicPartition(source + ".dead-letter", 0));
        await deadReader.InitializeAsync(token);
        var dead = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(Encoding.UTF8.GetString(dead.Value)).IsEqualTo("{\"Number\":2}");
        await Assert.That(Encoding.UTF8.GetString(dead.Key!)).IsEqualTo("customer-1");
        await Assert.That(dead.Headers.First(header => header.Key == "traceparent").GetValueAsString()).IsEqualTo(Creation);
        await Assert.That(dead.Headers.First(header => header.Key == "tracestate").GetValueAsString()).IsEqualTo("tenant=original");
        await Assert.That(dead.Headers.First(header => header.Key == FailureHeaders.History).GetValueAsString()).Contains("poison record");
        var nullable = dead.Headers.Where(header => header.Key == "nullable").ToArray();
        await Assert.That(nullable.Length).IsEqualTo(2);
        await Assert.That(nullable[0].IsValueNull).IsTrue();
        await Assert.That(nullable[1].GetValueAsString()).IsEqualTo("second");
        var deadTombstone = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(deadTombstone.Value).IsNull();
        await Assert.That(deadTombstone.Headers.First(header => header.Key == "traceparent").GetValueAsString()).IsEqualTo(Creation);
        await Assert.That(deadTombstone.Headers.First(header => header.Key == "tracestate").GetValueAsString()).IsEqualTo("tenant=original");
        var deadEmpty = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(deadEmpty.Value).IsNotNull();
        await Assert.That(deadEmpty.Value.Length).IsEqualTo(0);
        await Assert.That(deadEmpty.Headers.Any(header => header.Key == KafkaSendState.TombstoneHeader)).IsFalse();
        var processes = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "process " + source).ToArray();
        await Assert.That(processes.Length).IsEqualTo(5);
        await Assert.That(processes.Count(span => Equals(span.GetTagItem("messaging.kafka.message.tombstone"), true))).IsEqualTo(1);
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.consumer.group.name"), source))).IsTrue();
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.destination.partition.id"), "0"))).IsTrue();
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.kafka.message.key"), "customer-1"))).IsTrue();
        await Assert.That(processes.All(span => span.GetTagItem("messaging.kafka.offset") is long)).IsTrue();
        var commits = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "commit " + source).ToArray();
        await Assert.That(commits.Length).IsGreaterThan(0);
        await Assert.That(commits.All(span => span.GetTagItem("messaging.batch.message_count") is int &&
            processes.All(process => process.SpanId != span.ParentSpanId))).IsTrue();
        var copySends = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "send " + source + ".dead-letter").ToArray();
        await Assert.That(copySends.All(copy => spans.Any(span => span.Source.Name == "Dekaf" && span.ParentSpanId == copy.SpanId))).IsTrue();
    }

    [Test]
    public async Task Failed_dead_letter_copy_stops_receiver_without_committing_original()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl();
        using var host = BuildHost(fixture, control, missingDeadLetter: true);
        await host.StartAsync(token);
        await fixture.Transport.StartAsync(token);
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        var read = control.ReadHandledAsync(transport.Consumers[0].Completion, token);
        await Assert.That(read.IsCompleted).IsFalse();
        await fixture.Transport.SendAsync(Message(fixture.Destination, 2), token);
        await Assert.That(async () => await read).Throws<OperationCanceledException>();
        await Assert.That(timeout.IsCancellationRequested).IsFalse();
        await using var offsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await offsets.InitializeAsync(token);
        var committed = await offsets.GetCommittedOffsetAsync(new(fixture.Destination, 0), token);
        await Assert.That(committed is null or < 0).IsTrue();
    }

    [Test]
    public async Task Failed_consumer_reaches_host_while_sibling_is_running_and_shutdown_stops_both()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl();
        using var host = BuildHost(fixture, control, missingDeadLetter: true, healthySibling: true);
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 3), token);
        await Assert.That(await control.ReadHandledAsync(transport.Consumers[0].Completion, token)).IsEqualTo(3);
        await Assert.That(await control.ReadHandledAsync(transport.Consumers[0].Completion, token)).IsEqualTo(3);
        var service = host.Services.GetServices<IHostedService>().OfType<KafkaHostedService>().Single();
        await fixture.Transport.SendAsync(Message(fixture.Destination, 2), token);
        await Assert.That(async () => await transport.Consumers[0].Completion.WaitAsync(token)).Throws<OperationCanceledException>();
        await Assert.That(transport.Consumers[1].Completion.IsCompleted).IsFalse();
        await Assert.That(async () => await service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10), token))
            .Throws<OperationCanceledException>();
        await Assert.That(async () => await host.StopAsync(token)).Throws<OperationCanceledException>();
        await Assert.That(transport.Consumers[0].Completion.IsCompleted).IsTrue();
        await Assert.That(transport.Consumers[1].Completion.IsCompletedSuccessfully).IsTrue();
        await Assert.That(timeout.IsCancellationRequested).IsFalse();
    }

    // Keep completed read tasks out of the collecting test's stack, including in Release builds.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task ExerciseFaultedHandledReadAsync(TaskStatus receiverStatus, Exception channelFailure)
    {
        var control = new KafkaReceiveControl();
        control.Handled.Writer.TryComplete(channelFailure);
        using var receiverCancellation = new CancellationTokenSource();
        receiverCancellation.Cancel();
        var receiverFailure = new InvalidOperationException("receiver failed");
        var receiver = receiverStatus switch
        {
            TaskStatus.Faulted => Task.FromException(receiverFailure),
            TaskStatus.Canceled => Task.FromCanceled(receiverCancellation.Token),
            _ => Task.CompletedTask,
        };
        var read = control.ReadHandledAsync(receiver, CancellationToken.None);
        if (receiverStatus == TaskStatus.Canceled)
        {
            var error = await Assert.ThrowsAsync<OperationCanceledException>(() => read);
            await Assert.That(error!.CancellationToken).IsEqualTo(receiverCancellation.Token);
        }
        else
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => read);
            if (receiverStatus == TaskStatus.Faulted)
                await Assert.That(error).IsSameReferenceAs(receiverFailure);
            else
                await Assert.That(error!.Message).Contains("stopped before handling the expected message");
        }
    }

    [Test]
    [Arguments(false, false, false)]
    [Arguments(true, false, false)]
    [Arguments(false, true, false)]
    [Arguments(false, false, true)]
    public async Task Circuit_retains_failed_offset_and_preserves_permanent_dead_letter_handling(bool stopOpen, bool retry, bool permanentPoison)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl { CircuitFailure = true };
        var clock = new CircuitClock();
        var states = Channel.CreateUnbounded<int>();
        using var meter = new System.Diagnostics.Metrics.MeterListener();
        meter.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Relayer" && instrument.Name == "relayer.circuit.state") listener.EnableMeasurementEvents(instrument);
        };
        meter.SetMeasurementEventCallback<int>((_, state, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, fixture.Destination)) states.Writer.TryWrite(state);
        });
        meter.Start();
        using var host = BuildHost(fixture, control, circuitClock: clock, circuitRetry: retry);
        await host.StartAsync(token);
        var receiver = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers[0];
        await fixture.Transport.StartAsync(token);
        var retainedOffset = permanentPoison ? 1L : 0L;
        if (permanentPoison) await fixture.Transport.SendAsync(Message(fixture.Destination, 6), token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 5), token);
        await State(1);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 3), token);
        await using var offsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
        await offsets.InitializeAsync(token);
        var committedBeforeProbe = await offsets.GetCommittedOffsetAsync(new(fixture.Destination, 0), token);
        await Assert.That(committedBeforeProbe is null || committedBeforeProbe <= retainedOffset).IsTrue();
        // A replay control request drains the borrowed batch while preserving the open circuit.
        await host.Services.GetRequiredKeyedService<IKafkaConsumerControl>("events")
            .SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, retainedOffset), token);
        await Assert.That(control.CircuitAttempts).IsEqualTo(retry ? 2 : 1);
        if (stopOpen)
        {
            await host.StopAsync(token);
            clock.Recover(); // A late timer cannot restart a stopped native receiver.
            await Assert.That(receiver.Completion.IsCompletedSuccessfully).IsTrue();
            await Assert.That(control.CircuitAttempts).IsEqualTo(retry ? 2 : 1);
            await Assert.That(await offsets.GetCommittedOffsetAsync(new(fixture.Destination, 0), token) is null or < 0).IsTrue();
            return;
        }
        clock.Recover();
        await State(2);
        await State(1);
        await Assert.That(control.CircuitAttempts).IsEqualTo(retry ? 4 : 2);
        control.CircuitFailure = false;
        clock.Recover();
        await Assert.That(await control.ReadHandledAsync(receiver.Completion, token)).IsEqualTo(5);
        await Assert.That(await control.ReadHandledAsync(receiver.Completion, token)).IsEqualTo(3);
        await host.StopAsync(token);
        // GetCommittedOffsetAsync caches a successful lookup in this native consumer.
        // Use a fresh observer so the pre-probe lookup cannot hide the final broker commit.
        await using var finalOffsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
        await finalOffsets.InitializeAsync(token);
        await Assert.That(await finalOffsets.GetCommittedOffsetAsync(new(fixture.Destination, 0), token)).IsEqualTo(2L + retainedOffset);
        await using var dead = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await dead.InitializeAsync(token);
        var ends = await dead.QueryWatermarkOffsetsAsync(new(fixture.Destination + ".dead-letter", 0), token);
        await Assert.That(ends.High).IsEqualTo(retainedOffset);

        async Task State(int expected)
        {
            while (await states.Reader.ReadAsync(token) != expected) { }
        }
    }

    internal sealed class CircuitClock : TimeProvider
    {
        private ManualTimer? _recovery;
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => new ManualTimer(this, callback, state);
        internal void Recover() => (_recovery ?? throw new InvalidOperationException("No recovery is scheduled.")).Fire();
        private sealed class ManualTimer(CircuitClock clock, TimerCallback callback, object? state) : ITimer
        {
            private bool _disposed;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                if (_disposed) return false;
                if (dueTime != Timeout.InfiniteTimeSpan && period == Timeout.InfiniteTimeSpan) clock._recovery = this;
                return true;
            }
            public void Dispose() => _disposed = true;
            public ValueTask DisposeAsync() { Dispose(); return default; }
            internal void Fire() { if (!_disposed) callback(state); }
        }
    }

    private const string Creation = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    private static OutgoingMessage Message(string topic, int number) => new(topic, Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
        new KeyValuePair<string, WireString>[] { new("ce_id", new("record-" + number)), new("ce_type", new("tests.kafka.receive;v=1")),
            new("traceparent", new(Creation)), new("tracestate", new("tenant=original")) }, new WireString("customer-1"));

    private static IHost BuildHost(KafkaFixture fixture, KafkaReceiveControl control, bool missingDeadLetter = false,
        bool healthySibling = false, CircuitClock? circuitClock = null, bool circuitRetry = false)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        });
        builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddSingleton(control);
        if (circuitClock is not null) builder.Services.AddSingleton<TimeProvider>(circuitClock);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaReceiveJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(fixture.Bootstrap);
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest)
                        .WithOffsetCommitMode(OffsetCommitMode.Auto).WithAutoOffsetStore(); // Relayer must override both.
                    options.ConfigureProducer = native => native.WithAllowAutoCreateTopics(false);
                });
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination;
                    if (circuitClock is not null) consumer.Options.CircuitBreaker = new() { MinimumThroughput = 1 };
                    consumer.Failures(new()
                    {
                        RetryDelays = circuitClock is null || circuitRetry ? new[] { TimeSpan.Zero } : [], Jitter = 0,
                        DeadLetterEndpoint = missingDeadLetter ? fixture.Destination + ".missing" : null,
                        // Only missing-topic scenarios need a short failure-injection deadline.
                        CopyTimeout = missingDeadLetter ? TimeSpan.FromSeconds(1) : new FailureOptions().CopyTimeout,
                    });
                    consumer.Handler<KafkaReceiveHandler>();
                });
                if (healthySibling)
                    kafka.Consume(fixture.Destination, consumer =>
                    {
                        consumer.Options.Group = fixture.Destination + ".sibling";
                        consumer.Handler<KafkaReceiveHandler>();
                    });
            });
        });
        return builder.Build();
    }
}

[Message("tests.kafka.receive")]
public sealed record KafkaReceiveMessage(int Number);
public sealed class KafkaReceiveControl
{
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal Channel<int> Handled { get; } = Channel.CreateUnbounded<int>();
    internal int FirstAttempts;
    internal volatile bool CircuitFailure;
    internal int CircuitAttempts;
    internal KafkaConsumeFeature LastFeature;

    internal async Task<int> ReadHandledAsync(Task receiverCompletion, CancellationToken cancellationToken)
    {
        using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var read = Handled.Reader.ReadAsync(readCancellation.Token).AsTask();
        try
        {
            await Task.WhenAny(read, receiverCompletion);
            if (receiverCompletion.IsCompleted)
            {
                await receiverCompletion;
                throw new InvalidOperationException("Kafka receiver stopped before handling the expected message.");
            }
            cancellationToken.ThrowIfCancellationRequested();
            return await read;
        }
        finally
        {
            readCancellation.Cancel();
            try { await read; }
            catch (Exception error) when (error is OperationCanceledException or ChannelClosedException)
            {
                // Observe the losing read without replacing the receiver's exception or cancellation.
            }
        }
    }
}
public sealed class KafkaReceiveHandler(KafkaReceiveControl control) : IHandler<KafkaReceiveMessage>
{
    public async ValueTask HandleAsync(KafkaReceiveMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        control.LastFeature = context.Kafka();
        if (message.Number == 1 && Interlocked.Increment(ref control.FirstAttempts) == 1)
        {
            control.Entered.SetResult();
            await control.Release.Task.WaitAsync(cancellationToken);
            throw new InvalidOperationException("retry me");
        }
        if (message.Number == 6) throw new MessageEnvelopeException("permanent poison");
        if (message.Number == 5)
        {
            Interlocked.Increment(ref control.CircuitAttempts);
            if (control.CircuitFailure) throw new InvalidOperationException("dependency unavailable");
        }
        if (message.Number == 2) throw new InvalidOperationException("poison record");
        control.Handled.Writer.TryWrite(message.Number);
    }
}
[JsonSerializable(typeof(KafkaReceiveMessage))]
internal partial class KafkaReceiveJson : JsonSerializerContext;
