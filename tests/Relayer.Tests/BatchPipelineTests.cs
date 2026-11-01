using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public class BatchPipelineTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Batch_composition_rejects_single_message_callbacks(bool telemetry)
    {
        await using var provider = Provider();
        using var instrumentation = new RelayerInstrumentation();
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error));
        if (telemetry) builder.Telemetry(instrumentation.CreateEndpoint(new() { System = "test" }));
        else builder.Settle(static _ => default);

        await Assert.That(() => builder.BuildBatch(
            provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(), Definition(provider)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Full_partial_and_thrown_failure_settle_only_after_scope_and_failure_work(int failureMode)
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var handlerDone = new TaskCompletionSource<BatchResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposalDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Disposal = disposalDone.Task;
        control.DisposalStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Invoke = (_, _) => new(handlerDone.Task);
        var failedIds = new List<string>();
        var failureDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failureStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline(provider, async (context, _, _) =>
        {
            failedIds.Add(context.MessageId);
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.DeadLettered(new("failed"));
            failureStarted.TrySetResult();
            await failureDone.Task;
        });
        var output = Sentinels(3);
        var pending = pipeline.ProcessBatchAsync(Input(3), output);
        await Assert.That(pending.IsCompleted).IsFalse();
        if (failureMode == 2) handlerDone.SetException(new InvalidOperationException("total failure"));
        else handlerDone.SetResult(failureMode == 0 ? BatchResult.Success : BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = new InvalidOperationException("partial") }));
        await control.DisposalStarted.Task;
        await Assert.That(output.All(d => d.Kind == DispositionKind.SettledByHandler)).IsTrue();
        await Assert.That(failedIds.Count).IsEqualTo(0);
        disposalDone.SetResult();
        if (failureMode > 0)
        {
            await failureStarted.Task;
            await Assert.That(pending.IsCompleted).IsFalse();
            await Assert.That(output.All(d => d.Kind == DispositionKind.SettledByHandler)).IsTrue();
        }
        failureDone.SetResult();
        await pending;
        await Assert.That(string.Join(",", failedIds)).IsEqualTo(failureMode switch { 0 => "", 1 => "1", _ => "0,1,2" });
        for (var i = 0; i < 3; i++)
            await Assert.That(output[i].Kind).IsEqualTo(failureMode == 2 || failureMode == 1 && i == 1 ? DispositionKind.DeadLetter : DispositionKind.Complete);
        await Assert.That(control.Scopes.Count).IsEqualTo(1);
        await Assert.That(control.Disposed).IsEqualTo(1);
    }

    [Test]
    public async Task Retry_invokes_only_failed_items_in_fresh_scope_with_native_durable_dispositions()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var batches = new List<string>();
        control.Invoke = (batch, _) =>
        {
            batches.Add(string.Join(",", batch.Items.Select(item => item.Context.MessageId + ":" + item.Context.Attempt)));
            return new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [batch.Count - 1] = new InvalidOperationException("failed") }));
        };
        var failures = new FailurePipeline(provider, "test", "source", "batch", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var pipeline = Pipeline(provider, failures: failures);
        var output = new Disposition[3];
        await pipeline.ProcessBatchAsync(Input(3), output);
        await Assert.That(string.Join(";", batches)).IsEqualTo("0:1,1:1,2:1;2:2");
        await Assert.That(output[0].Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(output[1].Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(output[2].Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(control.Scopes.Count).IsEqualTo(2);
        await Assert.That(control.Scopes[0]).IsNotSameReferenceAs(control.Scopes[1]);
        await Assert.That(control.Disposed).IsEqualTo(2);
        await Assert.That(provider.GetRequiredService<Transport>().Reads).IsEqualTo(2); // source and route, cached across retry
    }

    [Test]
    public async Task Poison_items_are_excluded_and_partial_indexes_map_to_original_deliveries()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        control.Invoke = (batch, _) =>
        {
            if (string.Join(",", batch.Items.Select(item => item.Context.MessageId)) != "0,3") throw new Exception("Wrong batch");
            return new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = new ApplicationException("handler") }));
        };
        var errors = new List<(string Id, Exception Error, FailureAction Action)>();
        var input = Input(4);
        input[1] = input[1] with { Type = "unknown;v=1" };
        input[2] = input[2] with { Value = 0 };
        var output = new Disposition[4];
        await Pipeline(provider, (context, error, action) => { errors.Add((context.MessageId, error, action)); return default; }).ProcessBatchAsync(input, output);
        await Assert.That(string.Join(",", errors.Select(e => e.Id))).IsEqualTo("1,2,3");
        await Assert.That(errors[0].Error is UnknownMessageTypeException).IsTrue();
        await Assert.That(errors[1].Error is MessageDeserializationException).IsTrue();
        await Assert.That(errors[0].Action).IsEqualTo(FailureAction.DeadLetter);
        await Assert.That(errors[1].Action).IsEqualTo(FailureAction.DeadLetter);
        await Assert.That(errors[2].Action).IsEqualTo(FailureAction.Retry);
    }

    [Test]
    public async Task Invalid_results_fail_every_item_without_dropping_invalid_indexes()
    {
        await using var provider = Provider();
        provider.GetRequiredService<Control>().Invoke = (_, _) => new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [3] = new Exception() }));
        var failures = 0;
        await Pipeline(provider, (_, _, _) => { failures++; return default; }).ProcessBatchAsync(Input(3), new Disposition[3]);
        await Assert.That(failures).IsEqualTo(3);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Disposal_or_failure_executor_errors_leave_output_untouched(bool failDisposal)
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        if (failDisposal) control.Disposal = Task.FromException(new ApplicationException("dispose"));
        else control.Invoke = (_, _) => ValueTask.FromException<BatchResult>(new Exception("handler"));
        var output = Sentinels(3);
        await Assert.That(async () => await Pipeline(provider, (_, _, _) => ValueTask.FromException(new ApplicationException("durability")))
            .ProcessBatchAsync(Input(3), output)).Throws<ApplicationException>();
        await Assert.That(output.All(d => d.Kind == DispositionKind.SettledByHandler)).IsTrue();
    }

    [Test]
    public async Task Cancellation_keeps_output_untouched_and_releases_all_native_references()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var retained = new List<NativeMessage<Incoming>>();
        using var cancellation = new CancellationTokenSource();
        control.Invoke = (batch, _) =>
        {
            retained.AddRange(batch.Items.Select(item => item.Context.Features.GetRequired<NativeMessage<Incoming>>()));
            cancellation.Cancel();
            return ValueTask.FromCanceled<BatchResult>(cancellation.Token);
        };
        var output = Sentinels(3);
        var pipeline = Pipeline(provider);
        await Assert.That(async () => await pipeline.ProcessBatchAsync(Input(3), output, cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(output.All(d => d.Kind == DispositionKind.SettledByHandler)).IsTrue();
        await Assert.That(retained.All(item => item.Message is null)).IsTrue();
        control.Invoke = null;
        await pipeline.ProcessBatchAsync(Input(3), output);
        await Assert.That(output.All(d => d.Kind == DispositionKind.Complete)).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Delivery_cancellation_during_handler_or_disposal_discards_sends_and_releases_contexts(bool duringDisposal)
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var transport = provider.GetRequiredService<Transport>();
        using var cancellation = new CancellationTokenSource();
        var disposalDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (duringDisposal)
        {
            control.Disposal = disposalDone.Task;
            control.DisposalStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        control.Invoke = async (batch, _) =>
        {
            await batch.Items[0].Context.Services.GetRequiredService<ISender>().SendNowAsync(new Payload());
            if (!duringDisposal) cancellation.Cancel();
            return BatchResult.Success;
        };
        var failures = 0;
        var pipeline = Consumer(provider, (_, _, _) => { failures++; return default; });
        var message = new ConsumeMessage { MessageId = "cancelled", MessageType = new("batch.payload", 1), Body = new(new byte[] { 1 }) };
        {
            using var first = pipeline.RentContext(message);
            using var second = pipeline.RentContext(message, cancellation.Token);
            var contexts = new[] { first, second };
            var pending = pipeline.ProcessBatchAsync(contexts);
            if (duringDisposal)
            {
                await control.DisposalStarted!.Task;
                cancellation.Cancel();
                disposalDone.SetResult();
            }
            await Assert.That(async () => await pending).Throws<OperationCanceledException>();
            await Assert.That(transport.Sends).IsEqualTo(0);
            await Assert.That(failures).IsEqualTo(0);
            await Assert.That(control.Disposed).IsEqualTo(1);
        }
        control.Invoke = null;
        using var reused = pipeline.RentContext(message);
        await pipeline.ProcessBatchAsync(new[] { reused });
        await Assert.That(transport.Sends).IsEqualTo(0);
    }

    [Test]
    [Arguments(0, false)]
    [Arguments(1, false)]
    [Arguments(2, false)]
    [Arguments(0, true)]
    [Arguments(1, true)]
    [Arguments(2, true)]
    public async Task Cancellation_during_flush_reaches_transport_and_prevents_batch_success(int tokenIndex, bool lastSend)
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var transport = provider.GetRequiredService<Transport>();
        using var cancellation = new CancellationTokenSource();
        using var otherDelivery = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Invoke = async (batch, _) =>
        {
            var sender = batch.Items[0].Context.Services.GetRequiredService<ISender>();
            for (var i = 0; i < 3; i++) await sender.SendNowAsync(new Payload());
            return BatchResult.Success;
        };
        transport.Send = async (_, token) =>
        {
            if (transport.Sends != (lastSend ? 3 : 1)) return;
            using var registration = token.Register(() => observed.TrySetResult());
            started.TrySetResult();
            // Deliberately return success after cancellation, as a native client may do.
            await release.Task;
        };
        var failures = 0;
        var pipeline = Consumer(provider, (_, _, _) => { failures++; return default; });
        var message = new ConsumeMessage { MessageId = "flush", MessageType = new("batch.payload", 1), Body = new(new byte[] { 1 }) };
        {
            using var first = pipeline.RentContext(message, tokenIndex == 0 ? cancellation.Token : otherDelivery.Token);
            using var second = pipeline.RentContext(message, tokenIndex == 1 ? cancellation.Token : otherDelivery.Token);
            var pending = pipeline.ProcessBatchAsync(new[] { first, second }, tokenIndex == 2 ? cancellation.Token : default);
            await started.Task;
            cancellation.Cancel();
            var reachedTransport = observed.Task.IsCompleted;
            release.SetResult();
            await Assert.That(async () => await pending).Throws<OperationCanceledException>();
            await Assert.That(reachedTransport).IsTrue();
            await Assert.That(transport.Sends).IsEqualTo(lastSend ? 3 : 1);
            await Assert.That(failures).IsEqualTo(0);
            await Assert.That(control.Disposed).IsEqualTo(1);
        }
        // A cancelled flush must release registrations and reset pooled state for a later flush.
        using var reused = pipeline.RentContext(message, otherDelivery.Token);
        await pipeline.ProcessBatchAsync(new[] { reused });
        await Assert.That(transport.Sends).IsEqualTo((lastSend ? 3 : 1) + 3);
        otherDelivery.Cancel();
    }

    [Test]
    public async Task Delivery_cancelled_between_attempts_is_not_dispatched_again()
    {
        await using var provider = Provider();
        using var cancellation = new CancellationTokenSource();
        var control = provider.GetRequiredService<Control>();
        control.Invoke = (_, _) => new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = new Exception("retry") }));
        var failures = new FailurePipeline(provider, "test", "source", "batch", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
            failures, new CancelOnClassify(cancellation))
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(), Definition(provider));
        var message = new ConsumeMessage { MessageId = "retry", MessageType = new("batch.payload", 1), Body = new(new byte[] { 1 }) };
        using var first = pipeline.RentContext(message);
        using var second = pipeline.RentContext(message, cancellation.Token);
        await Assert.That(async () => await pipeline.ProcessBatchAsync(new[] { first, second })).Throws<OperationCanceledException>();
        await Assert.That(control.Scopes.Count).IsEqualTo(1);
        await Assert.That(control.Disposed).IsEqualTo(1);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    public async Task Each_batch_owns_one_scope_and_concurrent_batches_do_not_share_storage()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var release = new TaskCompletionSource<BatchResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var batches = new List<Batch<Payload>>();
        control.Invoke = (batch, _) => { batches.Add(batch); return new(release.Task); };
        var pipeline = Pipeline(provider);
        var first = pipeline.ProcessBatchAsync(Input(3), new Disposition[3]);
        var secondInput = Input(2);
        secondInput[0] = secondInput[0] with { Id = "second" };
        var second = pipeline.ProcessBatchAsync(secondInput, new Disposition[2]);
        await Assert.That(batches[0]).IsNotSameReferenceAs(batches[1]);
        await Assert.That(batches[0].Count).IsEqualTo(3);
        await Assert.That(batches[1].Items[0].Context.MessageId).IsEqualTo("second");
        release.SetResult(BatchResult.Success);
        await first;
        await second;
        await Assert.That(control.Disposed).IsEqualTo(2);
        await Assert.That(batches.All(batch => batch.Count == 0)).IsTrue();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Batch_span_links_every_creation_context_and_uses_only_ambient_parent(bool ambient, bool utf8)
    {
        await using var provider = Provider();
        using var source = new ActivitySource("Relayer");
        Activity? observed = null;
        var sampledCount = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => ReferenceEquals(candidate, source),
            Sample = (ref ActivityCreationOptions<ActivityContext> options) =>
            {
                foreach (var tag in options.Tags!)
                    if (tag.Key == "messaging.batch.message_count") sampledCount = (int)tag.Value!;
                return ActivitySamplingResult.AllDataAndRecorded;
            },
            ActivityStopped = activity => observed = activity
        };
        ActivitySource.AddActivityListener(listener);
        using var parent = ambient ? new Activity("ambient").Start() : null;
        var expectedActivity = Activity.Current;
        var expectedParent = expectedActivity?.Context ?? default;
        var input = Input(3);
        if (utf8)
            for (var i = 0; i < input.Length; i++)
                input[i] = input[i] with { TraceParent = new("00-11111111111111111111111111111111-2222222222222222-01"u8.ToArray()) };
        var pipeline = Pipeline(provider, source: source);
        await pipeline.ProcessBatchAsync(input, new Disposition[3]);
        await Assert.That(observed!.DisplayName).IsEqualTo("process source");
        await Assert.That(observed.Kind).IsEqualTo(ActivityKind.Consumer);
        await Assert.That(observed.ParentSpanId).IsEqualTo(expectedParent.SpanId);
        await Assert.That((int)observed.GetTagItem("messaging.batch.message_count")!).IsEqualTo(3);
        await Assert.That(observed.GetTagItem("messaging.operation.type") as string).IsEqualTo("process");
        await Assert.That(observed.Links.Count()).IsEqualTo(3);
        await Assert.That(observed.Links.All(link => link.Context.IsRemote)).IsTrue();
        await Assert.That(observed.Links.All(link => link.Context.TraceId.ToString() == "11111111111111111111111111111111" &&
            link.Context.SpanId.ToString() == "2222222222222222" && link.Context.TraceFlags == ActivityTraceFlags.Recorded)).IsTrue();
        await Assert.That(sampledCount).IsEqualTo(3);
        await Assert.That(Activity.Current).IsSameReferenceAs(expectedActivity);
        await pipeline.ProcessBatchAsync(input.AsMemory(0, 1), new Disposition[1]);
        await Assert.That(sampledCount).IsEqualTo(1);
        await Assert.That((int)observed!.GetTagItem("messaging.batch.message_count")!).IsEqualTo(1);
        await Assert.That(observed.Links.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Batch_size_output_and_empty_boundaries_are_enforced()
    {
        await using var provider = Provider(size: 3);
        var pipeline = Pipeline(provider);
        await pipeline.ProcessBatchAsync(Input(3), new Disposition[3]);
        await pipeline.ProcessBatchAsync(Array.Empty<Incoming>(), Memory<Disposition>.Empty);
        await Assert.That(async () => await pipeline.ProcessBatchAsync(Input(4), new Disposition[4])).Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await pipeline.ProcessBatchAsync(Input(3), new Disposition[2])).Throws<ArgumentException>();
        await Assert.That(provider.GetRequiredService<Control>().Scopes.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Buffered_sends_flush_only_after_whole_batch_success_and_disposal()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var transport = provider.GetRequiredService<Transport>();
        control.Invoke = async (batch, _) =>
        {
            await batch.Items[0].Context.Services.GetRequiredService<ISender>().SendNowAsync(new Payload());
            await Assert.That(transport.Sends).IsEqualTo(0);
            return BatchResult.Success;
        };
        await Pipeline(provider).ProcessBatchAsync(Input(3), new Disposition[3]);
        await Assert.That(transport.Sends).IsEqualTo(1);
        await Assert.That(control.Disposed).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Partial_failure_with_buffered_sends_retries_whole_attempt_and_reaches_bounded_settlement(bool poison)
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var transport = provider.GetRequiredService<Transport>();
        var calls = 0;
        var attempts = new List<string>();
        control.Invoke = async (batch, _) =>
        {
            calls++;
            attempts.Add(string.Join(",", batch.Items.Select(item => item.Context.MessageId + ":" + item.Context.Attempt)));
            for (var i = 0; i < batch.Count; i++)
                await batch.Items[i].Context.Services.GetRequiredService<ISender>().SendNowAsync(new Payload());
            return poison || calls == 1
                ? BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = new Exception("partial") })
                : BatchResult.Success;
        };
        var failures = new FailurePipeline(provider, "test", "source", "batch", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var output = Sentinels(3);
        await Pipeline(provider, failures: failures).ProcessBatchAsync(Input(3), output);
        await Assert.That(string.Join(";", attempts)).IsEqualTo("0:1,1:1,2:1;0:2,1:2,2:2");
        await Assert.That(output.All(item => item.Kind == (poison ? DispositionKind.DeadLetter : DispositionKind.Complete))).IsTrue();
        await Assert.That(transport.Sends).IsEqualTo(poison ? 0 : 3);
        await Assert.That(control.Disposed).IsEqualTo(2);
    }

    [Test]
    public async Task Durable_copy_must_finish_before_any_batch_disposition_is_published()
    {
        await using var provider = Provider();
        var transport = provider.GetRequiredService<Transport>();
        transport.NativeDeadLetter = false;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var durable = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Send = (_, _) => { started.SetResult(); return new(durable.Task); };
        provider.GetRequiredService<Control>().Invoke = (_, _) => new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = new Exception("failed") }));
        var failures = new FailurePipeline(provider, "test", "source", "batch", new() { DeadLetterEndpoint = "dead" },
            context => new("source", new byte[] { 1 }, new[] { new KeyValuePair<string, WireString>("ce_id", new(context.MessageId)) }));
        var output = Sentinels(3);
        var pending = Pipeline(provider, failures: failures).ProcessBatchAsync(Input(3), output);
        await started.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(output.All(item => item.Kind == DispositionKind.SettledByHandler)).IsTrue();
        durable.SetResult();
        await pending;
        await Assert.That(output.All(item => item.Kind == DispositionKind.Complete)).IsTrue();
        await Assert.That(transport.Sends).IsEqualTo(1);
    }

    [Test]
    [Arguments(false, 0)]
    [Arguments(true, 0)]
    [Arguments(false, 1)]
    [Arguments(true, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 2)]
    public async Task Manual_batch_outcomes_follow_per_item_exception_precedence(bool executable, int failureMode)
    {
        await using var provider = Provider();
        provider.GetRequiredService<Transport>().RedeliveryDelay = TimeSpan.FromSeconds(5);
        var control = provider.GetRequiredService<Control>();
        control.Invoke = (batch, _) =>
        {
            batch.Items[0].Context.Features.GetRequired<ISettlementFeature>().Complete();
            batch.Items[1].Context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.FromSeconds(5));
            batch.Items[2].Context.Features.GetRequired<ISettlementFeature>().DeadLetter("manual");
            if (failureMode == 2) throw new InvalidOperationException("whole batch failed");
            return new(failureMode == 1 ? BatchResult.PartialFailure(new Dictionary<int, Exception> { [0] = new InvalidOperationException("first failed") }) : BatchResult.Success);
        };
        var failures = executable ? new FailurePipeline(provider, "test", "source", "consumer") : null;
        var pipeline = Pipeline(provider, static (context, _, _) =>
        {
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.DeadLettered(new("failure"));
            return default;
        }, failures);
        var output = new Disposition[3];
        await pipeline.ProcessBatchAsync(Input(3), output);
        await Assert.That(output[0].Kind).IsEqualTo(failureMode == 0 ? DispositionKind.Complete : DispositionKind.DeadLetter);
        await Assert.That(output[1].Kind).IsEqualTo(failureMode == 2 ? DispositionKind.DeadLetter : DispositionKind.Redeliver);
        var failureReason = executable ? "Processing failed" : "failure";
        await Assert.That(output[2].DeadLetter.Reason).IsEqualTo(failureMode == 2 ? failureReason : "manual");
        control.Invoke = static (_, _) => new(BatchResult.Success);
        await pipeline.ProcessBatchAsync(Input(3), output);
        await Assert.That(output.All(item => item.Kind == DispositionKind.Complete)).IsTrue();
    }

    [Test]
    public async Task Batch_manual_retry_clears_request_before_exception_retry()
    {
        await using var provider = Provider();
        provider.GetRequiredService<Control>().Invoke = (batch, _) =>
        {
            var context = batch.Items[0].Context;
            context.Features.GetRequired<ISettlementFeature>().Complete();
            if (context.Attempt > 1) return new(BatchResult.Success);
            return new(BatchResult.PartialFailure(new Dictionary<int, Exception> { [0] = new InvalidOperationException("retry") }));
        };
        var failures = new FailurePipeline(provider, "test", "source", "consumer", new() { RetryDelays = new[] { TimeSpan.Zero } });
        var output = new Disposition[1];
        await Pipeline(provider, failures: failures).ProcessBatchAsync(Input(1), output);
        await Assert.That(output[0].Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(provider.GetRequiredService<Control>().Scopes.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Buffered_batch_sends_are_discarded_and_mixed_manual_outcomes_fail_explicitly(bool mixed)
    {
        await using var provider = Provider();
        provider.GetRequiredService<Control>().Invoke = async (batch, _) =>
        {
            batch.Items[0].Context.Features.GetRequired<ISettlementFeature>().DeadLetter("manual");
            if (!mixed) batch.Items[1].Context.Features.GetRequired<ISettlementFeature>().DeadLetter("manual");
            await batch.Items[0].Context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
            return BatchResult.Success;
        };
        var output = new Disposition[2];
        var failures = new FailurePipeline(provider, "test", "source", "consumer");
        await Pipeline(provider, failures: failures).ProcessBatchAsync(Input(2), output);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
        await Assert.That(output.All(item => item.Kind == DispositionKind.DeadLetter)).IsTrue();
        await Assert.That(output[0].DeadLetter.Reason).IsEqualTo(mixed ? "Processing failed" : "manual");
    }

    [Test]
    public async Task Batch_context_ownership_and_single_message_entry_are_validated_before_dispatch()
    {
        await using var provider = Provider();
        var first = Consumer(provider);
        var second = Consumer(provider);
        var message = new ConsumeMessage { MessageId = "id", MessageType = new("batch.payload", 1), Body = new(new byte[] { 1 }) };
        using var owned = first.RentContext(message);
        using var foreign = second.RentContext(message);
        await Assert.That(() => { _ = first.ProcessAsync(owned); }).Throws<InvalidOperationException>();
        await Assert.That(async () => await first.ProcessBatchAsync(new[] { owned, foreign })).Throws<InvalidOperationException>();
        // Rejection must not consume the foreign rental.
        await second.ProcessBatchAsync(new[] { foreign });
        await Assert.That(provider.GetRequiredService<Control>().Scopes.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Warmed_batches_add_no_allocations_beyond_identical_fresh_DI_scope_and_sender(bool bufferedSends)
    {
        await using var provider = Provider(size: 10);
        provider.GetRequiredService<AllocationHandler>().BufferedSends = bufferedSends;
        using var batchCancellation = new CancellationTokenSource();
        var deliveries = Enumerable.Range(0, 10).Select(_ => new CancellationTokenSource()).ToArray();
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var consumer = new ConsumePipelineBuilder(scopes, provider.GetRequiredService<MessageTypeRegistry>(),
            provider.GetRequiredService<IMessageSerializer>(), static (_, error, _) => ValueTask.FromException(error))
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, AllocationHandler>(), Definition(provider));
        var message = new ConsumeMessage { MessageId = "allocation", MessageType = new("batch.payload", 1), Body = new(new byte[] { 1 }) };
        var contexts = new DefaultMessageContext[10];
        for (var iteration = 0; iteration < 1000; iteration++)
        {
            for (var i = 0; i < contexts.Length; i++) contexts[i] = consumer.RentContext(message, deliveries[i].Token);
            await consumer.ProcessBatchAsync(contexts, batchCancellation.Token);
            for (var i = 0; i < contexts.Length; i++) contexts[i].Dispose();
            using var scope = scopes.CreateScope();
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
        }
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 1000; iteration++)
        {
            using var scope = scopes.CreateScope();
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
        }
        var baseline = GC.GetAllocatedBytesForCurrentThread() - start;
        start = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 1000; iteration++)
        {
            for (var i = 0; i < contexts.Length; i++) contexts[i] = consumer.RentContext(message, deliveries[i].Token);
            await consumer.ProcessBatchAsync(contexts, batchCancellation.Token);
            for (var i = 0; i < contexts.Length; i++) contexts[i].Dispose();
        }
        var composed = GC.GetAllocatedBytesForCurrentThread() - start;
        foreach (var delivery in deliveries) delivery.Dispose();
        await Assert.That(composed).IsEqualTo(baseline);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Executable_receive_and_generated_batch_share_existing_size_wait_and_scope_declaration(bool native)
    {
        await using var provider = Provider(size: 3, nativeSize: native ? 10 : 1);
        var incoming = Input(3);
        var offset = 0;
        BatchReceiveDelegate<Incoming> read = (destination, wait, _) =>
        {
            if (wait > Definition(provider).BatchWait) throw new Exception("Wait exceeds declaration");
            incoming.AsMemory(offset, destination.Length).CopyTo(destination);
            offset += destination.Length;
            return new(destination.Length);
        };
        var executor = new BatchReceiveExecutor<Incoming>(provider, "test", Definition(provider), "audit", native ? read : null, native ? null : read);
        var storage = new Incoming[executor.MaximumSize];
        var count = await executor.ReceiveAsync(storage);
        var output = Sentinels(4);
        await Pipeline(provider).ProcessBatchAsync(storage.AsMemory(0, count), output);
        await Assert.That(count).IsEqualTo(3);
        await Assert.That(provider.GetRequiredService<Control>().Scopes.Count).IsEqualTo(1);
        await Assert.That(output.Take(3).All(item => item.Kind == DispositionKind.Complete)).IsTrue();
        await Assert.That(output[3].Kind).IsEqualTo(DispositionKind.SettledByHandler);
    }

    [Test]
    public async Task Handled_batch_failures_mark_error_without_exception_events()
    {
        await using var provider = Provider();
        using var source = new ActivitySource("Relayer");
        Activity? observed = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => ReferenceEquals(candidate, source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => observed = activity
        };
        ActivitySource.AddActivityListener(listener);
        provider.GetRequiredService<Control>().Invoke = (_, _) => ValueTask.FromException<BatchResult>(new ApplicationException("handler"));
        await Pipeline(provider, static (_, _, _) => default, source: source).ProcessBatchAsync(Input(3), new Disposition[3]);
        await Assert.That(observed!.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(observed.GetTagItem("error.type") as string).IsEqualTo("System.ApplicationException");
        await Assert.That(observed.Events.Count()).IsEqualTo(0);
    }

    internal static ServiceProvider Provider(int size = 4, int nativeSize = 10)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddSingleton(new Transport(nativeSize));
        services.AddSingleton<ITransport>(provider => provider.GetRequiredService<Transport>());
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddScoped<Lifetime>();
        services.AddSingleton<AllocationHandler>();
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Payload>("batch.payload");
            builder.Handler<AllocationHandler>();
            builder.AddTransport<Options, Options, Options>("test", transport =>
            {
                transport.Send<Payload>("destination");
                transport.Consume("source", consumer => consumer.Handler<Handler>().Batch(size, TimeSpan.FromSeconds(10)));
            });
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    internal static IConsumerDefinition Definition(ServiceProvider provider) => provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
    internal static ConsumePipeline Consumer(ServiceProvider provider, ConsumeFailureDelegate? failure = null, FailurePipeline? failures = null, ActivitySource? source = null)
    {
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var messages = provider.GetRequiredService<MessageTypeRegistry>();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var builder = failures is null ? new ConsumePipelineBuilder(scopes, messages, serializer, failure ?? (static (_, error, _) => ValueTask.FromException(error)))
            : new ConsumePipelineBuilder(scopes, messages, serializer, failures);
        return builder.BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(), Definition(provider), source);
    }
    private static ReceivePipeline<Incoming, Binding> Pipeline(ServiceProvider provider, ConsumeFailureDelegate? failure = null, FailurePipeline? failures = null, ActivitySource? source = null)
        => new(Consumer(provider, failure, failures, source), new HeaderNames(), 8);
    internal static Incoming[] Input(int count) => Enumerable.Range(0, count).Select(i => new Incoming(i.ToString())).ToArray();
    private static Disposition[] Sentinels(int count) => Enumerable.Repeat(Disposition.SettledByHandler, count).ToArray();
    public sealed class Options;
    [Message("batch.payload")]
    public sealed class Payload;
    public sealed class Control
    {
        public Func<Batch<Payload>, CancellationToken, ValueTask<BatchResult>>? Invoke { get; set; }
        public List<IServiceProvider> Scopes { get; } = [];
        public int Disposed { get; set; }
        public Task? Disposal { get; set; }
        public TaskCompletionSource? DisposalStarted { get; set; }
    }
    private sealed class CancelOnClassify(CancellationTokenSource cancellation) : IFailureClassifier
    {
        public FailureAction Classify(Exception exception, MessageContext context)
        {
            cancellation.Cancel();
            return FailureAction.Retry;
        }
    }

    public sealed class Lifetime(Control control) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            control.DisposalStarted?.TrySetResult();
            if (control.Disposal is { } disposal) await disposal;
            control.Disposed++;
        }
    }
    public sealed class Handler(Control control, Lifetime lifetime) : IBatchHandler<Payload>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken)
        {
            _ = lifetime;
            var scope = batch.Items[0].Context.Services;
            if (batch.Items.Any(item => !ReferenceEquals(scope, item.Context.Services))) throw new Exception("Scopes differ");
            control.Scopes.Add(scope);
            return control.Invoke?.Invoke(batch, cancellationToken) ?? new(BatchResult.Success);
        }
    }
    public sealed class AllocationHandler : IBatchHandler<Payload>
    {
        public bool BufferedSends { get; set; }
        public async ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken)
        {
            if (BufferedSends)
            {
                var sender = batch.Items[0].Context.Services.GetRequiredService<ISender>();
                for (var i = 0; i < batch.Count; i++) await sender.SendNowAsync(batch.Items[i].Message, cancellationToken);
            }
            return BatchResult.Success;
        }
    }
    internal sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => source.FirstSpan[0] == 0 ? throw new FormatException() : (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
    }
    internal sealed record Incoming(string Id)
    {
        public WireString TraceParent { get; init; } = new("00-11111111111111111111111111111111-2222222222222222-01");
        public string Type { get; init; } = "batch.payload;v=1";
        public byte Value { get; init; } = 1;
    }
    internal readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => new(new[] { message.Value });
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch { "ce_id" => new(message.Id), "ce_type" => new(message.Type), "traceparent" => message.TraceParent, _ => default };
            return !value.IsEmpty;
        }
    }
    internal sealed class Transport(int nativeSize) : ITransport
    {
        public string Name => "test";
        public int Reads { get; private set; }
        public int Sends { get; private set; }
        public bool NativeDeadLetter { get; set; } = true;
        public TimeSpan? RedeliveryDelay { get; set; }
        public Func<OutgoingMessage, CancellationToken, ValueTask>? Send { get; set; }
        public TransportCapabilities GetCapabilities(string endpoint) { Reads++; return new() { DeadLetter = NativeDeadLetter, MaxBatchSize = nativeSize, MaxRedeliveryDelay = RedeliveryDelay }; }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Sends++; return Send?.Invoke(message, cancellationToken) ?? default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
