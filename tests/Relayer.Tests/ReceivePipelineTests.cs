using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class ReceivePipelineTests
{
    [Test]
    public async Task Partition_scheduler_waits_for_existing_receive_failure_pipeline_before_checkpointing()
    {
        await using var provider = Provider();
        var durable = new TaskCompletionSource();
        provider.GetRequiredService<Control>().Invoke = static _ => ValueTask.FromException(new FormatException("poison"));
        var receive = Pipeline(provider, (_, _, _) => new(durable.Task));
        var source = new PartitionSource();
        Task<Disposition>? processing = null;
        var scheduler = new PartitionScheduler<Incoming, PartitionBinding, int, long>(source, (message, cancellation) =>
        {
            processing = receive.ProcessAsync(message, cancellation).AsTask();
            return new(processing);
        });
        var assignment = scheduler.Assign(1);
        scheduler.TryEnqueue(assignment, new(), 91);
        scheduler.Pump();
        await Assert.That(source.Checkpoint).IsEqualTo(0L);
        await Assert.That(scheduler.ActiveCount).IsEqualTo(1);
        durable.SetResult();
        await processing!;
        scheduler.Pump();
        await Assert.That(source.Checkpoint).IsEqualTo(91L);
        await Assert.That(scheduler.BufferedCount).IsEqualTo(0);
    }

    private sealed class PartitionSource : IPartitionSource<int, long>
    {
        internal long Checkpoint;
        public void Commit(int partition, long position) => Checkpoint = position;
        public void Pause(int partition) { }
        public void Resume(int partition) { }
    }

    private readonly struct PartitionBinding : IPartitionMessageBinding<Incoming>
    {
        public static bool KeysEqual(Incoming first, Incoming second) => ReferenceEquals(first, second);
        public static ReadOnlySequence<byte> GetBody(Incoming message) => Binding.GetBody(message);
        public static int? GetDeliveryCount(Incoming message) => Binding.GetDeliveryCount(message);
        public static bool TryGetHeader(Incoming message, string name, out WireString value) => Binding.TryGetHeader(message, name, out value);
    }

    [Test]
    public async Task Native_delivery_body_and_lazy_headers_reach_handler_without_copying()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var native = new Incoming { Count = 7, Body = new(new byte[] { 42 }) };
        control.Invoke = context =>
        {
            if (!ReferenceEquals(context.Features.GetRequired<NativeMessage<Incoming>>().Message, native))
                throw new InvalidOperationException("Native delivery was replaced.");
            if (context.Attempt != 7 || context.MessageType != new MessageType("payload", 1))
                throw new InvalidOperationException("Metadata was changed.");
            if (native.HeaderReads != 2) throw new InvalidOperationException("Headers beyond required identity were read.");
            if (!context.Headers.TryGetValue("tenant", out var tenant) || tenant.Text is not null ||
                !tenant.Utf8.Span.SequenceEqual("東京"u8)) throw new InvalidOperationException("Header was decoded.");
            if (context.MessageId != "id" || context.MessageId != "id" || native.IdReads != 1)
                throw new InvalidOperationException("ID was not cached lazily.");
            return default;
        };
        var pipeline = Pipeline(provider);
        var result = await pipeline.ProcessAsync(native);
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(control.Body.Equals(native.Body)).IsTrue();
        await Assert.That(control.Calls).IsEqualTo(1);
    }

    [Test]
    public async Task Returned_context_and_native_feature_are_cleared_and_reused()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        MessageContext? first = null;
        NativeMessage<Incoming>? feature = null;
        control.Invoke = context =>
        {
            first = context;
            feature = context.Features.GetRequired<NativeMessage<Incoming>>();
            feature.Disposition = Disposition.Redeliver(TimeSpan.FromSeconds(2));
            return default;
        };
        var pipeline = Pipeline(provider);
        var firstResult = await pipeline.ProcessAsync(new Incoming());
        await Assert.That(firstResult.Delay).IsEqualTo(TimeSpan.FromSeconds(2));
        await Assert.That(feature!.Message).IsNull();
        await Assert.That(first!.Features.Get<NativeMessage<Incoming>>()).IsNull();
        await Assert.That(first.Headers).IsSameReferenceAs(MessageHeaders.Empty);
        var second = new Incoming { Count = null };
        control.Invoke = context =>
        {
            if (!ReferenceEquals(first, context) || !ReferenceEquals(feature, context.Features.GetRequired<NativeMessage<Incoming>>()) ||
                !ReferenceEquals(feature.Message, second) || context.Attempt != 1)
                throw new InvalidOperationException("Context or feature was not reused cleanly.");
            return default;
        };
        await Assert.That((await pipeline.ProcessAsync(second)).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Opt_in_settled_failure_signal_preserves_original_error_and_clears_native_state(bool asynchronous)
    {
        await using var provider = Provider();
        var error = new ApplicationException("after settlement");
        NativeMessage<Incoming>? observed = null;
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            observed = context.Features.GetRequired<NativeMessage<Incoming>>();
            observed.Disposition = Disposition.SettledByHandler;
            if (asynchronous) await Task.Yield();
            throw error;
        };
        var ordinary = Pipeline(provider);
        await Assert.That(async () => await ordinary.ProcessAsync(new Incoming())).Throws<ApplicationException>();
        var optedIn = new ReceivePipeline<Incoming, Binding>(PipelineConsumer(provider), new HeaderNames(), 1, preserveSettledFailures: true);
        SettledReceiveException? signal = null;
        try { await optedIn.ProcessAsync(new Incoming()); }
        catch (SettledReceiveException failure) { signal = failure; }
        await Assert.That(signal).IsNotNull();
        await Assert.That(signal!.InnerException).IsSameReferenceAs(error);
        await Assert.That(observed!.Message).IsNull();
    }

    [Test]
    public async Task Failure_pipeline_sets_native_dispositions_and_errors_do_not_complete()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        control.Invoke = _ => ValueTask.FromException(new FormatException("handler failed"));
        NativeMessage<Incoming>? observed = null;
        var headers = new[] { new KeyValuePair<string, WireString>("ce_error", new("bad payload")) };
        var pipeline = Pipeline(provider, (context, _, action) =>
        {
            if (action != FailureAction.Retry) throw new InvalidOperationException("Wrong classifier action.");
            observed = context.Features.GetRequired<NativeMessage<Incoming>>();
            observed.Disposition = Disposition.DeadLettered(new("invalid", "description", headers));
            return default;
        });
        var result = await pipeline.ProcessAsync(new Incoming());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(result.DeadLetter.Reason).IsEqualTo("invalid");
        await Assert.That(result.DeadLetter.Description).IsEqualTo("description");
        await Assert.That(result.DeadLetter.Headers.Equals(headers.AsMemory())).IsTrue();
        await Assert.That(observed!.Message).IsNull();

        var broken = Pipeline(provider, (context, _, _) =>
        {
            observed = context.Features.GetRequired<NativeMessage<Incoming>>();
            return ValueTask.FromException(new ApplicationException("failure persistence failed"));
        });
        await Assert.That(async () => await broken.ProcessAsync(new Incoming())).Throws<ApplicationException>();
        await Assert.That(observed!.Message).IsNull();
        control.Invoke = context =>
        {
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.SettledByHandler;
            return default;
        };
        await Assert.That((await broken.ProcessAsync(new Incoming())).Kind).IsEqualTo(DispositionKind.SettledByHandler);
    }

    [Test]
    public async Task Unknown_and_malformed_types_enter_failure_pipeline_once()
    {
        await using var provider = Provider();
        var errors = new List<Exception>();
        var pipeline = Pipeline(provider, (context, error, action) =>
        {
            errors.Add(error);
            if (action != FailureAction.DeadLetter) throw new InvalidOperationException("Poison metadata was retried.");
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.DeadLettered(new("invalid envelope"));
            return default;
        });
        foreach (var type in new[] { "unknown;v=2", "missing-version", "payload;v=0", "" })
            await Assert.That((await pipeline.ProcessAsync(new Incoming { Type = new(type) })).Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That((await pipeline.ProcessAsync(new Incoming { Id = default })).Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(errors.Count).IsEqualTo(5);
        await Assert.That(errors[0] is UnknownMessageTypeException).IsTrue();
        await Assert.That(errors.Skip(1).All(e => e is MessageEnvelopeException)).IsTrue();
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(0);
    }

    [Test]
    public async Task Batch_writes_each_disposition_in_order_and_preserves_unused_output()
    {
        await using var provider = Provider();
        var seen = new List<int>();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            seen.Add(context.Attempt);
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.Redeliver(TimeSpan.FromSeconds(context.Attempt));
            return default;
        };
        var pipeline = Pipeline(provider);
        var input = new[] { new Incoming { Count = 1 }, new Incoming { Count = 3 } };
        var output = new[] { Disposition.SettledByHandler, Disposition.SettledByHandler, Disposition.SettledByHandler };
        await pipeline.ProcessBatchAsync(input, output);
        await Assert.That(string.Join(",", seen)).IsEqualTo("1,3");
        await Assert.That(output[0].Delay).IsEqualTo(TimeSpan.FromSeconds(1));
        await Assert.That(output[1].Delay).IsEqualTo(TimeSpan.FromSeconds(3));
        await Assert.That(output[2].Kind).IsEqualTo(DispositionKind.SettledByHandler);
        await pipeline.ProcessBatchAsync(ReadOnlyMemory<Incoming>.Empty, Memory<Disposition>.Empty);
        await Assert.That(async () => await pipeline.ProcessBatchAsync(input, new Disposition[1])).Throws<ArgumentException>();
    }

    [Test]
    public async Task Pending_concurrent_deliveries_keep_native_state_until_processing_finishes()
    {
        await using var provider = Provider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var features = new List<NativeMessage<Incoming>>();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            features.Add(context.Features.GetRequired<NativeMessage<Incoming>>());
            return new ValueTask(release.Task);
        };
        var pipeline = Pipeline(provider);
        var first = new Incoming();
        var second = new Incoming();
        var firstCall = pipeline.ProcessAsync(first);
        var secondCall = pipeline.ProcessAsync(second);
        await Assert.That(firstCall.IsCompleted).IsFalse();
        await Assert.That(secondCall.IsCompleted).IsFalse();
        await Assert.That(features[0]).IsNotSameReferenceAs(features[1]);
        await Assert.That(features[0].Message).IsSameReferenceAs(first);
        await Assert.That(features[1].Message).IsSameReferenceAs(second);
        release.SetResult();
        await firstCall;
        await secondCall;
        await Assert.That(features.All(f => f.Message is null)).IsTrue();
    }

    [Test]
    public async Task Native_state_survives_async_scope_disposal_and_clears_when_disposal_fails()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var disposal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.DisposalStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Disposal = disposal.Task;
        NativeMessage<Incoming>? feature = null;
        control.Invoke = context =>
        {
            feature = context.Features.GetRequired<NativeMessage<Incoming>>();
            _ = context.Services.GetRequiredService<Lifetime>();
            return default;
        };
        var native = new Incoming();
        var pending = Pipeline(provider).ProcessAsync(native);
        await control.DisposalStarted.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(feature!.Message).IsSameReferenceAs(native);
        disposal.SetException(new ApplicationException("scope disposal failed"));
        await Assert.That(async () => await pending).Throws<ApplicationException>();
        await Assert.That(feature.Message).IsNull();
    }

    [Test]
    public async Task Cancellation_and_binding_failures_clear_native_state_without_settlement()
    {
        await using var provider = Provider();
        var pipeline = Pipeline(provider);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var unread = new Incoming();
        await Assert.That(async () => await pipeline.ProcessAsync(unread, cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(unread.HeaderReads).IsEqualTo(0);
        await Assert.That(async () => await pipeline.ProcessBatchAsync(new[] { unread }, new Disposition[1], cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await pipeline.ProcessAsync(new Incoming { Count = 0 })).Throws<ArgumentOutOfRangeException>();
        NativeMessage<Incoming>? feature = null;
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            feature = context.Features.GetRequired<NativeMessage<Incoming>>();
            return ValueTask.FromCanceled(cancellation.Token);
        };
        // An unrelated cancellation is classified as a handler failure and propagates through the explicit failure delegate.
        await Assert.That(async () => await pipeline.ProcessAsync(new Incoming())).Throws<OperationCanceledException>();
        await Assert.That(feature!.Message).IsNull();
        provider.GetRequiredService<Control>().Invoke = null;
        await Assert.That((await pipeline.ProcessAsync(new Incoming())).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    public async Task Warmed_receive_state_and_feature_reuse_add_no_allocations_beyond_native_DI_scope()
    {
        await using var provider = Provider();
        var pipeline = Pipeline(provider);
        var native = new Incoming { TrackReads = false };
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        for (var i = 0; i < 1000; i++)
        {
            await pipeline.ProcessAsync(native);
            scopes.CreateScope().Dispose();
        }
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) scopes.CreateScope().Dispose();
        var nativeBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            var pending = pipeline.ProcessAsync(native);
            if (!pending.IsCompletedSuccessfully) throw new InvalidOperationException("Expected synchronous completion.");
            await pending;
        }
        var receiveBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        await Assert.That(receiveBytes).IsEqualTo(nativeBytes);
    }

    [Test]
    public async Task Dispositions_validate_arguments_and_preserve_values()
    {
        await Assert.That(Disposition.Complete.Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(Disposition.Redeliver(TimeSpan.Zero).Kind).IsEqualTo(DispositionKind.Redeliver);
        await Assert.That(() => Disposition.Redeliver(TimeSpan.FromTicks(-1))).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new DeadLetterInfo(" ")).Throws<ArgumentException>();
        await Assert.That(() => Disposition.DeadLettered(default)).Throws<ArgumentException>();
    }

    [Test]
    [Arguments(DispositionKind.Complete, false)]
    [Arguments(DispositionKind.Redeliver, false)]
    [Arguments(DispositionKind.DeadLetter, false)]
    [Arguments(DispositionKind.Complete, true)]
    [Arguments(DispositionKind.Redeliver, true)]
    [Arguments(DispositionKind.DeadLetter, true)]
    public async Task Explicit_failure_delegate_honors_requests_and_exception_precedence(DispositionKind kind, bool fail)
    {
        using var provider = Provider();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            var settlement = context.Features.GetRequired<ISettlementFeature>();
            switch (kind)
            {
                case DispositionKind.Complete: settlement.Complete(); break;
                case DispositionKind.Redeliver: settlement.Retry(TimeSpan.Zero); break;
                case DispositionKind.DeadLetter: settlement.DeadLetter("manual"); break;
            }
            if (fail) throw new InvalidOperationException("handler failed");
            return default;
        };
        var pipeline = Pipeline(provider, static (context, _, _) =>
        {
            context.Features.GetRequired<NativeMessage<Incoming>>().Disposition = Disposition.DeadLettered(new("failure"));
            return default;
        });
        var result = await pipeline.ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(fail ? DispositionKind.DeadLetter : kind);
        if (result.Kind == DispositionKind.DeadLetter)
            await Assert.That(result.DeadLetter.Reason).IsEqualTo(fail ? "failure" : "manual");
    }

    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddScoped<Lifetime>();
        services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("payload"); new ConsumerBuilder<object>(r.Services, r.Handlers, "manual").Handler<Handler>(); });
        return services.BuildServiceProvider();
    }

    private static ReceivePipeline<Incoming, Binding> Pipeline(ServiceProvider provider, ConsumeFailureDelegate? failure = null)
        => new(PipelineConsumer(provider, failure), new HeaderNames(), 1);

    private static ConsumePipeline PipelineConsumer(ServiceProvider provider, ConsumeFailureDelegate? failure = null)
    {
        var control = provider.GetRequiredService<Control>();
        return new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(control),
            failure ?? (static (_, error, _) => ValueTask.FromException(error)))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(1);
    }

    [Message("payload")]
    public sealed class Payload;
    public sealed class Control
    {
        public Func<MessageContext, ValueTask>? Invoke { get; set; }
        public int Calls { get; set; }
        public ReadOnlySequence<byte> Body { get; set; }
        public TaskCompletionSource? DisposalStarted { get; set; }
        public Task? Disposal { get; set; }
    }
    private sealed class Lifetime(Control control) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            control.DisposalStarted?.SetResult();
            return control.Disposal is { } task ? new(task) : default;
        }
    }
    public sealed class Handler(Control control) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            control.Calls++;
            return control.Invoke?.Invoke(context) ?? default;
        }
    }
    private sealed class Serializer(Control control) : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            control.Body = source;
            return (TMessage)(object)_payload;
        }
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class Incoming
    {
        public ReadOnlySequence<byte> Body { get; init; } = new(new byte[] { 1 });
        public WireString Type { get; init; } = new("payload;v=1"u8.ToArray());
        public WireString Id { get; init; } = new("id"u8.ToArray());
        public WireString Tenant { get; } = new("東京"u8.ToArray());
        public int? Count { get; init; } = 1;
        public int HeaderReads { get; set; }
        public int IdReads { get; set; }
        public bool TrackReads { get; init; } = true;
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => message.Body;
        public static int? GetDeliveryCount(Incoming message) => message.Count;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            if (message.TrackReads) message.HeaderReads++;
            value = name switch
            {
                "ce_type" => message.Type,
                "ce_id" => message.Id,
                "tenant" => message.Tenant,
                _ => default,
            };
            if (name == "ce_id") message.IdReads++;
            return !value.IsEmpty;
        }
    }
}
