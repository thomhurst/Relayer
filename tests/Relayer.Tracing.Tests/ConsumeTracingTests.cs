#pragma warning disable RLY001 // Fixtures explicitly register their cached payload and wire identity.
using System.Buffers;
using System.Diagnostics;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class ConsumeTracingTests
{
    private static readonly MessagingEndpoint Endpoint = new()
    {
        System = "kafka", Destination = "orders", ConsumerGroup = "billing", ClientId = "consumer",
        ServerAddress = "broker", ServerPort = 9092,
    };

    [Test]
    [Arguments(false, true)]
    [Arguments(false, false)]
    [Arguments(true, true)]
    public async Task Native_delivery_links_creation_and_obeys_parent_policy(bool ambient, bool creationAsParent)
    {
        using var capture = new Capture(new() { CreationContextAsParent = creationAsParent, IncludeBodySize = true });
        await using var provider = Provider();
        using var parent = ambient ? new Activity("request").Start() : null;
        var incoming = new Incoming();
        var pipeline = Builder(provider).Tracing(capture.Tracing, Endpoint).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new HeaderNames("cloudEvents:"), 1);
        await receive.ProcessAsync(incoming);
        var process = capture.Stopped.Single();
        await Assert.That(process.DisplayName).IsEqualTo("process orders");
        await Assert.That(process.Kind).IsEqualTo(ActivityKind.Consumer);
        var creation = process.Links.Single().Context;
        await Assert.That(creation.IsRemote).IsTrue();
        await Assert.That(process.ParentSpanId).IsEqualTo(parent?.SpanId ?? (creationAsParent ? creation.SpanId : default));
        await Assert.That(capture.SamplingTags["messaging.destination.partition.id"]).IsEqualTo("3");
        await Assert.That(capture.SamplingTags["messaging.consumer.group.name"]).IsEqualTo("billing");
        await Assert.That(capture.SamplingLinks.Single().Context).IsEqualTo(creation);
        await Assert.That(process.GetTagItem("messaging.message.id")).IsEqualTo("native");
        await Assert.That(process.GetTagItem("cloudevents.event_type")).IsEqualTo("number;v=1");
        await Assert.That(process.GetTagItem("cloudevents.event_source")).IsEqualTo("/shop");
        await Assert.That(process.GetTagItem("messaging.message.conversation_id")).IsEqualTo("conversation");
        await Assert.That(process.GetTagItem("messaging.message.body.size")).IsEqualTo(0L);
        await Assert.That(process.GetTagItem("messaging.kafka.offset")).IsEqualTo(42L);
        await Assert.That(process.GetTagItem("messaging.batch.message_count")).IsNull();
        await Assert.That(Activity.Current).IsSameReferenceAs(parent);
    }

    [Test]
    public async Task Process_remains_current_through_async_disposal_and_native_settlement()
    {
        using var capture = new Capture();
        var gate = new DisposalGate();
        await using var provider = Provider(services => services.AddScoped(_ => gate));
        Activity? handling = null;
        provider.GetRequiredService<Control>().Handle = context =>
        {
            context.Services.GetRequiredService<DisposalGate>();
            handling = Activity.Current;
            return default;
        };
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var complete = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "complete")
            .WrapSettlement(async _ =>
            {
                entered.SetResult();
                await finished.Task;
            });
        var pipeline = Builder(provider).Tracing(capture.Tracing, Endpoint).Settle(complete).Build(1);
        using var context = pipeline.RentContext(Message());
        var pending = pipeline.ProcessAsync(context).AsTask();
        await Task.WhenAny(gate.Entered.Task, pending);
        try
        {
            await Assert.That(gate.Entered.Task.IsCompleted).IsTrue();
            await Assert.That(gate.Activity).IsSameReferenceAs(handling);
            await Assert.That(capture.Stopped).IsEmpty();
        }
        finally { gate.Finished.TrySetResult(); }
        await Task.WhenAny(entered.Task, pending);
        try
        {
            await Assert.That(entered.Task.IsCompleted).IsTrue();
            await Assert.That(capture.Stopped).IsEmpty();
        }
        finally { finished.TrySetResult(); }
        await pending;
        var settle = capture.Stopped[0];
        var process = capture.Stopped[1];
        await Assert.That(settle.DisplayName).IsEqualTo("complete orders");
        await Assert.That(settle.ParentSpanId).IsEqualTo(process.SpanId);
        await Assert.That(settle.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(settle.GetTagItem("messaging.operation.type")).IsEqualTo("settle");
        await Assert.That(process).IsSameReferenceAs(handling);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Manual_settlement_has_one_child_span_and_core_skips_second_settlement(bool fails)
    {
        using var capture = new Capture();
        await using var provider = Provider();
        var calls = 0;
        var complete = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "ack")
            .WrapSettlement(_ => { calls++; return fails ? ValueTask.FromException(new ApplicationException("native")) : default; });
        provider.GetRequiredService<Control>().Handle = async context =>
        {
            await complete(context);
            context.Features.Set(new Settled());
        };
        var pipeline = Builder(provider, static (_, error, _) => ValueTask.FromException(error))
            .Tracing(capture.Tracing, Endpoint)
            .Settle(context => context.Features.Get<Settled>() is null ? complete(context) : default).Build(1);
        using var context = pipeline.RentContext(Message());
        if (fails) await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>();
        else await pipeline.ProcessAsync(context);
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(capture.Stopped.Count).IsEqualTo(2);
        var settle = capture.Stopped[0];
        var process = capture.Stopped[1];
        await Assert.That(settle.ParentSpanId).IsEqualTo(process.SpanId);
        foreach (var activity in capture.Stopped)
        {
            await Assert.That(activity.Status).IsEqualTo(fails ? ActivityStatusCode.Error : ActivityStatusCode.Unset);
            await Assert.That(activity.GetTagItem("error.type")).IsEqualTo(fails ? "_OTHER" : null);
            await Assert.That(activity.Events).IsEmpty();
        }
    }

    [Test]
    [Arguments("handler")]
    [Arguments("failure")]
    [Arguments("scope")]
    [Arguments("settlement")]
    public async Task Unrecovered_errors_mark_process_and_preserve_original_error(string stage)
    {
        using var capture = new Capture();
        var error = new ApplicationException(stage);
        await using var provider = Provider(services => services.AddScoped(_ => new DisposalFailure(error)));
        provider.GetRequiredService<Control>().Handle = context =>
        {
            if (stage == "scope") context.Services.GetRequiredService<DisposalFailure>();
            return stage is "handler" or "failure" ? ValueTask.FromException(error) : default;
        };
        var settle = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "complete")
            .WrapSettlement(_ => stage == "settlement" ? ValueTask.FromException(error) : default);
        var pipeline = Builder(provider, (_, _, _) => stage == "failure" ? ValueTask.FromException(error) : default)
            .Tracing(capture.Tracing, Endpoint).Settle(settle).Build(1);
        using var context = pipeline.RentContext(Message());
        if (stage != "handler") await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>();
        else await pipeline.ProcessAsync(context);
        var process = capture.Stopped.Last();
        await Assert.That(process.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(process.GetTagItem("error.type")).IsEqualTo("_OTHER");
        await Assert.That(process.Events).IsEmpty();
        await Assert.That(Activity.Current).IsNull();
    }

    [Test]
    public async Task Transport_batch_entry_still_creates_one_process_per_delivery_and_no_receive_spans()
    {
        using var capture = new Capture();
        await using var provider = Provider();
        var receive = new ReceivePipeline<Incoming, Binding>(Builder(provider).Tracing(capture.Tracing, Endpoint).Build(1), new("cloudEvents:"), 1);
        await receive.ProcessBatchAsync(new Incoming[] { new(), new() }, new Disposition[2]);
        await Assert.That(capture.Stopped.Count).IsEqualTo(2);
        foreach (var activity in capture.Stopped)
        {
            await Assert.That(activity.OperationName).IsEqualTo("process orders");
            await Assert.That(activity.Links.Count()).IsEqualTo(1);
            await Assert.That(activity.GetTagItem("messaging.batch.message_count")).IsNull();
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task No_listener_and_dropped_spans_leave_recorded_headers_and_native_attributes_unread(bool drop)
    {
        using var capture = new Capture(sampling: drop ? ActivitySamplingResult.None : null);
        await using var provider = Provider();
        var incoming = new Incoming { FailRecordedReads = true, FailTracingReads = !drop };
        var receive = new ReceivePipeline<Incoming, Binding>(Builder(provider).Tracing(capture.Tracing, Endpoint).Build(1), new("cloudEvents:"), 1);
        await receive.ProcessAsync(incoming);
        await Assert.That(capture.Stopped).IsEmpty();
        await Assert.That(incoming.Enrichments).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Immediate_retries_share_process_and_success_clears_failure(bool recover)
    {
        using var capture = new Capture();
        await using var transport = new InMemoryTransport("test", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = new() });
        await using var provider = Provider(services =>
        {
            services.AddSingleton<ITransport>(transport);
            services.AddSingleton<IMessageSerializer, Serializer>();
        });
        var attempts = 0;
        var activities = new List<Activity?>();
        provider.GetRequiredService<Control>().Handle = _ =>
        {
            activities.Add(Activity.Current);
            return ++attempts == 1 || !recover ? ValueTask.FromException(new ApplicationException("attempt")) : default;
        };
        var failures = new FailurePipeline(provider, "test", "orders", "billing", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), failures)
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<int, Handler>())
            .Tracing(capture.Tracing, Endpoint).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 1);
        var disposition = await receive.ProcessAsync(new());
        await Assert.That(disposition.Kind).IsEqualTo(recover ? DispositionKind.Complete : DispositionKind.DeadLetter);
        await Assert.That(attempts).IsEqualTo(2);
        var process = capture.Stopped.Single();
        await Assert.That(activities.All(activity => ReferenceEquals(activity, process))).IsTrue();
        await Assert.That(process.Status).IsEqualTo(recover ? ActivityStatusCode.Unset : ActivityStatusCode.Error);
        await Assert.That(process.GetTagItem("error.type")).IsEqualTo(recover ? null : "_OTHER");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Settlement_failure_logs_once_with_settlement_ids_even_when_handler_catches_it(bool caught)
    {
        var logger = new Logger();
        using var capture = new Capture(logger: logger);
        await using var provider = Provider();
        var error = new ApplicationException("native");
        var settle = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "ack")
            .WrapSettlement(_ => ValueTask.FromException(error));
        if (caught)
            provider.GetRequiredService<Control>().Handle = async context =>
            {
                try { await settle(context); }
                catch (ApplicationException) { }
            };
        var builder = Builder(provider).Tracing(capture.Tracing, Endpoint);
        if (!caught) builder.Settle(settle);
        var pipeline = builder.Build(1);
        using var context = pipeline.RentContext(Message());
        if (caught) await pipeline.ProcessAsync(context);
        else await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>();
        var entry = logger.Entries.Single();
        var settlement = capture.Stopped[0];
        await Assert.That(entry.Error).IsSameReferenceAs(error);
        await Assert.That(entry.SpanId).IsEqualTo(settlement.SpanId.ToString());
        await Assert.That(entry.TraceId).IsEqualTo(settlement.TraceId.ToString());
        await Assert.That(settlement.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(capture.Stopped[1].Status).IsEqualTo(caught ? ActivityStatusCode.Unset : ActivityStatusCode.Error);
    }

    [Test]
    public async Task Delivery_token_cancellation_ends_process_without_marking_failure()
    {
        using var capture = new Capture();
        await using var provider = Provider();
        using var cancellation = new CancellationTokenSource();
        provider.GetRequiredService<Control>().Handle = context =>
        {
            cancellation.Cancel();
            return ValueTask.FromCanceled(context.CancellationToken);
        };
        var pipeline = Builder(provider).Tracing(capture.Tracing, Endpoint).Build(1);
        using var context = pipeline.RentContext(Message(), cancellation.Token);
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<OperationCanceledException>();
        await Assert.That(capture.Stopped.Single().Status).IsEqualTo(ActivityStatusCode.Unset);
        await Assert.That(Activity.Current).IsNull();
    }

    [Test]
    public async Task No_listener_tracing_adds_no_allocations_over_identical_receive_and_DI_work()
    {
        using var capture = new Capture(sampling: null);
        await using var provider = Provider();
        var baseline = new ReceivePipeline<Incoming, Binding>(Builder(provider).Settle(static _ => default).Build(1), new("cloudEvents:"), 1);
        var complete = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "complete").WrapSettlement(static _ => default);
        var traced = new ReceivePipeline<Incoming, Binding>(Builder(provider).Tracing(capture.Tracing, Endpoint).Settle(complete).Build(1), new("cloudEvents:"), 1);
        var incoming = new Incoming { FailRecordedReads = true, FailTracingReads = true };
        for (var i = 0; i < 1000; i++) { await baseline.ProcessAsync(incoming); await traced.ProcessAsync(incoming); }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await baseline.ProcessAsync(incoming);
        var baselineBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await traced.ProcessAsync(incoming);
        var tracedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(tracedBytes).IsEqualTo(baselineBytes);
    }

    [Test]
    public async Task Only_native_settlement_operations_can_wrap_callbacks()
    {
        using var capture = new Capture();
        var process = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process);
        await Assert.That(() => process.WrapSettlement(static _ => default)).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(ActivitySamplingResult.AllData, 0)]
    [Arguments(ActivitySamplingResult.AllDataAndRecorded, 1)]
    public async Task Recorded_enrichers_see_native_and_message_attributes(ActivitySamplingResult sampling, int expected)
    {
        var enricher = new Enricher();
        using var capture = new Capture(sampling: sampling, enricher: enricher);
        await using var provider = Provider();
        var receive = new ReceivePipeline<Incoming, Binding>(Builder(provider).Tracing(capture.Tracing, Endpoint).Build(1), new("cloudEvents:"), 1);
        await receive.ProcessAsync(new());
        await Assert.That(enricher.Calls).IsEqualTo(expected);
        if (expected != 0)
        {
            await Assert.That(enricher.MessageId).IsEqualTo("native");
            await Assert.That(enricher.Offset).IsEqualTo(42L);
        }
        await Assert.That(capture.Stopped.Single().GetTagItem("messaging.message.body.size")).IsNull();
    }

    [Test]
    public async Task Malformed_envelope_still_reaches_durable_failure_handling_with_tracing_enabled()
    {
        using var capture = new Capture();
        await using var provider = Provider();
        var failures = 0;
        var pipeline = Builder(provider, (_, error, action) =>
        {
            if (error is not MessageEnvelopeException || action != FailureAction.DeadLetter)
                throw new InvalidOperationException("Malformed message escaped permanent failure classification.");
            failures++;
            return default;
        }).Tracing(capture.Tracing, Endpoint).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 1);
        await receive.ProcessAsync(new() { MissingType = true });
        await Assert.That(failures).IsEqualTo(1);
        await Assert.That(capture.Stopped.Single().Status).IsEqualTo(ActivityStatusCode.Error);
    }

    [Test]
    [Arguments(1, false)]
    [Arguments(1, true)]
    [Arguments(3, false)]
    [Arguments(3, true)]
    public async Task Composed_batch_uses_shared_source_endpoint_tags_and_links_only(int size, bool ambient)
    {
        using var capture = new Capture();
        await using var provider = BatchProvider();
        using var parent = ambient ? new Activity("request").Start() : null;
        var pipeline = BatchBuilder(provider).Tracing(capture.Tracing, Endpoint).BuildBatch(
            provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<int, BatchHandler>(), BatchDefinition(provider));
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 4);
        await receive.ProcessBatchAsync(Enumerable.Range(0, size).Select(_ => new Incoming()).ToArray(), new Disposition[size]);
        var process = capture.Stopped.Single();
        await Assert.That(process.ParentSpanId).IsEqualTo(parent?.SpanId ?? default);
        await Assert.That(process.Links.Count()).IsEqualTo(size);
        await Assert.That(process.GetTagItem("messaging.batch.message_count")).IsEqualTo(size);
        await Assert.That(capture.SamplingTags["messaging.batch.message_count"]).IsEqualTo(size);
        await Assert.That(capture.SamplingTags["messaging.system"]).IsEqualTo("kafka");
        await Assert.That(process.GetTagItem("messaging.message.id")).IsNull();
        await Assert.That(process.GetTagItem("messaging.destination.partition.id")).IsNull();
        await Assert.That(process.GetTagItem("messaging.kafka.offset")).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Batch_settlement_runs_after_durability_inside_process_and_preserves_manual_disposition(bool poison)
    {
        using var capture = new Capture();
        await using var provider = BatchProvider();
        if (poison) provider.GetRequiredService<BatchControl>().Handle = static _ => throw new ApplicationException("batch");
        var failures = new FailurePipeline(provider, "test", "orders", "billing", new() { RetryDelays = ReadOnlyMemory<TimeSpan>.Empty });
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settled = 0;
        var complete = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "complete")
            .WrapSettlement(async context =>
            {
                var native = context.Features.GetRequired<NativeMessage<Incoming>>();
                if (native.Disposition.Kind != (poison ? DispositionKind.DeadLetter : DispositionKind.Complete))
                    throw new InvalidOperationException("Durable disposition missing before settlement.");
                if (++settled == 1) { entered.SetResult(); await finished.Task; }
                native.Disposition = Disposition.SettledByHandler;
            });
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), failures);
        var pipeline = builder.Tracing(capture.Tracing, Endpoint).Settle(complete).BuildBatch(
            provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<int, BatchHandler>(), BatchDefinition(provider));
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 4);
        var output = new Disposition[2];
        var pending = receive.ProcessBatchAsync(new Incoming[] { new(), new() }, output).AsTask();
        await Task.WhenAny(entered.Task, pending);
        try
        {
            await Assert.That(entered.Task.IsCompleted).IsTrue();
            await Assert.That(capture.Stopped).IsEmpty();
        }
        finally { finished.TrySetResult(); }
        await pending;
        await Assert.That(output.All(d => d.Kind == DispositionKind.SettledByHandler)).IsTrue();
        var process = capture.Stopped.Last();
        await Assert.That(process.GetTagItem("messaging.batch.message_count")).IsEqualTo(2);
        await Assert.That(process.Status).IsEqualTo(poison ? ActivityStatusCode.Error : ActivityStatusCode.Unset);
        await Assert.That(capture.Stopped.Count).IsEqualTo(3);
        await Assert.That(capture.Stopped.Take(2).All(a => a.ParentSpanId == process.SpanId)).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Batch_retry_recovery_clears_status_but_cancellation_retains_unrecovered_failure(bool cancel)
    {
        using var capture = new Capture();
        await using var provider = BatchProvider();
        using var cancellation = new CancellationTokenSource();
        var attempts = 0;
        provider.GetRequiredService<BatchControl>().Handle = _ =>
        {
            if (++attempts == 1) throw new ApplicationException("retry");
            if (cancel) { cancellation.Cancel(); return ValueTask.FromCanceled<BatchResult>(cancellation.Token); }
            return new(BatchResult.Success);
        };
        var failures = new FailurePipeline(provider, "test", "orders", "billing", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), failures);
        var pipeline = builder.Tracing(capture.Tracing, Endpoint).BuildBatch(
            provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<int, BatchHandler>(), BatchDefinition(provider));
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 4);
        if (cancel)
            await Assert.That(async () => await receive.ProcessBatchAsync(new Incoming[] { new() }, new Disposition[1], cancellation.Token)).Throws<OperationCanceledException>();
        else await receive.ProcessBatchAsync(new Incoming[] { new() }, new Disposition[1]);
        await Assert.That(attempts).IsEqualTo(2);
        await Assert.That(capture.Stopped.Single().Status).IsEqualTo(cancel ? ActivityStatusCode.Error : ActivityStatusCode.Unset);
    }

    private static ServiceProvider BatchProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<BatchControl>();
        services.AddSingleton<BatchHandler>();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddSingleton<ITransport>(new InMemoryTransport("test", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = new() }));
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<int>("number");
            r.Handler<BatchHandler>();
            r.AddTransport<BatchOptions, BatchOptions, BatchOptions>("test", transport =>
                transport.Consume("orders", consumer => consumer.Handler<BatchHandler>().Batch(4, TimeSpan.FromMilliseconds(100))));
        });
        return services.BuildServiceProvider();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Hosted_in_memory_transport_traces_send_process_and_core_or_manual_settlement(int mode)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddSingleton<Handler>();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<int>("number");
            r.InMemory("local", transport =>
            {
                transport.Send<int>("orders");
                transport.Consume("orders", consumer => consumer.Handler<Handler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<Control>().Handle = context =>
        {
            if (mode == 2) throw new ApplicationException("poison");
            if (mode == 1)
            {
                var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
                native.Message.Complete();
                native.Disposition = Disposition.SettledByHandler;
            }
            return default;
        };
        var tracing = provider.GetRequiredService<MessageTracing>();
        var stopped = new ConcurrentQueue<Activity>();
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                stopped.Enqueue(activity);
                if (activity.OperationName == "process orders") processed.TrySetResult();
            },
        };
        ActivitySource.AddActivityListener(listener);
        var hosted = provider.GetServices<IHostedService>().ToArray();
        try
        {
            foreach (var service in hosted) await service.StartAsync(default);
            await using var scope = provider.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(1);
            await processed.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            for (var i = hosted.Length - 1; i >= 0; i--) await hosted[i].StopAsync(default);
        }
        var send = stopped.Single(a => a.OperationName == "send orders");
        var process = stopped.Single(a => a.OperationName == "process orders");
        var settlement = stopped.Single(a => a.GetTagItem("messaging.operation.type") as string == "settle");
        await Assert.That(process.Links.Single().Context.SpanId).IsEqualTo(send.SpanId);
        await Assert.That(settlement.ParentSpanId).IsEqualTo(process.SpanId);
        await Assert.That(settlement.OperationName).IsEqualTo(mode == 2 ? "dead_letter orders" : "complete orders");
        await Assert.That(process.Status).IsEqualTo(mode == 2 ? ActivityStatusCode.Error : ActivityStatusCode.Unset);
        await Assert.That(process.GetTagItem("messaging.destination.partition.id")).IsEqualTo("0");
        await Assert.That(stopped.Count).IsEqualTo(3);
    }

    [Test]
    public async Task Batch_settlement_error_logs_native_span_once_without_reentering_failure_handling()
    {
        var logger = new Logger();
        using var capture = new Capture(logger: logger);
        await using var provider = BatchProvider();
        var failureCalls = 0;
        var complete = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "complete")
            .WrapSettlement(static _ => ValueTask.FromException(new ApplicationException("native")));
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), (_, _, _) => { failureCalls++; return default; });
        var pipeline = builder.Tracing(capture.Tracing, Endpoint).Settle(complete).BuildBatch(
            provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<int, BatchHandler>(), BatchDefinition(provider));
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new("cloudEvents:"), 4);
        await Assert.That(async () => await receive.ProcessBatchAsync(new Incoming[] { new(), new() }, new Disposition[2])).Throws<ApplicationException>();
        await Assert.That(failureCalls).IsEqualTo(0);
        await Assert.That(logger.Entries.Single().SpanId).IsEqualTo(capture.Stopped[0].SpanId.ToString());
        await Assert.That(capture.Stopped.Count).IsEqualTo(2);
        await Assert.That(capture.Stopped.All(a => a.Status == ActivityStatusCode.Error)).IsTrue();
    }
    private static IConsumerDefinition BatchDefinition(ServiceProvider provider) => provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
    private static ConsumePipelineBuilder BatchBuilder(ServiceProvider provider) => new(provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error));
    public sealed class BatchOptions;
    public sealed class BatchControl { public Func<Batch<int>, ValueTask<BatchResult>>? Handle { get; set; } }
    public sealed class BatchHandler(BatchControl control) : IBatchHandler<int>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<int> batch, CancellationToken cancellationToken) => control.Handle?.Invoke(batch) ?? new(BatchResult.Success);
    }

    private static ServiceProvider Provider(Action<ServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<int>("number"); r.Handler<Handler>(); });
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static ConsumePipelineBuilder Builder(ServiceProvider provider, ConsumeFailureDelegate? failure = null) =>
        new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(), provider.GetRequiredService<MessageTypeRegistry>(),
                new Serializer(), failure ?? (static (_, _, _) => default))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<int, Handler>());

    private static ConsumeMessage Message() => new() { MessageId = "delivery", MessageType = new("number", 1), Body = default };
    public sealed class Control { public ConsumeDelegate? Handle { get; set; } }
    public sealed class Handler(Control control) : IHandler<int>
    {
        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken) => control.Handle?.Invoke(context) ?? default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "test/cached";
        public T Deserialize<T>(ReadOnlySequence<byte> source) => (T)(object)1;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
    }
    private sealed class Settled;
    private sealed class DisposalGate : IAsyncDisposable
    {
        internal Activity? Activity;
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask DisposeAsync() { Activity = System.Diagnostics.Activity.Current; Entered.SetResult(); return new(Finished.Task); }
    }
    private sealed class DisposalFailure(Exception error) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.FromException(error);
    }
    private sealed class Incoming
    {
        internal bool MissingType;
        internal bool FailRecordedReads;
        internal bool FailTracingReads;
        internal int Enrichments;
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => default;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static string? GetPartitionId(Incoming message) => message.FailTracingReads ? throw new InvalidOperationException("Tracing read") : "3";
        public static void EnrichActivity(Activity activity, Incoming message) { message.Enrichments++; activity.SetTag("messaging.kafka.offset", 42L); }
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            if (name == "cloudEvents:type" && message.MissingType) { value = default; return false; }
            if (name == "traceparent" && message.FailTracingReads) throw new InvalidOperationException("Tracing read");
            if (name is "cloudEvents:source" or "cloudEvents:correlationid" && message.FailRecordedReads) throw new InvalidOperationException("Recorded read");
            value = name switch
            {
                "cloudEvents:id" => new("native"), "cloudEvents:type" => new("number;v=1"),
                "cloudEvents:source" => new("/shop"), "cloudEvents:correlationid" => new("conversation"),
                "traceparent" => new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"),
                _ => default,
            };
            return !value.IsEmpty;
        }
    }
    private sealed class Capture : IDisposable
    {
        internal MessageTracing Tracing { get; }
        internal List<Activity> Stopped { get; } = [];
        internal Dictionary<string, object?> SamplingTags { get; } = [];
        internal List<ActivityLink> SamplingLinks { get; } = [];
        private readonly ActivityListener? _listener;
        internal Capture(TracingOptions? options = null, ActivitySamplingResult? sampling = ActivitySamplingResult.AllDataAndRecorded, Logger? logger = null, IActivityEnricher? enricher = null)
        {
            Tracing = new(Options.Create(options ?? new()), enricher is null ? [] : [enricher], logger is null ? null : new LoggerFactory(logger));
            if (sampling is null) return;
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, Tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> creation) =>
                {
                    SamplingTags.Clear();
                    foreach (var tag in creation.Tags!) SamplingTags[tag.Key] = tag.Value;
                    SamplingLinks.Clear();
                    if (creation.Links is not null) SamplingLinks.AddRange(creation.Links);
                    return sampling.Value;
                },
                ActivityStopped = activity => Stopped.Add(activity),
            };
            ActivitySource.AddActivityListener(_listener);
        }
        public void Dispose() { _listener?.Dispose(); Tracing.Dispose(); }
    }
    private sealed class Enricher : IActivityEnricher
    {
        internal int Calls;
        internal object? MessageId;
        internal object? Offset;
        public void Enrich(Activity activity)
        {
            Calls++;
            MessageId = activity.GetTagItem("messaging.message.id");
            Offset = activity.GetTagItem("messaging.kafka.offset");
        }
    }
    private sealed class Logger : ILogger
    {
        internal List<(Exception? Error, string? TraceId, string? SpanId)> Entries { get; } = [];
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> formatter)
        {
            var values = ((IEnumerable<KeyValuePair<string, object?>>)(object)state!).ToDictionary();
            Entries.Add((error, (string?)values["TraceId"], (string?)values["SpanId"]));
        }
    }
    private sealed class LoggerFactory(Logger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }
}
