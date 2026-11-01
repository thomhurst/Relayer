using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Tests;

public class FailurePipelineTests
{
    [Test]
    public async Task Retries_discard_failed_sends_and_flush_only_success_with_fresh_scopes()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        var attempts = new List<int>();
        var scopes = new List<IServiceProvider>();
        control.Invoke = async context =>
        {
            attempts.Add(context.Attempt);
            var sender = context.Services.GetRequiredService<ISender>();
            scopes.Add(context.Services);
            await sender.SendNowAsync(new Payload(), options =>
            {
                options.Headers["attempt"] = context.Attempt.ToString();
                options.ConfigureNative<Native>(native => native.Attempt = context.Attempt);
            });
            await Assert.That(transport.Sends.Count).IsEqualTo(0);
            if (context.Attempt < 3) throw new InvalidOperationException("try again");
        };
        var pipeline = Pipeline(provider, new() { RetryDelays = new TimeSpan[2], Jitter = 0 });
        await Assert.That((await pipeline.ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(string.Join(",", attempts)).IsEqualTo("1,2,3");
        await Assert.That(scopes[0]).IsNotSameReferenceAs(scopes[1]);
        await Assert.That(transport.Sends.Count).IsEqualTo(1);
        await Assert.That(transport.Sends[0].Headers["attempt"]).IsEqualTo("3");
        await Assert.That(transport.Sends[0].NativeAttempt).IsEqualTo(3);
        // Pool reuse must not replay prior buffered entries or metadata.
        control.Invoke = static _ => default;
        await pipeline.ProcessAsync(new());
        await Assert.That(transport.Sends.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(-1, DispositionKind.Redeliver)]
    [Arguments(0, DispositionKind.Redeliver)]
    [Arguments(1, DispositionKind.Complete)]
    public async Task Exact_native_redelivery_boundary(long ticks, DispositionKind expected)
    {
        var delay = TimeSpan.FromSeconds(10);
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = delay, MaxDeliveryDelay = delay + delay } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        var result = await Pipeline(provider, new() { RedeliveryDelays = new[] { delay + TimeSpan.FromTicks(ticks) }, Jitter = 0 }, Copy).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(expected);
        await Assert.That(transport.Sends.Count).IsEqualTo(expected == DispositionKind.Complete ? 1 : 0);
        if (expected == DispositionKind.Redeliver)
        {
            await Assert.That(result.Delay).IsEqualTo(delay + TimeSpan.FromTicks(ticks));
            await Assert.That(result.Headers.Length).IsEqualTo(3);
        }
    }

    [Test]
    [Arguments(0, false)]
    [Arguments(1, true)]
    public async Task Exact_scheduled_copy_boundary_then_durable_scheduler(long ticks, bool outbox)
    {
        var delay = TimeSpan.FromSeconds(10);
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxDeliveryDelay = delay } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        var scheduler = new Scheduler();
        var pipeline = Pipeline(provider, new() { RedeliveryDelays = new[] { delay + TimeSpan.FromTicks(ticks) }, Jitter = 0 }, Copy, scheduler);
        var result = await pipeline.ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(scheduler.Copies.Count).IsEqualTo(outbox ? 1 : 0);
        var copy = outbox ? scheduler.Copies.Single() : transport.Sends.Single();
        await Assert.That(copy.Destination).IsEqualTo("source");
        await Assert.That(copy.Headers["ce_id"]).IsEqualTo("original");
        await Assert.That(copy.Headers["traceparent"]).IsEqualTo("creation-context");
        await Assert.That(copy.Headers["custom"]).IsEqualTo("preserved");
        await Assert.That(copy.Key).IsEqualTo("ordering-key");
        await Assert.That(copy.Body).IsEqualTo("{}");
        await Assert.That(copy.DeliverAt).IsNotNull();
        await Assert.That(copy.Headers[FailureHeaders.Attempts]).IsEqualTo("1");
        await Assert.That(copy.Headers[FailureHeaders.Redeliveries]).IsEqualTo("1");
        await Assert.That(transport.CapabilityReads).IsEqualTo(2);
    }

    [Test]
    public async Task Metadata_survives_redelivery_and_exhausted_policy_dead_letters_every_attempt()
    {
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = TimeSpan.Zero } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        var pipeline = Pipeline(provider, new() { RetryDelays = new[] { TimeSpan.Zero }, RedeliveryDelays = new[] { TimeSpan.Zero } });
        var first = await pipeline.ProcessAsync(new());
        await Assert.That(first.Kind).IsEqualTo(DispositionKind.Redeliver);
        var next = new Incoming();
        foreach (var pair in first.Headers.Span) next.Headers[pair.Key] = pair.Value.ToString();
        var final = await pipeline.ProcessAsync(next);
        await Assert.That(final.Kind).IsEqualTo(DispositionKind.DeadLetter);
        var headers = final.DeadLetter.Headers.ToArray().ToDictionary(p => p.Key, p => p.Value.ToString());
        await Assert.That(headers[FailureHeaders.Attempts]).IsEqualTo("4");
        using var history = JsonDocument.Parse(headers[FailureHeaders.History]);
        var root = history.RootElement;
        await Assert.That(root.GetProperty("OriginalMessageId").GetString()).IsEqualTo("original");
        await Assert.That(root.GetProperty("Consumer").GetString()).IsEqualTo("consumer");
        await Assert.That(root.GetProperty("Host").GetString()).IsEqualTo(Environment.MachineName);
        var attempts = root.GetProperty("Attempts");
        await Assert.That(attempts.GetArrayLength()).IsEqualTo(4);
        for (var i = 0; i < 4; i++)
        {
            await Assert.That(attempts[i].GetProperty("Attempt").GetInt32()).IsEqualTo(i + 1);
            await Assert.That(attempts[i].GetProperty("ExceptionType").GetString()).IsEqualTo(typeof(InvalidOperationException).FullName);
            await Assert.That(attempts[i].GetProperty("Message").GetString()).IsEqualTo("failed attempt");
            await Assert.That(attempts[i].GetProperty("StackTrace").GetString()).IsNotNull();
            await Assert.That(attempts[i].GetProperty("Time").GetDateTimeOffset() <= DateTimeOffset.UtcNow).IsTrue();
        }
    }

    [Test]
    [Arguments("ce_type")]
    [Arguments("ce_id")]
    [Arguments(FailureHeaders.Attempts)]
    public async Task Malformed_poison_metadata_dead_letters_without_retry_or_stalling(string header)
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        var pipeline = Pipeline(provider, new() { RetryDelays = new TimeSpan[2] });
        var incoming = new Incoming();
        if (header == FailureHeaders.Attempts) incoming.Headers[header] = "invalid";
        else incoming.Headers.Remove(header);
        var result = await pipeline.ProcessAsync(incoming);
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(0);
        await Assert.That((await pipeline.ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    public async Task Classifier_discard_bypasses_retry_and_discards_buffered_sends()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            await context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
            throw new ArgumentException("discard");
        };
        var classifier = new FailureClassifierBuilder().On<ArgumentException>(FailureAction.Discard).Build();
        var result = await Pipeline(provider, new() { RetryDelays = new TimeSpan[2] }, classifier: classifier).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(transport.Sends.Count).IsEqualTo(0);
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(1);
    }

    [Test]
    public async Task Dead_letter_copy_is_explicit_and_preserves_original_headers_and_body()
    {
        var transport = new Transport { Capabilities = new() };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        var result = await Pipeline(provider, new() { DeadLetterEndpoint = "errors" }, Copy).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.Complete);
        var copy = transport.Sends.Single();
        await Assert.That(copy.Destination).IsEqualTo("errors");
        await Assert.That(copy.DeliverAt).IsNull();
        await Assert.That(copy.Headers["ce_id"]).IsEqualTo("original");
        await Assert.That(copy.Headers["custom"]).IsEqualTo("preserved");
        await Assert.That(copy.Headers.ContainsKey(FailureHeaders.History)).IsTrue();
        await Assert.That(transport.CapabilityReads).IsEqualTo(3);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Pending_and_failed_fallback_never_returns_completion(bool durableScheduler)
    {
        var transport = new Transport { Capabilities = new() { MaxDeliveryDelay = TimeSpan.FromSeconds(10) } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Pending = gate.Task;
        var scheduler = new Scheduler { Pending = gate.Task };
        var options = new FailureOptions { DeadLetterEndpoint = "errors", RedeliveryDelays = durableScheduler ? new[] { TimeSpan.FromMinutes(1) } : default };
        var pipeline = Pipeline(provider, options, Copy, scheduler);
        var pending = pipeline.ProcessAsync(new());
        await Assert.That(pending.IsCompleted).IsFalse();
        gate.SetException(new ApplicationException("durable write failed"));
        await Assert.That(async () => await pending).Throws<ApplicationException>();
    }

    [Test]
    public async Task Flush_failure_propagates_without_retry_or_dead_letter_after_partial_send()
    {
        var transport = new Transport { FailAfter = 1 };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            var sender = context.Services.GetRequiredService<ISender>();
            await sender.SendAsync(new Payload());
            await sender.SendAsync(new Payload());
        };
        var pipeline = Pipeline(provider, new() { RetryDelays = new TimeSpan[2] });
        await Assert.That(async () => await pipeline.ProcessAsync(new())).Throws<ApplicationException>();
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(1);
        await Assert.That(transport.Sends.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Retry_backoff_is_jittered_and_cancellation_does_not_settle()
    {
        var time = new ControlledTime();
        var transport = new Transport();
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        using var cancellation = new CancellationTokenSource();
        var pipeline = Pipeline(provider, new() { RetryDelays = new[] { TimeSpan.FromSeconds(10) }, Jitter = .2 }, time: time);
        var pending = pipeline.ProcessAsync(new(), cancellation.Token);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(time.Delay >= TimeSpan.FromSeconds(8) && time.Delay <= TimeSpan.FromSeconds(10)).IsTrue();
        cancellation.Cancel();
        await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(1);
        await Assert.That(transport.Sends.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Startup_rejects_missing_executors_even_when_capability_limits_exist()
    {
        var transport = new Transport { Capabilities = new() { MaxDeliveryDelay = TimeSpan.FromHours(1) } };
        using var provider = Provider(transport);
        await Assert.That(() => new FailurePipeline(provider, "test", "source", "consumer"))
            .Throws<InvalidOperationException>().WithMessageContaining("DeadLetterEndpoint");
        transport.Capabilities = transport.Capabilities with { DeadLetter = true };
        // A fresh provider has a fresh capability cache; no mutable global state.
        using var other = Provider(transport);
        await Assert.That(() => new FailurePipeline(other, "test", "source", "consumer", new() { RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) } }))
            .Throws<InvalidOperationException>().WithMessageContaining("copy executor");
        using var third = Provider(new Transport());
        await Assert.That(() => new FailurePipeline(third, "test", "source", "consumer", new() { RedeliveryDelays = new[] { TimeSpan.FromDays(1) } }, Copy))
            .Throws<InvalidOperationException>().WithMessageContaining("IFailureScheduler");
    }

    [Test]
    public async Task Startup_validates_longest_delay_and_reuses_endpoint_validator()
    {
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = TimeSpan.FromSeconds(5) } };
        using var provider = Provider(transport);
        await Assert.That(() => new FailurePipeline(provider, "test", "source", "consumer", new() { RedeliveryDelays = new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(6) } }))
            .Throws<InvalidOperationException>().WithMessageContaining("source");
        using var invalid = Provider(new Transport { Capabilities = new() { DeadLetter = true, MaxBatchSize = 0 } });
        await Assert.That(() => new FailurePipeline(invalid, "test", "source", "consumer"))
            .Throws<InvalidOperationException>().WithMessageContaining("MaxBatchSize");
    }

    [Test]
    public async Task Concurrent_attempts_keep_senders_isolated_and_pool_handles_more_than_one_segment()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var senders = new HashSet<ISender>(ReferenceEqualityComparer.Instance);
        control.Invoke = context =>
        {
            senders.Add(context.Services.GetRequiredService<ISender>());
            return new(gate.Task);
        };
        var pipeline = Pipeline(provider);
        var tasks = new Task<Disposition>[70];
        for (var i = 0; i < tasks.Length; i++) tasks[i] = pipeline.ProcessAsync(new()).AsTask();
        await Assert.That(senders.Count).IsEqualTo(70);
        gate.SetResult();
        await Task.WhenAll(tasks);
        // Return and rent again after every sender slot is occupied.
        control.Invoke = null;
        await Assert.That((await pipeline.ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    public async Task Scope_disposal_must_succeed_before_buffered_sends_escape()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        control.Invoke = context => context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
        control.Dispose = static () => ValueTask.FromException(new ApplicationException("scope failed"));
        var pipeline = Pipeline(provider);
        await Assert.That(async () => await pipeline.ProcessAsync(new())).Throws<ApplicationException>();
        await Assert.That(transport.Sends.Count).IsEqualTo(0);
        control.Dispose = null;
        await pipeline.ProcessAsync(new());
        await Assert.That(transport.Sends.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    public async Task Disposal_sends_escape_only_when_handler_and_scope_succeed(bool failHandler, bool failDisposal)
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        ISender sender = null!;
        control.Invoke = context =>
        {
            sender = context.Services.GetRequiredService<ISender>();
            return failHandler ? Throw(context) : default;
        };
        control.Dispose = async () =>
        {
            await sender.SendNowAsync(new Payload());
            await Assert.That(transport.Sends.Count).IsEqualTo(0);
            if (failDisposal) throw new ApplicationException("scope failed after sending");
        };
        var pipeline = Pipeline(provider);
        if (failDisposal)
            await Assert.That(async () => await pipeline.ProcessAsync(new())).Throws<ApplicationException>();
        else
            await Assert.That((await pipeline.ProcessAsync(new())).Kind)
                .IsEqualTo(failHandler ? DispositionKind.DeadLetter : DispositionKind.Complete);
        await Assert.That(transport.Sends.Count).IsEqualTo(failHandler || failDisposal ? 0 : 1);

        control.Invoke = null;
        control.Dispose = null;
        await pipeline.ProcessAsync(new());
        await Assert.That(transport.Sends.Count).IsEqualTo(failHandler || failDisposal ? 0 : 1);
        await using var scope = provider.CreateAsyncScope();
        var reused = scope.ServiceProvider.GetRequiredService<ISender>();
        await Assert.That(reused).IsSameReferenceAs(sender);
        await reused.SendAsync(new Payload());
        await Assert.That(transport.Sends.Count).IsEqualTo(failHandler || failDisposal ? 1 : 2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Sender_stays_reserved_until_every_disposer_finishes(bool failDisposal)
    {
        var transport = new Transport();
        using var provider = Provider(transport, services => services.AddScoped<IOutboxEnlistment>(scope =>
        {
            // Sender resolves its outbox first, so DI disposes Sender before Lifetime.
            _ = scope.GetRequiredService<Lifetime>();
            return new InactiveOutbox();
        }));
        var control = provider.GetRequiredService<Control>();
        var disposing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ISender sender = null!;
        control.Invoke = context =>
        {
            sender = context.Services.GetRequiredService<ISender>();
            return default;
        };
        control.Dispose = async () =>
        {
            disposing.SetResult();
            await finish.Task;
            await sender.SendAsync(new Payload());
            if (failDisposal) throw new ApplicationException("scope failed after sending");
        };
        var pipeline = Pipeline(provider);
        var pending = pipeline.ProcessAsync(new());
        await disposing.Task;
        // The other scope also owns a Lifetime. Only the suspended disposer uses this callback.
        control.Dispose = null;
        try
        {
            await Assert.That(pending.IsCompleted).IsFalse();
            await using var other = provider.CreateAsyncScope();
            var otherSender = other.ServiceProvider.GetRequiredService<ISender>();
            await Assert.That(otherSender).IsNotSameReferenceAs(sender);
            await otherSender.SendAsync(new Payload());
            await Assert.That(transport.Sends.Count).IsEqualTo(1);
        }
        finally
        {
            finish.SetResult();
        }
        if (failDisposal)
            await Assert.That(async () => await pending).Throws<ApplicationException>();
        else
            await Assert.That((await pending).Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(transport.Sends.Count).IsEqualTo(failDisposal ? 1 : 2);
    }

    [Test]
    public async Task Successful_processing_adds_no_allocations_beyond_equivalent_native_DI_and_JSON()
    {
        var transport = new Transport();
        using var provider = Provider(transport, static services =>
        {
            // DI compiles constructor call sites in the background. Factories avoid temporary argument
            // arrays in its interpreter, so compilation timing cannot change either allocation sample.
            services.AddScoped(static scope => new Lifetime(scope.GetRequiredService<Control>()));
            services.AddScoped(static scope => new Handler(scope.GetRequiredService<Control>(), scope.GetRequiredService<Lifetime>()));
        });
        var pipeline = Pipeline(provider);
        var incoming = new Incoming();
        var serializer = provider.GetRequiredService<Relayer.Serialization.IMessageSerializer>();
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        for (var i = 0; i < 1000; i++)
        {
            await pipeline.ProcessAsync(incoming);
            var scope = new AsyncServiceScope(scopes.CreateScope());
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
            _ = scope.ServiceProvider.GetRequiredService<Handler>();
            _ = serializer.Deserialize<Payload>(new(incoming.Body));
            await scope.DisposeAsync();
        }
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            var scope = new AsyncServiceScope(scopes.CreateScope());
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
            _ = scope.ServiceProvider.GetRequiredService<Handler>();
            _ = serializer.Deserialize<Payload>(new(incoming.Body));
            await scope.DisposeAsync();
        }
        var nativeBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await pipeline.ProcessAsync(incoming);
        var pipelineBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        await Assert.That(pipelineBytes).IsEqualTo(nativeBytes);
    }

    [Test]
    public async Task A_completed_backoff_retries_then_processes_successfully()
    {
        var time = new ControlledTime();
        var transport = new Transport();
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        control.Invoke = context => context.Attempt == 1 ? Throw(context) : default;
        var pipeline = Pipeline(provider, new() { RetryDelays = new[] { TimeSpan.FromSeconds(10) }, Jitter = 0 }, time: time);
        var pending = pipeline.ProcessAsync(new());
        await Assert.That(control.Calls).IsEqualTo(1);
        await Assert.That(time.Delay).IsEqualTo(TimeSpan.FromSeconds(10));
        time.Fire();
        await Assert.That((await pending).Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(control.Calls).IsEqualTo(2);
    }

    [Test]
    public async Task Scheduled_copy_preserves_native_configuration_but_outbox_rejects_it()
    {
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxDeliveryDelay = TimeSpan.FromSeconds(1) } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = Throw;
        static OutgoingMessage NativeCopy(MessageContext context)
        {
            var original = Copy(context);
            var options = new SendOptions().ConfigureNative<Native>(static native => native.Attempt = 42);
            return new(original.Destination, original.Body, original.Headers, original.OrderingKey, options: options);
        }
        var pipeline = Pipeline(provider, new() { RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) } }, NativeCopy);
        await pipeline.ProcessAsync(new());
        await Assert.That(transport.Sends.Single().NativeAttempt).IsEqualTo(42);
        var scheduler = new Scheduler();
        var outbox = Pipeline(provider, new() { RedeliveryDelays = new[] { TimeSpan.FromSeconds(2) } }, NativeCopy, scheduler);
        await Assert.That(async () => await outbox.ProcessAsync(new())).Throws<InvalidOperationException>().WithMessageContaining("Native configurators");
        await Assert.That(scheduler.Copies.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Corrupt_history_does_not_prevent_dead_lettering()
    {
        using var provider = Provider(new Transport());
        provider.GetRequiredService<Control>().Invoke = Throw;
        var pipeline = Pipeline(provider);
        var incoming = new Incoming();
        incoming.Headers[FailureHeaders.History] = "not-json";
        var result = await pipeline.ProcessAsync(incoming);
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.DeadLetter);
    }

    [Test]
    public async Task Concurrent_sends_in_one_attempt_keep_distinct_payloads_and_options()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            var sender = context.Services.GetRequiredService<ISender>();
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sends = Enumerable.Range(0, 70).Select(async index =>
            {
                await gate.Task;
                await sender.SendAsync(new Payload(), options => options.ConfigureNative<Native>(native => native.Attempt = index));
            }).ToArray();
            gate.SetResult();
            await Task.WhenAll(sends);
        };
        var pipeline = Pipeline(provider);
        for (var iteration = 0; iteration < 2; iteration++)
        {
            transport.Sends.Clear();
            await pipeline.ProcessAsync(new());
            await Assert.That(transport.Sends.Count).IsEqualTo(70);
            await Assert.That(transport.Sends.Select(s => s.NativeAttempt).Distinct().Count()).IsEqualTo(70);
            await Assert.That(transport.Sends.Select(s => s.Headers["ce_id"]).Distinct().Count()).IsEqualTo(70);
        }
    }

    [Test]
    public async Task Cancelled_flush_leaves_no_settlement_and_buffer_can_be_reused()
    {
        var transport = new Transport { Pending = new TaskCompletionSource().Task };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = context => context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
        var pipeline = Pipeline(provider);
        using var cancellation = new CancellationTokenSource();
        var pending = pipeline.ProcessAsync(new(), cancellation.Token);
        await Assert.That(pending.IsCompleted).IsFalse();
        cancellation.Cancel();
        await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        transport.Pending = null;
        await pipeline.ProcessAsync(new());
        await Assert.That(transport.Sends.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(DispositionKind.Complete)]
    [Arguments(DispositionKind.Redeliver)]
    [Arguments(DispositionKind.DeadLetter)]
    public async Task Manual_requests_apply_only_after_success_and_reset_on_pool_reuse(DispositionKind kind)
    {
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = TimeSpan.FromMinutes(1) } };
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        ISettlementFeature? feature = null;
        control.Invoke = context =>
        {
            feature = context.Features.GetRequired<ISettlementFeature>();
            Request(feature, kind);
            return default;
        };
        var pipeline = Pipeline(provider);
        var result = await pipeline.ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(kind);
        if (kind == DispositionKind.Redeliver) await Assert.That(result.Delay).IsEqualTo(TimeSpan.FromSeconds(5));
        if (kind == DispositionKind.DeadLetter)
        {
            await Assert.That(result.DeadLetter.Reason).IsEqualTo("invalid order");
            var headers = result.DeadLetter.Headers.ToArray().ToDictionary(p => p.Key, p => p.Value.ToString());
            await Assert.That(headers[FailureHeaders.DeadLetterReason]).IsEqualTo("invalid order");
        }
        await Assert.That(() => feature!.Complete()).Throws<InvalidOperationException>();
        control.Invoke = static _ => default;
        await Assert.That((await pipeline.ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    [Arguments(DispositionKind.Complete)]
    [Arguments(DispositionKind.Redeliver)]
    [Arguments(DispositionKind.DeadLetter)]
    public async Task Handler_failure_overrides_request_and_retry_starts_without_request(DispositionKind kind)
    {
        using var provider = Provider(new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = TimeSpan.FromMinutes(1) } });
        var control = provider.GetRequiredService<Control>();
        control.Invoke = context =>
        {
            if (context.Attempt == 1)
            {
                Request(context.Features.GetRequired<ISettlementFeature>(), kind);
                throw new InvalidOperationException("failed after requesting settlement");
            }
            return default;
        };
        var result = await Pipeline(provider, new() { RetryDelays = new[] { TimeSpan.Zero } }).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That(control.Calls).IsEqualTo(2);
        control.Invoke = context =>
        {
            Request(context.Features.GetRequired<ISettlementFeature>(), kind);
            throw new InvalidOperationException("always fails");
        };
        result = await Pipeline(provider).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(result.DeadLetter.Reason).IsEqualTo("Processing failed");
    }

    [Test]
    [Arguments(DispositionKind.Complete)]
    [Arguments(DispositionKind.Redeliver)]
    [Arguments(DispositionKind.DeadLetter)]
    public async Task Manual_complete_flushes_sends_but_retry_and_dead_letter_discard_them(DispositionKind kind)
    {
        var transport = new Transport { Capabilities = new() { DeadLetter = true, MaxRedeliveryDelay = TimeSpan.FromMinutes(1) } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            await context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
            Request(context.Features.GetRequired<ISettlementFeature>(), kind);
        };
        var result = await Pipeline(provider).ProcessAsync(new());
        await Assert.That(result.Kind).IsEqualTo(kind);
        await Assert.That(transport.Sends.Count).IsEqualTo(kind == DispositionKind.Complete ? 1 : 0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Manual_fallback_must_be_durable_before_completion(bool retry)
    {
        var transport = new Transport { Capabilities = new() { MaxDeliveryDelay = TimeSpan.FromMinutes(1) } };
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            Request(context.Features.GetRequired<ISettlementFeature>(), retry ? DispositionKind.Redeliver : DispositionKind.DeadLetter);
            return default;
        };
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.Pending = gate.Task;
        var pipeline = Pipeline(provider, new() { DeadLetterEndpoint = "errors" }, Copy);
        var pending = pipeline.ProcessAsync(new());
        await Assert.That(pending.IsCompleted).IsFalse();
        gate.SetResult();
        await Assert.That((await pending).Kind).IsEqualTo(DispositionKind.Complete);
        var copy = transport.Sends.Single();
        await Assert.That(copy.Destination).IsEqualTo(retry ? "source" : "errors");
        await Assert.That(copy.Headers["ce_id"]).IsEqualTo("original");
        if (!retry) await Assert.That(copy.Headers[FailureHeaders.DeadLetterReason]).IsEqualTo("invalid order");
        transport.Pending = Task.FromException(new ApplicationException("copy failed"));
        await Assert.That(async () => await pipeline.ProcessAsync(new())).Throws<ApplicationException>();
    }

    [Test]
    public async Task Invalid_and_duplicate_requests_fail_without_replacing_valid_request()
    {
        using var provider = Provider(new Transport());
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            var settlement = context.Features.GetRequired<ISettlementFeature>();
            await Assert.That(() => settlement.Retry(TimeSpan.FromTicks(-1))).Throws<ArgumentOutOfRangeException>();
            await Assert.That(() => settlement.DeadLetter(" ")).Throws<ArgumentException>();
            await Assert.That(() => settlement.Retry(TimeSpan.FromSeconds(1))).Throws<InvalidOperationException>();
            settlement.Complete();
            await Assert.That(() => settlement.DeadLetter("later request")).Throws<InvalidOperationException>();
        };
        await Assert.That((await Pipeline(provider).ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Manual_completion_waits_for_disposal_and_never_settles_failed_durability(bool failDisposal)
    {
        var transport = new Transport { FailAfter = failDisposal ? int.MaxValue : 0 };
        using var provider = Provider(transport);
        var control = provider.GetRequiredService<Control>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Dispose = () => new(gate.Task);
        control.Invoke = async context =>
        {
            context.Features.GetRequired<ISettlementFeature>().Complete();
            await context.Services.GetRequiredService<ISender>().SendAsync(new Payload());
        };
        var pending = Pipeline(provider).ProcessAsync(new());
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(transport.Sends.Count).IsEqualTo(0);
        if (failDisposal) gate.SetException(new ApplicationException("disposal failed"));
        else gate.SetResult();
        await Assert.That(async () => await pending).Throws<ApplicationException>();
        await Assert.That(control.Calls).IsEqualTo(1);
        await Assert.That(transport.Sends.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Manual_requests_are_isolated_across_concurrent_deliveries()
    {
        using var provider = Provider(new Transport());
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            if (context.MessageId == "first")
            {
                context.Features.GetRequired<ISettlementFeature>().DeadLetter("first only");
                await gate.Task;
            }
        };
        var first = new Incoming();
        first.Headers["ce_id"] = "first";
        var pipeline = Pipeline(provider);
        var pending = pipeline.ProcessAsync(first);
        await Assert.That((await pipeline.ProcessAsync(new())).Kind).IsEqualTo(DispositionKind.Complete);
        gate.SetResult();
        await Assert.That((await pending).Kind).IsEqualTo(DispositionKind.DeadLetter);
    }

    [Test]
    public async Task Cancellation_overrides_manual_completion()
    {
        using var provider = Provider(new Transport());
        using var cancellation = new CancellationTokenSource();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            context.Features.GetRequired<ISettlementFeature>().Complete();
            cancellation.Cancel();
            return default;
        };
        await Assert.That(async () => await Pipeline(provider).ProcessAsync(new(), cancellation.Token)).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Manual_retry_uses_durable_scheduler_and_preserves_prior_failure_history()
    {
        var transport = new Transport();
        using var provider = Provider(transport);
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            if (context.Attempt == 1) throw new InvalidOperationException("first failure");
            context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.FromSeconds(5));
            return default;
        };
        var scheduler = new Scheduler();
        await Assert.That((await Pipeline(provider, new() { RetryDelays = new[] { TimeSpan.Zero } }, Copy, scheduler).ProcessAsync(new())).Kind)
            .IsEqualTo(DispositionKind.Complete);
        var copy = scheduler.Copies.Single();
        await Assert.That(copy.Headers[FailureHeaders.Attempts]).IsEqualTo("2");
        await Assert.That(copy.Headers[FailureHeaders.Redeliveries]).IsEqualTo("1");
        using var history = JsonDocument.Parse(copy.Headers[FailureHeaders.History]);
        await Assert.That(history.RootElement.GetProperty("Attempts").GetArrayLength()).IsEqualTo(1);
    }

    private static void Request(ISettlementFeature settlement, DispositionKind kind)
    {
        switch (kind)
        {
            case DispositionKind.Complete: settlement.Complete(); break;
            case DispositionKind.Redeliver: settlement.Retry(TimeSpan.FromSeconds(5)); break;
            case DispositionKind.DeadLetter: settlement.DeadLetter("invalid order"); break;
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static ValueTask Throw(MessageContext context) => throw new InvalidOperationException("failed attempt");

    private static ServiceProvider Provider(Transport transport, Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(transport);
        services.AddSingleton<Control>();
        services.AddScoped<Lifetime>();
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.Handler<Handler>();
            r.DeclareRoute<Payload>("test", "destination");
        });
        configure?.Invoke(services);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ReceivePipeline<Incoming, Binding> Pipeline(ServiceProvider provider, FailureOptions? options = null,
        FailureMessageCopyDelegate? copy = null, IFailureScheduler? scheduler = null,
        IFailureClassifier? classifier = null, TimeProvider? time = null)
    {
        var failure = new FailurePipeline(provider, "test", "source", "consumer", options, copy, scheduler, time);
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<Relayer.Serialization.IMessageSerializer>(), failure, classifier)
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(1);
        return new(pipeline, new HeaderNames(), 1);
    }

    private static OutgoingMessage Copy(MessageContext context)
    {
        var incoming = context.Features.Get<NativeMessage<Incoming>>()!.Message;
        return new("source", incoming.Body, incoming.Headers.Select(p => new KeyValuePair<string, WireString>(p.Key, new(p.Value))).ToArray(), new("ordering-key"));
    }

    [Message("failure.payload")]
    public sealed class Payload;
    public sealed class Control
    {
        public Func<MessageContext, ValueTask>? Invoke { get; set; }
        public int Calls { get; set; }
        public Func<ValueTask>? Dispose { get; set; }
    }
    public sealed class Handler(Control control, Lifetime lifetime) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            _ = lifetime;
            control.Calls++;
            return control.Invoke?.Invoke(context) ?? default;
        }
    }
    public sealed class Lifetime(Control control) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => control.Dispose?.Invoke() ?? default;
    }
    private sealed class InactiveOutbox : IOutboxEnlistment
    {
        public bool IsActive => false;
        public ValueTask EnlistAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
    private sealed class Incoming
    {
        internal byte[] Body { get; init; } = "{}"u8.ToArray();
        internal Dictionary<string, string> Headers { get; } = new()
        {
            ["ce_id"] = "original", ["ce_type"] = "failure.payload;v=1", ["traceparent"] = "creation-context", ["custom"] = "preserved",
        };
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => new(message.Body);
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            var found = message.Headers.TryGetValue(name, out var text);
            value = found ? new(text!) : default;
            return found;
        }
    }
    private sealed class Native { internal int Attempt { get; set; } }
    private sealed record Snapshot(string Destination, string Body, Dictionary<string, string> Headers, string? Key, DateTimeOffset? DeliverAt, int NativeAttempt)
    {
        internal static Snapshot From(OutgoingMessage message)
        {
            var native = new Native();
            message.ApplyNative(native);
            return new(message.Destination, Encoding.UTF8.GetString(message.Body.Span),
                message.Headers.ToArray().ToDictionary(p => p.Key, p => p.Value.ToString()), message.OrderingKey?.ToString(), message.DeliverAt, native.Attempt);
        }
    }
    private sealed class Transport : ITransport
    {
        public string Name => "test";
        internal TransportCapabilities Capabilities { get; set; } = new() { DeadLetter = true };
        internal List<Snapshot> Sends { get; } = [];
        internal int CapabilityReads { get; private set; }
        internal Task? Pending { get; set; }
        internal int FailAfter { get; init; } = int.MaxValue;
        public TransportCapabilities GetCapabilities(string endpoint) { CapabilityReads++; return Capabilities; }
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (Pending is not null) await Pending.WaitAsync(cancellationToken);
            if (Sends.Count >= FailAfter) throw new ApplicationException("partial send failure");
            Sends.Add(Snapshot.From(message));
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
            => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
    private sealed class Scheduler : IFailureScheduler
    {
        internal List<Snapshot> Copies { get; } = [];
        internal Task? Pending { get; init; }
        public async ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (Pending is not null) await Pending.WaitAsync(cancellationToken);
            Copies.Add(Snapshot.From(message));
        }
    }
    private sealed class ControlledTime : TimeProvider
    {
        internal TimeSpan Delay { get; private set; }
        private TimerCallback? _callback;
        private object? _state;
        internal void Fire() => _callback!(_state);
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            Delay = dueTime;
            _callback = callback;
            _state = state;
            return new Timer();
        }
        private sealed class Timer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() { }
            public ValueTask DisposeAsync() => default;
        }
    }
}
