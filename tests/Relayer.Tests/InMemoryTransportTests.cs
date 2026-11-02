using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class InMemoryTransportTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(10);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static InMemoryTransport Transport(InMemoryEndpointOptions? options = null, TimeProvider? clock = null) =>
        new("local", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = options ?? new() }, clock);
    private static ValueTask Send(InMemoryTransport transport, int value, string? key = null, DateTimeOffset? at = null,
        CancellationToken cancellationToken = default) =>
        transport.SendAsync(new("orders", BitConverter.GetBytes(value), orderingKey: key is null ? null : new WireString(key), deliverAt: at), cancellationToken);
    private static int Value(InMemoryDelivery delivery) => BitConverter.ToInt32(delivery.Message.Body.Span);
    private static async Task<int> Next(Channel<int> values) => await values.Reader.ReadAsync().AsTask().WaitAsync(Limit);

    [Test]
    public async Task Send_copies_borrowed_body_headers_and_key_and_applies_native_callback()
    {
        await using var transport = Transport(new() { Capabilities = new() { OrderedByKey = true } });
        var body = new byte[] { 42 };
        var header = Encoding.UTF8.GetBytes("tenant");
        var key = Encoding.UTF8.GetBytes("key");
        var headers = new[] { new KeyValuePair<string, WireString>("tenant", new(header)), new("traceparent", new WireString("original-context")) };
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(native =>
        {
            if (native.Body.Span[0] != 42) throw new InvalidOperationException("Callback did not receive native storage.");
        });
        await transport.SendAsync(new("orders", body, headers, new WireString(key), options: options), default);
        Array.Fill(body, (byte)0);
        Array.Fill(header, (byte)0);
        Array.Fill(key, (byte)0);
        Array.Clear(headers);
        var seen = Signal();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (delivery.Message.Body.Span[0] != 42 || delivery.Message.OrderingKey?.ToString() != "key" ||
                delivery.Message.Headers.Span[0].Value.ToString() != "tenant" || delivery.Message.Headers.Span[1].Value.ToString() != "original-context")
                throw new InvalidOperationException("Borrowed send storage or creation context was retained or overwritten.");
            seen.SetResult();
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await seen.Task.WaitAsync(Limit);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Queue_consumers_compete_and_settle_each_delivery_once()
    {
        await using var transport = Transport();
        var seen = new int[100];
        var finished = Signal();
        var count = 0;
        ValueTask<Disposition> Receive(InMemoryDelivery delivery, CancellationToken _)
        {
            if (Interlocked.Increment(ref seen[Value(delivery)]) != 1) throw new InvalidOperationException("Duplicate native queue delivery.");
            if (Interlocked.Increment(ref count) == seen.Length) finished.SetResult();
            return new(Disposition.Complete);
        }
        transport.RegisterConsumer("orders", Receive, concurrency: 2);
        transport.RegisterConsumer("orders", Receive, concurrency: 2);
        await transport.StartAsync(default);
        for (var i = 0; i < seen.Length; i++) await Send(transport, i);
        await finished.Task.WaitAsync(Limit);
        await transport.StopAsync(default);
        await Assert.That(seen.All(value => value == 1)).IsTrue();
    }

    [Test]
    public async Task Bounded_send_observes_cancellation_and_cancelled_message_is_not_delivered()
    {
        await using var transport = Transport(new() { Capacity = 1 });
        await Send(transport, 1);
        using var cancellation = new CancellationTokenSource();
        var blocked = Send(transport, 2, cancellationToken: cancellation.Token);
        await Assert.That(blocked.IsCompleted).IsFalse();
        cancellation.Cancel();
        await Assert.That(async () => await blocked).Throws<OperationCanceledException>();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(1);
        await Send(transport, 3);
        await Assert.That(await Next(values)).IsEqualTo(3);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Partitioned_logs_fan_out_to_independent_groups_and_commit_only_completed_positions()
    {
        await using var transport = Transport(new() { Capacity = 4, Capabilities = new() { ConsumptionModel = ConsumptionModel.Log } });
        var slowStarted = Signal();
        var slowDone = Signal();
        var slowCount = 0;
        var fastAtThird = Signal();
        var release = Signal();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            slowStarted.TrySetResult();
            await release.Task;
            if (Interlocked.Increment(ref slowCount) == 3) slowDone.SetResult();
            return Disposition.Complete;
        }, "slow");
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            if (Value(delivery) == 3)
            {
                if (delivery.Position != 2 || transport.GetCommittedPosition("orders", "fast", 0) != 1)
                    throw new InvalidOperationException("Log committed an unfinished position or changed order.");
                fastAtThird.SetResult();
                await release.Task;
            }
            return Disposition.Complete;
        }, "fast");
        for (var i = 1; i <= 3; i++) await Send(transport, i);
        await transport.StartAsync(default);
        await Task.WhenAll(slowStarted.Task, fastAtThird.Task).WaitAsync(Limit);
        await Assert.That(transport.GetCommittedPosition("orders", "slow", 0)).IsEqualTo(-1);
        await Assert.That(transport.GetCommittedPosition("orders", "fast", 0)).IsEqualTo(1);
        var stop = transport.StopAsync(default);
        release.SetResult();
        await stop.WaitAsync(Limit);
        await Assert.That(transport.GetCommittedPosition("orders", "fast", 0)).IsEqualTo(2);
        await Assert.That(transport.GetCommittedPosition("orders", "slow", 0)).IsEqualTo(0);
        await transport.StartAsync(default);
        await slowDone.Task.WaitAsync(Limit);
        await transport.StopAsync(default);
        await Assert.That(transport.GetCommittedPosition("orders", "slow", 0)).IsEqualTo(2);
        await Assert.That(transport.GetCommittedPosition("orders", "fast", 0)).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Key_order_is_preserved_across_redelivery_with_parallel_partitions(bool log)
    {
        await using var transport = Transport(new()
        {
            Partitions = 2,
            Capabilities = new() { ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue, OrderedByKey = true, MaxRedeliveryDelay = TimeSpan.Zero },
        });
        var values = Channel.CreateUnbounded<int>();
        var firstStarted = Signal();
        var release = Signal();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            if (Value(delivery) == 1 && delivery.Attempt == 1)
            {
                firstStarted.SetResult();
                await release.Task;
                return Disposition.Redeliver(TimeSpan.Zero);
            }
            values.Writer.TryWrite(Value(delivery) * 10 + delivery.Attempt);
            return Disposition.Complete;
        }, log ? "workers" : null, 2);
        await transport.StartAsync(default);
        await Send(transport, 1, "a");
        await firstStarted.Task.WaitAsync(Limit);
        await Send(transport, 2, "a");
        await Send(transport, 3, "b");
        await Assert.That(await Next(values)).IsEqualTo(31);
        release.SetResult();
        await Assert.That(await Next(values)).IsEqualTo(12);
        await Assert.That(await Next(values)).IsEqualTo(21);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Utf8_and_text_keys_select_the_same_native_partition()
    {
        await using var transport = Transport(new() { Partitions = 3, Capabilities = new() { OrderedByKey = true } });
        var partitions = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { partitions.Writer.TryWrite(delivery.Partition); return new(Disposition.Complete); }, concurrency: 3);
        await transport.StartAsync(default);
        await Send(transport, 1, "東京");
        await transport.SendAsync(new("orders", new byte[] { 1 }, orderingKey: new WireString(Encoding.UTF8.GetBytes("東京"))), default);
        await Assert.That(await Next(partitions)).IsEqualTo(await Next(partitions));
    }

    [Test]
    public async Task Native_dead_letters_preserve_error_metadata_and_do_not_stop_other_messages()
    {
        await using var transport = Transport();
        var seen = Signal();
        var errorBytes = Encoding.UTF8.GetBytes("failure");
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (Value(delivery) == 1) return new(Disposition.DeadLettered(new("invalid", "description", new[] { new KeyValuePair<string, WireString>("error", new(errorBytes)) })));
            seen.SetResult();
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await Send(transport, 2);
        await seen.Task.WaitAsync(Limit);
        var dead = await transport.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(Limit);
        Array.Clear(errorBytes);
        await Assert.That(BitConverter.ToInt32(dead.Body.Span)).IsEqualTo(1);
        await Assert.That(dead.Error.Reason).IsEqualTo("invalid");
        await Assert.That(dead.Error.Headers.Span[0].Value.ToString()).IsEqualTo("failure");
        await Assert.That(dead.Attempt).IsEqualTo(1);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Manual_settlement_is_applied_once_and_redelivery_increments_attempt()
    {
        await using var transport = Transport();
        var done = Signal();
        var calls = 0;
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            calls++;
            if (delivery.Attempt == 1) delivery.Redeliver(TimeSpan.Zero);
            else { delivery.Complete(); done.SetResult(); }
            return new(Disposition.SettledByHandler);
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await done.Task.WaitAsync(Limit);
        await transport.StopAsync(default);
        await Assert.That(calls).IsEqualTo(2);
    }

    [Test]
    public async Task Graceful_stop_drains_handler_and_preserves_queued_messages_for_restart()
    {
        await using var transport = Transport();
        var entered = Signal();
        var release = Signal();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, token) =>
        {
            if (Value(delivery) == 1)
            {
                entered.SetResult();
                await release.Task.WaitAsync(token);
            }
            values.Writer.TryWrite(Value(delivery));
            return Disposition.Complete;
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await entered.Task.WaitAsync(Limit);
        await Send(transport, 2);
        var stop = transport.StopAsync(default);
        await Assert.That(stop.IsCompleted).IsFalse();
        release.SetResult();
        await stop.WaitAsync(Limit);
        await Assert.That(await Next(values)).IsEqualTo(1);
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(2);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Unexpected_failure_is_observed_and_unsettled_delivery_is_retained_for_restart()
    {
        await using var transport = Transport();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (delivery.Attempt == 1) throw new InvalidOperationException("failure executor failed");
            values.Writer.TryWrite(delivery.Attempt);
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await Assert.That(async () => await transport.Completion.WaitAsync(Limit)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.StopAsync(default)).Throws<InvalidOperationException>();
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(2);
        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Hosted_manual_settlement_uses_native_queue_operations(int action)
    {
        var received = Channel.CreateUnbounded<int>();
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Send<InMemoryOrder>("orders");
                transport.Consume("orders", consumer => consumer.Handler<ManualSettlementOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        try
        {
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new InMemoryOrder(action));
            await Assert.That(await Next(received)).IsEqualTo(action * 10 + 1);
            if (action == 1) await Assert.That(await Next(received)).IsEqualTo(12);
            // This barrier delivery runs only after the first delivery's native settlement.
            await sender.SendAsync(new InMemoryOrder(3));
            await Assert.That(await Next(received)).IsEqualTo(31);
            var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
            if (action == 2)
            {
                var dead = await native.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(Limit);
                await Assert.That(dead.Error.Reason).IsEqualTo("manual order");
            }
            else await Assert.That(native.DeadLetters("orders").TryRead(out _)).IsFalse();
        }
        finally
        {
            foreach (var service in hosted.Reverse()) await service.StopAsync(default);
        }
    }

    [Test]
    public async Task Hosted_round_trip_uses_generated_handler_and_dead_letters_poison_messages()
    {
        var received = Channel.CreateUnbounded<int>();
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Send<InMemoryOrder>("orders");
                transport.Consume("orders", consumer => consumer.Handler<InMemoryOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(-1));
        await sender.SendAsync(new InMemoryOrder(42));
        await Assert.That(await Next(received)).IsEqualTo(42);
        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        var poison = await native.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(Limit);
        await Assert.That(poison.Error.Description).IsEqualTo("poison order");
        foreach (var service in hosted.Reverse()) await service.StopAsync(default);
    }

    [Test]
    public async Task Unsupported_native_operations_fail_explicitly()
    {
        await using var transport = Transport(new() { Capabilities = new() { MaxMessageBytes = 4 } });
        await Assert.That(() => transport.GetCapabilities("missing")).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[5]), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await Send(transport, 1, "key")).Throws<InvalidOperationException>();
        await Assert.That(async () => await Send(transport, 1, at: DateTimeOffset.UtcNow.AddHours(1))).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendBatchAsync(new OutgoingMessage[] { new("orders", new byte[1]), new("orders", new byte[1]) }, default)).Throws<InvalidOperationException>();
        await Assert.That(() => Transport(new() { Capabilities = new() { Replay = true } })).Throws<NotSupportedException>();
        await Assert.That(() => Transport(new() { Capacity = 0 })).Throws<ArgumentException>();
    }

    [Test]
    public async Task Invalid_settlement_does_not_complete_a_delivery()
    {
        await using var transport = Transport(new() { Capabilities = new() });
        transport.RegisterConsumer("orders", static (_, _) => new(Disposition.DeadLettered(new("invalid"))));
        await transport.StartAsync(default);
        await Send(transport, 1);
        await Assert.That(async () => await transport.Completion.WaitAsync(Limit)).Throws<InvalidOperationException>();
        await Assert.That(transport.DeadLetters("orders").TryRead(out _)).IsFalse();
    }
    [Test]
    public async Task Native_scheduling_accepts_immediately_and_delivers_earlier_due_messages_first()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(clock: clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        await transport.StartAsync(default);
        await Send(transport, 60, at: clock.GetUtcNow().AddHours(1));
        await clock.NextTimer();
        await Send(transport, 1, at: clock.GetUtcNow().AddMinutes(1));
        await clock.NextTimer();
        await Send(transport, 0);
        await Assert.That(await Next(values)).IsEqualTo(0);
        clock.Advance(TimeSpan.FromMinutes(2));
        await Assert.That(await Next(values)).IsEqualTo(1);
        await clock.NextTimer();
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(1));
        await Assert.That(await Next(values)).IsEqualTo(60);
        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(1)]
    [Arguments(4)]
    public async Task Scheduling_reuses_one_timer_per_partition_across_idle_and_future_waits(int partitions)
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capacity = 1,
            Partitions = partitions,
            Capabilities = new() { OrderedByKey = true, MaxDeliveryDelay = TimeSpan.FromHours(1) },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        }, concurrency: partitions);
        await transport.StartAsync(default);
        for (var i = 0; i < partitions * 4; i++)
        {
            await Send(transport, i, at: clock.GetUtcNow().AddMinutes(1));
            await clock.NextTimer();
            await Assert.That(values.Reader.TryRead(out _)).IsFalse();
            clock.Advance(TimeSpan.FromMinutes(1));
            await Assert.That(await Next(values)).IsEqualTo(i);
        }
        await transport.StopAsync(default).WaitAsync(Limit);
        await Assert.That(clock.TimersCreated).IsEqualTo(partitions);
    }

    [Test]
    [Arguments(1)]
    [Arguments(4)]
    public async Task Scheduler_wakeup_does_not_take_admission_capacity_while_stopped(int partitions)
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capacity = 1,
            Partitions = partitions,
            Capabilities = new() { OrderedByKey = true, MaxDeliveryDelay = TimeSpan.FromHours(1) },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        }, concurrency: partitions);
        for (var i = 0; i < 8; i++)
        {
            await transport.StartAsync(default);
            await transport.StopAsync(default).WaitAsync(Limit);
            // All admission credits remain available even if stop left a wake-up in the channel.
            for (var j = 0; j < partitions; j++)
                await Send(transport, j, "key", at: clock.GetUtcNow().AddMinutes(1)).AsTask().WaitAsync(Limit);
            using var cancellation = new CancellationTokenSource();
            var blocked = Send(transport, -1, "key", at: clock.GetUtcNow().AddMinutes(1), cancellationToken: cancellation.Token);
            await Assert.That(blocked.IsCompleted).IsFalse();
            cancellation.Cancel();
            await Assert.That(async () => await blocked).Throws<OperationCanceledException>();
            clock.Advance(TimeSpan.FromMinutes(1));
            await transport.StartAsync(default);
            var seen = new bool[partitions];
            for (var j = 0; j < partitions; j++) seen[await Next(values)] = true;
            await transport.StopAsync(default).WaitAsync(Limit);
            await Assert.That(seen.All(static value => value)).IsTrue();
            await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        }
    }

    [Test]
    public async Task Scheduled_future_deadline_survives_stop_and_rearms_after_restart()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capacity = 1,
            Capabilities = new() { MaxDeliveryDelay = TimeSpan.FromDays(3) },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        await Send(transport, 1, at: clock.GetUtcNow().AddDays(2));
        await transport.StartAsync(default);
        await clock.NextTimer();
        await transport.StopAsync(default).WaitAsync(Limit);
        clock.Advance(TimeSpan.FromHours(12)); // The stopped run's timer must not control the next run.
        await transport.StartAsync(default);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromDays(1)); // Deadlines longer than one day re-arm without delivering early.
        await clock.NextTimer();
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(12));
        await Assert.That(await Next(values)).IsEqualTo(1);
        await transport.StopAsync(default).WaitAsync(Limit);
        await Assert.That(clock.TimersCreated).IsEqualTo(2);
    }

    [Test]
    public async Task Unordered_queue_redelivery_does_not_stall_ready_messages_with_a_full_buffer()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new() { Capacity = 2 }, clock);
        var entered = Signal();
        var release = Signal();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            if (Value(delivery) == 1 && delivery.Attempt == 1)
            {
                entered.SetResult();
                await release.Task;
                return Disposition.Redeliver(TimeSpan.FromMinutes(1));
            }
            values.Writer.TryWrite(Value(delivery) * 10 + delivery.Attempt);
            return Disposition.Complete;
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await entered.Task.WaitAsync(Limit);
        await Send(transport, 2);
        release.SetResult();
        await Assert.That(await Next(values)).IsEqualTo(21);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(await Next(values)).IsEqualTo(12);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Scheduling_and_redelivery_respect_exact_capability_boundaries()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capabilities = new() { MaxDeliveryDelay = TimeSpan.FromMinutes(1), MaxRedeliveryDelay = TimeSpan.FromMinutes(1) },
        }, clock);
        await Send(transport, 1, at: clock.GetUtcNow().AddMinutes(1));
        await Assert.That(async () => await Send(transport, 2, at: clock.GetUtcNow().AddMinutes(1).AddTicks(1))).Throws<InvalidOperationException>();
        transport.RegisterConsumer("orders", static (_, _) => new(Disposition.Redeliver(TimeSpan.FromMinutes(1).Add(TimeSpan.FromTicks(1)))));
        await transport.StartAsync(default);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(async () => await transport.Completion.WaitAsync(Limit)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Dispose_unblocks_sends_waiting_for_native_capacity()
    {
        await using var transport = Transport(new() { Capacity = 1 });
        await Send(transport, 1);
        var blocked = Send(transport, 2);
        await Assert.That(blocked.IsCompleted).IsFalse();
        await transport.DisposeAsync();
        await Assert.That(async () => await blocked).Throws<ChannelClosedException>();
    }

    [Test]
    public async Task Cancelled_shutdown_keeps_unsettled_work_and_counts_a_new_delivery_on_restart()
    {
        await using var transport = Transport();
        var entered = Signal();
        var unwinding = Signal();
        var release = Signal();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, token) =>
        {
            if (delivery.Attempt == 1)
            {
                entered.SetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
                finally
                {
                    unwinding.SetResult();
                    await release.Task;
                }
            }
            values.Writer.TryWrite(delivery.Attempt);
            return Disposition.Complete;
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await entered.Task.WaitAsync(Limit);
        using var cancellation = new CancellationTokenSource();
        var stop = transport.StopAsync(cancellation.Token);
        cancellation.Cancel();
        try
        {
            await Assert.That(async () => await stop.WaitAsync(Limit)).Throws<OperationCanceledException>();
            await unwinding.Task.WaitAsync(Limit);
            await Assert.That(transport.Completion.IsCompleted).IsFalse();
            await Assert.That(() => transport.StartAsync(default)).Throws<InvalidOperationException>();
        }
        finally { release.TrySetResult(); }
        await transport.Completion.WaitAsync(Limit);
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(2);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Repeated_stop_of_idle_receivers_completes_during_reader_startup()
    {
        await using var transport = Transport();
        transport.RegisterConsumer("orders", static (_, _) => new(Disposition.Complete), concurrency: 32);
        for (var iteration = 0; iteration < 64; iteration++)
        {
            await transport.StartAsync(default);
            try { await transport.StopAsync(default).WaitAsync(Limit); }
            catch
            {
                // Unblock a stranded read so a failure cannot hang asynchronous disposal.
                for (var receiver = 0; receiver < 32; receiver++) await Send(transport, receiver);
                throw;
            }
        }
    }

    [Test]
    public async Task Shared_queue_stop_wakes_every_outstanding_read()
    {
        var channel = Channel.CreateBounded<InMemoryMessage>(new BoundedChannelOptions(1) { AllowSynchronousContinuations = true });
        var reader = new InMemoryReadState(channel, new(true), shared: true);
        using var stop = new CancellationTokenSource();
        using var registration = reader.RegisterStop(stop.Token);
        var first = reader.ReadAsync(stop.Token);
        var second = reader.ReadAsync(stop.Token);
        stop.Cancel();
        var bothCompleted = first.IsCompleted && second.IsCompleted;
        // Release the old implementation's stranded reader before asserting.
        channel.Writer.TryWrite(new(true));
        await first;
        await second;
        await Assert.That(bothCompleted).IsTrue();
    }

    [Test]
    public async Task Shared_queue_restart_removes_wakeup_without_losing_full_capacity_data()
    {
        var channel = Channel.CreateBounded<InMemoryMessage>(3);
        var first = new InMemoryMessage();
        var second = new InMemoryMessage();
        var reader = new InMemoryReadState(channel, new(true), shared: true);
        channel.Writer.TryWrite(first);
        for (var iteration = 0; iteration < 4; iteration++)
        {
            reader.Prepare();
            using var stop = new CancellationTokenSource();
            using var registration = reader.RegisterStop(stop.Token);
            stop.Cancel();
            // A handler may return its pending delivery after stop inserted the wake-up.
            await Assert.That(channel.Writer.TryWrite(second)).IsTrue();
            reader.Prepare();
            await Assert.That(await reader.ReadAsync(default)).IsSameReferenceAs(first);
            await Assert.That(await reader.ReadAsync(default)).IsSameReferenceAs(second);
            await Assert.That(channel.Reader.Count).IsEqualTo(0);
            channel.Writer.TryWrite(first);
        }
    }

    [Test]
    public async Task Small_shared_queue_restarts_at_full_admission_capacity()
    {
        await using var transport = Transport(new() { Capacity = 1 });
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); }, concurrency: 32);
        for (var iteration = 0; iteration < 32; iteration++)
        {
            await Send(transport, iteration).AsTask().WaitAsync(Limit);
            await transport.StartAsync(default);
            await Assert.That(await Next(values)).IsEqualTo(iteration);
            await transport.StopAsync(default).WaitAsync(Limit);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Scheduled_batch_preserves_equal_due_time_order_across_restart(bool log)
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capabilities = new() { ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true, MaxBatchSize = 4, MaxDeliveryDelay = TimeSpan.FromHours(1) },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); }, log ? "workers" : null);
        var at = clock.GetUtcNow().AddMinutes(1);
        var messages = Enumerable.Range(1, 3).Select(value => new OutgoingMessage("orders", BitConverter.GetBytes(value),
            orderingKey: new WireString("key"), deliverAt: at)).ToArray();
        await transport.SendBatchAsync(messages, default);
        await transport.StartAsync(default);
        await clock.NextTimer(); // All three messages are now in the due heap.
        await transport.StopAsync(default);
        await Send(transport, 4, "key", at);
        await transport.StartAsync(default);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromMinutes(1));
        for (var value = 1; value <= 4; value++) await Assert.That(await Next(values)).IsEqualTo(value);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Start_with_suppressed_execution_context_preserves_suppression_and_isolates_consumers()
    {
        await using var transport = Transport();
        var ambient = new AsyncLocal<string?> { Value = "caller" };
        var seen = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.RegisterConsumer("orders", (_, _) =>
        {
            seen.SetResult(ambient.Value);
            return new(Disposition.Complete);
        });
        Task start;
        using (ExecutionContext.SuppressFlow())
        {
            start = transport.StartAsync(default);
            if (!ExecutionContext.IsFlowSuppressed()) throw new InvalidOperationException("Caller suppression was restored early.");
        }
        await start;
        await Send(transport, 1);
        await Assert.That(await seen.Task.WaitAsync(Limit)).IsNull();
        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Full_partition_does_not_block_scheduled_delivery_to_other_partitions(bool log)
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capacity = 3,
            Partitions = 4,
            Capabilities = new() { ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue, OrderedByKey = true, MaxDeliveryDelay = TimeSpan.FromHours(1) },
        }, clock);
        var entered = Signal();
        var release = Signal();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, token) =>
        {
            if (Value(delivery) == 1)
            {
                entered.SetResult();
                await release.Task.WaitAsync(token);
            }
            values.Writer.TryWrite(Value(delivery));
            return Disposition.Complete;
        }, log ? "workers" : null, 4);
        await transport.StartAsync(default);
        try
        {
            await Send(transport, 1, "a");
            await entered.Task.WaitAsync(Limit);
            // Fill the partition, plus the group queue and fan-out pending slot for logs.
            var last = log ? 8 : 4;
            for (var i = 2; i <= last; i++) await Send(transport, i, "a").AsTask().WaitAsync(Limit);
            await Send(transport, 20, "a", clock.GetUtcNow().AddMinutes(1));
            await clock.NextTimer();
            await Send(transport, 21, "b", clock.GetUtcNow().AddMinutes(2));
            await clock.NextTimer();
            clock.Advance(TimeSpan.FromMinutes(2));
            await Assert.That(await Next(values)).IsEqualTo(21);
            var stop = transport.StopAsync(default);
            release.SetResult();
            await stop.WaitAsync(Limit);
            await transport.StartAsync(default);
            for (var i = 1; i <= last; i++) await Assert.That(await Next(values)).IsEqualTo(i);
            await Assert.That(await Next(values)).IsEqualTo(20);
        }
        finally { release.TrySetResult(); }
        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_batch_sends_preserve_key_order(bool log)
    {
        await using var transport = Transport(new()
        {
            Capabilities = new() { ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue, OrderedByKey = true, MaxBatchSize = 3 },
        });
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); }, log ? "workers" : null);
        await transport.StartAsync(default);
        var messages = Enumerable.Range(1, 3).Select(value => new OutgoingMessage("orders", BitConverter.GetBytes(value), orderingKey: new WireString("key"))).ToArray();
        await transport.SendBatchAsync(messages, default);
        await Assert.That(await Next(values)).IsEqualTo(1);
        await Assert.That(await Next(values)).IsEqualTo(2);
        await Assert.That(await Next(values)).IsEqualTo(3);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Batch_cancellation_preserves_already_accepted_messages()
    {
        await using var transport = Transport(new() { Capacity = 1 });
        var messages = new OutgoingMessage[] { new("orders", BitConverter.GetBytes(1)), new("orders", BitConverter.GetBytes(2)) };
        using var cancellation = new CancellationTokenSource();
        var batch = transport.SendBatchAsync(messages, cancellation.Token);
        await Assert.That(batch.IsCompleted).IsFalse();
        cancellation.Cancel();
        await Assert.That(async () => await batch).Throws<OperationCanceledException>();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) => { values.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(1);
        await Send(transport, 3);
        await Assert.That(await Next(values)).IsEqualTo(3);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Instances_with_identical_endpoint_names_do_not_share_messages()
    {
        await using var first = Transport();
        await using var second = Transport();
        var firstValues = Channel.CreateUnbounded<int>();
        var secondValues = Channel.CreateUnbounded<int>();
        first.RegisterConsumer("orders", (delivery, _) => { firstValues.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        second.RegisterConsumer("orders", (delivery, _) => { secondValues.Writer.TryWrite(Value(delivery)); return new(Disposition.Complete); });
        await first.StartAsync(default);
        await second.StartAsync(default);
        await Send(first, 1);
        await Send(second, 2);
        await Assert.That(await Next(firstValues)).IsEqualTo(1);
        await Assert.That(await Next(secondValues)).IsEqualTo(2);
    }

    [Test]
    public async Task Hosted_startup_rejects_a_consumer_with_no_durable_failure_executor()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Channel.CreateUnbounded<int>());
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", local =>
            {
                local.Options.Configure(options => options.Endpoints.Add("orders", new() { Capabilities = new() }));
                local.Consume("orders", consumer => consumer.Handler<InMemoryOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Assert.That(async () =>
        {
            foreach (var service in provider.GetServices<IHostedService>()) await service.StartAsync(default);
        }).Throws<InvalidOperationException>().WithMessageContaining("Options.Failure");
    }

    [Test]
    public async Task Hosted_default_failure_executor_honors_explicit_discard_classification()
    {
        var received = Channel.CreateUnbounded<int>();
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddSingleton(new FailureClassifierBuilder().On<InvalidOperationException>(FailureAction.Discard).Build());
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", local =>
            {
                local.Send<InMemoryOrder>("orders");
                local.Consume("orders", consumer => consumer.Handler<InMemoryOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(-1));
        await sender.SendAsync(new InMemoryOrder(42));
        await Assert.That(await Next(received)).IsEqualTo(42);
        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        await Assert.That(native.DeadLetters("orders").TryRead(out _)).IsFalse();
        foreach (var service in hosted.Reverse()) await service.StopAsync(default);
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.Provision)]
    [Arguments(TopologyMode.None)]
    public async Task Explicit_native_endpoints_satisfy_topology_without_external_provisioning(TopologyMode mode)
    {
        await using var transport = Transport();
        var report = await transport.ApplyTopologyAsync(mode, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(() => transport.GetCapabilities("undeclared")).Throws<InvalidOperationException>();
    }

}

[Message("tests.inmemory-order")]
public sealed record InMemoryOrder(int Id);
public sealed class InMemoryOrderHandler(Channel<int> received) : IHandler<InMemoryOrder>
{
    public ValueTask HandleAsync(InMemoryOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Id < 0) throw new InvalidOperationException("poison order");
        received.Writer.TryWrite(message.Id);
        return default;
    }
}

public sealed class ManualSettlementOrderHandler(Channel<int> received) : IHandler<InMemoryOrder>
{
    public ValueTask HandleAsync(InMemoryOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        var settlement = context.Features.GetRequired<ISettlementFeature>();
        if (message.Id == 1 && context.Attempt == 1) settlement.Retry(TimeSpan.Zero);
        else if (message.Id == 2) settlement.DeadLetter("manual order");
        else settlement.Complete();
        received.Writer.TryWrite(message.Id * 10 + context.Attempt);
        return default;
    }
}
