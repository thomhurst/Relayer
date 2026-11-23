using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class InMemoryTransportTests
{
    private static readonly TimeSpan s_limit = TimeSpan.FromSeconds(10);

    private static TaskCompletionSource Signal()
    {
        return new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static InMemoryTransport Transport(InMemoryEndpointOptions? options = null, TimeProvider? clock = null)
    {
        return new("local", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = options ?? new() }, clock);
    }

    private static ValueTask Send(InMemoryTransport transport, int value, string? key = null, DateTimeOffset? at = null,
        CancellationToken cancellationToken = default)
    {
        return transport.SendAsync(new("orders", BitConverter.GetBytes(value), orderingKey: key is null ? null : new WireString(key), deliverAt: at), cancellationToken);
    }

    private static int Value(InMemoryDelivery delivery)
    {
        return BitConverter.ToInt32(delivery.Message.Body.Span);
    }

    private static async Task<int> Next(Channel<int> values)
    {
        return await values.Reader.ReadAsync().AsTask().WaitAsync(s_limit);
    }

    [Test]
    public async Task Send_copies_borrowed_body_headers_and_key_and_applies_native_callback()
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                OrderedByKey = true
            }
        });
        var body = new byte[] { 42 };
        var header = Encoding.UTF8.GetBytes("tenant");
        var key = Encoding.UTF8.GetBytes("key");
        var headers = new[] { new KeyValuePair<string, WireString>("tenant", new(header)), new("traceparent", new WireString("original-context")) };
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(native =>
        {
            if (native.Body.Span[0] != 42)
            {
                throw new InvalidOperationException("Callback did not receive native storage.");
            }
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
            {
                throw new InvalidOperationException("Borrowed send storage or creation context was retained or overwritten.");
            }

            seen.SetResult();
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await seen.Task.WaitAsync(s_limit);
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
            if (Interlocked.Increment(ref seen[Value(delivery)]) != 1)
            {
                throw new InvalidOperationException("Duplicate native queue delivery.");
            }

            if (Interlocked.Increment(ref count) == seen.Length)
            {
                finished.SetResult();
            }

            return new(Disposition.Complete);
        }

        transport.RegisterConsumer("orders", Receive, concurrency: 2);
        transport.RegisterConsumer("orders", Receive, concurrency: 2);
        await transport.StartAsync(default);

        for (var i = 0; i < seen.Length; i++)
        {
            await Send(transport, i);
        }

        await finished.Task.WaitAsync(s_limit);
        await transport.StopAsync(default);
        await Assert.That(seen.All(value => value == 1)).IsTrue();
    }

    [Test]
    public async Task Populated_batch_holds_admission_until_first_delivery_settles()
    {
        const int BatchSize = 64;
        await using var transport = Transport(new()
        {
            Capacity = BatchSize,
            MessagePoolCapacity = BatchSize,
            Capabilities = new()
            {
                MaxBatchSize = BatchSize
            },
        });
        var entered = Signal();
        var finished = Signal();
        var release = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = new int[BatchSize + 1];
        var count = 0;
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            seen[count++] = Value(delivery);

            if (count == 1)
            {
                entered.TrySetResult();
                return new(release.Task);
            }

            if (count == seen.Length)
            {
                finished.TrySetResult();
            }

            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);

        try
        {
            var batch = new OutgoingMessage[BatchSize];

            for (var i = 0; i < batch.Length; i++)
            {
                batch[i] = new("orders", BitConverter.GetBytes(i));
            }

            await transport.SendBatchAsync(batch, default);
            await entered.Task.WaitAsync(s_limit);
            var blocked = Send(transport, BatchSize);
            await Assert.That(blocked.IsCompleted).IsFalse();
            release.TrySetResult(Disposition.Complete);
            await blocked.AsTask().WaitAsync(s_limit);
            await finished.Task.WaitAsync(s_limit);
            await transport.StopAsync(default).WaitAsync(s_limit);

            for (var i = 0; i < seen.Length; i++)
            {
                await Assert.That(seen[i]).IsEqualTo(i);
            }
        }
        finally
        {
            release.TrySetResult(Disposition.Complete);
        }
    }

    [Test]
    public async Task Bounded_send_observes_cancellation_and_cancelled_message_is_not_delivered()
    {
        await using var transport = Transport(new()
        {
            Capacity = 1
        });
        await Send(transport, 1);
        using var cancellation = new CancellationTokenSource();
        var blocked = Send(transport, 2, cancellationToken: cancellation.Token);
        await Assert.That(blocked.IsCompleted).IsFalse();
        cancellation.Cancel();
        await Assert.That(async () => await blocked).Throws<OperationCanceledException>();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await Assert.That(await Next(values)).IsEqualTo(1);
        await Send(transport, 3);
        await Assert.That(await Next(values)).IsEqualTo(3);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Partitioned_logs_fan_out_to_independent_groups_and_commit_only_completed_positions()
    {
        await using var transport = Transport(new()
        {
            Capacity = 4,
            Capabilities = new()
            {
                ConsumptionModel = ConsumptionModel.Log
            }
        });
        var slowStarted = Signal();
        var slowDone = Signal();
        var slowCount = 0;
        var fastAtThird = Signal();
        var release = Signal();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            slowStarted.TrySetResult();
            await release.Task;

            if (Interlocked.Increment(ref slowCount) == 3)
            {
                slowDone.SetResult();
            }

            return Disposition.Complete;
        }, "slow");
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            if (Value(delivery) == 3)
            {
                if (delivery.Position != 2 || transport.GetCommittedPosition("orders", "fast", 0) != 1)
                {
                    throw new InvalidOperationException("Log committed an unfinished position or changed order.");
                }

                fastAtThird.SetResult();
                await release.Task;
            }

            return Disposition.Complete;
        }, "fast");

        for (var i = 1; i <= 3; i++)
        {
            await Send(transport, i);
        }

        await transport.StartAsync(default);
        await Task.WhenAll(slowStarted.Task, fastAtThird.Task).WaitAsync(s_limit);
        await Assert.That(transport.GetCommittedPosition("orders", "slow", 0)).IsEqualTo(-1);
        await Assert.That(transport.GetCommittedPosition("orders", "fast", 0)).IsEqualTo(1);
        var stop = transport.StopAsync(default);
        release.SetResult();
        await stop.WaitAsync(s_limit);
        await Assert.That(transport.GetCommittedPosition("orders", "fast", 0)).IsEqualTo(2);
        await Assert.That(transport.GetCommittedPosition("orders", "slow", 0)).IsEqualTo(0);
        await transport.StartAsync(default);
        await slowDone.Task.WaitAsync(s_limit);
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
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxRedeliveryDelay = TimeSpan.Zero
            },
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
        await firstStarted.Task.WaitAsync(s_limit);
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
        await using var transport = Transport(new()
        {
            Partitions = 3,
            Capabilities = new()
            {
                OrderedByKey = true
            }
        });
        var partitions = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer(
            "orders",
            (delivery, _) =>
            {
                partitions.Writer.TryWrite(delivery.Partition);
                return new(Disposition.Complete);
            },
            concurrency: 3
        );
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
        byte[] originalBytes = [0x80, 0x00, 0xFF];
        byte[] errorBytes = [0xFF, 0x00, 0x80];
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (Value(delivery) == 1)
            {
                return new(Disposition.DeadLettered(new("invalid", "description", new[] { new KeyValuePair<string, WireString>("error", new(errorBytes)) })));
            }

            seen.SetResult();
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        KeyValuePair<string, WireString>[] headers = [new("original", new(originalBytes))];
        await transport.SendAsync(new("orders", BitConverter.GetBytes(1), headers), default);
        Array.Clear(originalBytes);
        await Send(transport, 2);
        await seen.Task.WaitAsync(s_limit);
        var dead = await transport.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(s_limit);
        Array.Clear(errorBytes);
        await Assert.That(BitConverter.ToInt32(dead.Body.Span)).IsEqualTo(1);
        await Assert.That(dead.Headers.Span[0].Value.Text).IsNull();
        await Assert.That(dead.Headers.Span[0].Value.Utf8.Span.SequenceEqual(new byte[] { 0x80, 0x00, 0xFF })).IsTrue();
        await Assert.That(dead.Error.Reason).IsEqualTo("invalid");
        await Assert.That(dead.Error.Headers.Span[0].Value.Text).IsNull();
        await Assert.That(dead.Error.Headers.Span[0].Value.Utf8.Span.SequenceEqual(new byte[] { 0xFF, 0x00, 0x80 })).IsTrue();
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

            if (delivery.Attempt == 1)
            {
                delivery.Redeliver(TimeSpan.Zero);
            }
            else
            {
                delivery.Complete();
                done.SetResult();
            }

            return new(Disposition.SettledByHandler);
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await done.Task.WaitAsync(s_limit);
        await transport.StopAsync(default);
        await Assert.That(calls).IsEqualTo(2);
    }

    [Test]
    [Arguments(false, DispositionKind.Complete)]
    [Arguments(false, DispositionKind.DeadLetter)]
    [Arguments(false, DispositionKind.Redeliver)]
    [Arguments(true, DispositionKind.Complete)]
    [Arguments(true, DispositionKind.DeadLetter)]
    [Arguments(true, DispositionKind.Redeliver)]
    public async Task Native_settlement_survives_callback_failure(bool log, DispositionKind settlement)
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromSeconds(1);
        await using var transport = Transport(new()
        {
            Capacity = 1,
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                DeadLetter = true,
                MaxDeliveryDelay = delay,
                MaxRedeliveryDelay = delay
            }
        }, clock);
        var error = new InvalidOperationException("after native settlement");
        var received = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            int value = Value(delivery);

            if (value == 1 && delivery.Attempt == 1)
            {
                switch (settlement)
                {
                    case DispositionKind.Complete:
                        delivery.Complete();
                        break;
                    case DispositionKind.DeadLetter:
                        delivery.DeadLetter(new("manual", "details"));
                        break;
                    case DispositionKind.Redeliver:
                        delivery.Redeliver(delay);
                        break;
                }

                await Task.Yield();
                throw error;
            }

            received.Writer.TryWrite(value * 10 + delivery.Attempt);
            return Disposition.Complete;
        }, log ? "workers" : null);
        await transport.StartAsync(default);
        await Send(transport, 1);
        Exception? fault = await GetReceiverFault(transport);
        await Assert.That(fault).IsSameReferenceAs(error);

        var terminal = settlement != DispositionKind.Redeliver;

        if (log)
        {
            await Assert.That(transport.GetCommittedPosition("orders", "workers", 0)).IsEqualTo(terminal ? 0L : -1L);
        }

        bool deadLettered = transport.DeadLetters("orders").TryRead(out var dead);
        await Assert.That(deadLettered).IsEqualTo(settlement == DispositionKind.DeadLetter);

        if (dead is not null)
        {
            await Assert.That(dead.Error.Reason).IsEqualTo("manual");
            await Assert.That(dead.Error.Description).IsEqualTo("details");
            await Assert.That(dead.Attempt).IsEqualTo(1);
            await Assert.That(BitConverter.ToInt32(dead.Body.Span)).IsEqualTo(1);
        }

        await transport.StartAsync(default);
        using var timeout = new CancellationTokenSource(s_limit);

        if (!terminal)
        {
            var redelivered = Next(received);
            var armed = clock.NextTimer();
            await Assert.That(await Task.WhenAny(armed, redelivered).WaitAsync(s_limit)).IsSameReferenceAs(armed);
            await armed;
            clock.Advance(delay);
            await Assert.That(await redelivered).IsEqualTo(12);
        }

        await Send(transport, 2, cancellationToken: timeout.Token);
        await Assert.That(await Next(received)).IsEqualTo(21);
        await transport.StopAsync(default);
        await Assert.That(transport.DeadLetters("orders").TryRead(out _)).IsFalse();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task Cancelled_stop_preserves_native_queue_redelivery_at_full_admission(int delaySeconds)
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromSeconds(delaySeconds);
        await using var transport = Transport(new()
        {
            Capacity = 2
        }, clock);
        var settled = Signal();
        var release = Signal();
        var received = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            int value = Value(delivery);

            if (value == 1 && delivery.Attempt == 1)
            {
                delivery.Redeliver(delay);
                settled.SetResult();
                await release.Task;
                return Disposition.SettledByHandler;
            }

            received.Writer.TryWrite(value * 10 + delivery.Attempt);
            return Disposition.Complete;
        });

        await transport.StartAsync(default);

        try
        {
            await Send(transport, 1);
            await settled.Task.WaitAsync(s_limit);
            await Send(transport, 2);
            using var cancellation = new CancellationTokenSource();
            var stop = transport.StopAsync(cancellation.Token);
            cancellation.Cancel();
            await Assert.That(async () => await stop.WaitAsync(s_limit)).Throws<OperationCanceledException>();
        }
        finally
        {
            release.TrySetResult();
        }

        await transport.Completion.WaitAsync(s_limit);
        await transport.StartAsync(default);
        await Assert.That(await Next(received)).IsEqualTo(21);
        var redelivery = Next(received);

        if (delay > TimeSpan.Zero)
        {
            var armed = clock.NextTimer();
            await Assert.That(await Task.WhenAny(armed, redelivery).WaitAsync(s_limit)).IsSameReferenceAs(armed);
            await armed;
            clock.Advance(delay);
        }

        await Assert.That(await redelivery).IsEqualTo(12);
        await transport.StopAsync(default).WaitAsync(s_limit);
    }

    private static async Task<Exception?> GetReceiverFault(InMemoryTransport transport)
    {
        try
        {
            await transport.Completion.WaitAsync(s_limit);
            return null;
        }
        catch (InvalidOperationException error)
        {
            return error;
        }
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
        await entered.Task.WaitAsync(s_limit);
        await Send(transport, 2);
        var stop = transport.StopAsync(default);
        await Assert.That(stop.IsCompleted).IsFalse();
        release.SetResult();
        await stop.WaitAsync(s_limit);
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
            if (delivery.Attempt == 1)
            {
                throw new InvalidOperationException("failure executor failed");
            }

            values.Writer.TryWrite(delivery.Attempt);
            return new(Disposition.Complete);
        });
        await transport.StartAsync(default);
        await Send(transport, 1);
        await Assert.That(async () => await transport.Completion.WaitAsync(s_limit)).Throws<InvalidOperationException>();
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

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        try
        {
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new InMemoryOrder(action));
            await Assert.That(await Next(received)).IsEqualTo(action * 10 + 1);

            if (action == 1)
            {
                await Assert.That(await Next(received)).IsEqualTo(12);
            }
            // This barrier delivery runs only after the first delivery's native settlement.
            await sender.SendAsync(new InMemoryOrder(3));
            await Assert.That(await Next(received)).IsEqualTo(31);
            var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();

            if (action == 2)
            {
                var dead = await native.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(s_limit);
                await Assert.That(dead.Error.Reason).IsEqualTo("manual order");
            }
            else
            {
                await Assert.That(native.DeadLetters("orders").TryRead(out _)).IsFalse();
            }
        }
        finally
        {
            foreach (var service in hosted.Reverse())
            {
                await service.StopAsync(default);
            }
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Hosted_native_settlement_preserves_the_handler_error(bool deadLetter)
    {
        var received = Channel.CreateUnbounded<int>();
        var error = new InvalidOperationException("after hosted native settlement");
        var failures = deadLetter ? new FailureOptions(RetryDelays: [TimeSpan.Zero]) : FailureOptions.None;
        var services = new ServiceCollection();
        services.AddSingleton(received);
        services.AddSingleton(error);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Send<InMemoryOrder>("orders");
                transport.Consume("orders", consumer => consumer
                    .Failures(failures)
                    .Handler<NativeSettlementFaultOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var hosted = provider.GetServices<IHostedService>().ToArray();

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(deadLetter ? -2 : -1));
        Exception? fault = await GetReceiverFault(native);
        await Assert.That(fault).IsSameReferenceAs(error);
        await Assert.That(await Next(received)).IsEqualTo(1);
        bool deadLettered = native.DeadLetters("orders").TryRead(out var dead);
        await Assert.That(deadLettered).IsEqualTo(deadLetter);

        if (dead is not null)
        {
            await Assert.That(dead.Error.Reason).IsEqualTo("manual");
        }

        await native.StartAsync(default);
        await sender.SendAsync(new InMemoryOrder(42));
        await Assert.That(await Next(received)).IsEqualTo(42);

        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(default);
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

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(-1));
        await sender.SendAsync(new InMemoryOrder(42));
        await Assert.That(await Next(received)).IsEqualTo(42);
        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        var poison = await native.DeadLetters("orders").ReadAsync().AsTask().WaitAsync(s_limit);
        await Assert.That(poison.Error.Description).IsEqualTo("poison order");

        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(default);
        }
    }

    [Test]
    public async Task Hosted_scheduling_uses_the_clock_configured_through_named_options()
    {
        var clock = new InMemoryTestClock();
        clock.Advance(DateTimeOffset.UtcNow.AddDays(3) - clock.GetUtcNow());
        var received = Channel.CreateUnbounded<int>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", local =>
            {
                local.Options.Configure(options =>
                {
                    options.UseTimeProvider(clock);
                    options.Endpoints.Add("orders", new()
                    {
                        Capabilities = new()
                        {
                            DeadLetter = true,
                            MaxDeliveryDelay = TimeSpan.FromHours(1),
                        },
                    });
                });
                local.Send<InMemoryOrder>("orders");
                local.Consume("orders", consumer => consumer.Handler<InMemoryOrderHandler>());
            });
        });
        using var host = builder.Build();
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var delay = TimeSpan.FromMinutes(5);
        var due = clock.GetUtcNow() + delay;
        await sender.SendAsync(new InMemoryOrder(42), options => options.DeliverAt = due);
        await clock.NextTimer();
        await Assert.That(received.Reader.TryRead(out _)).IsFalse();

        clock.Advance(delay);
        await Assert.That(await Next(received)).IsEqualTo(42);
        await host.StopAsync();
    }

    [Test]
    public async Task Buffered_send_timeout_faults_the_hosted_receiver_when_queue_admission_is_full()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("local", transport =>
            {
                transport.Options.Configure(options => options.Endpoints.Add("orders", new() { Capacity = 1 }));
                transport.Send<InMemoryOrder>("orders", route => route.SendOptions = new() { Timeout = TimeSpan.FromSeconds(1) });
                transport.Consume("orders", consumer => consumer.Handler<InMemoryFollowUpOrderHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var hosted = provider.GetServices<IHostedService>().ToArray();

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(1));

        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        await Assert.That(async () => await native.Completion.WaitAsync(s_limit))
            .Throws<TimeoutException>().WithMessageContaining("route timeout");
        await Assert.That(native.Completion.IsFaulted).IsTrue();
        await Assert.That(native.DeadLetters("orders").TryRead(out _)).IsFalse();
        await Assert.That(async () => await native.StopAsync(default)).Throws<TimeoutException>();
    }

    [Test]
    public async Task Endpoint_construction_rejects_tombstones()
    {
        var options = new InMemoryEndpointOptions();
        var capabilities = options.Capabilities with
        {
            Tombstones = true
        };

        await Assert.That(() => Transport(options with { Capabilities = capabilities }))
            .Throws<NotSupportedException>()
            .WithMessage("Endpoint 'orders': in-memory endpoints cannot preserve native null values. Disable Tombstones.");
    }

    [Test]
    public async Task Native_sends_reject_tombstones_and_accept_empty_bodies()
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                OrderedByKey = true
            }
        });
        var key = new WireString("order-1");
        var tombstone = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty, orderingKey: key, isTombstone: true);

        await Assert.That(async () => await transport.SendAsync(tombstone, default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendBatchAsync(new[] { tombstone }, default)).Throws<NotSupportedException>();
        await transport.SendAsync(new("orders", ReadOnlyMemory<byte>.Empty, orderingKey: key), default);
    }

    [Test]
    public async Task Unsupported_native_operations_fail_explicitly()
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                MaxMessageBytes = 4
            }
        });
        await Assert.That(() => transport.GetCapabilities("missing")).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[5]), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await Send(transport, 1, "key")).Throws<InvalidOperationException>();
        await Assert.That(async () => await Send(transport, 1, at: DateTimeOffset.UtcNow.AddHours(1))).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendBatchAsync(new OutgoingMessage[] { new("orders", new byte[1]), new("orders", new byte[1]) }, default)).Throws<InvalidOperationException>();
        await Assert.That(() => Transport(new() { Capabilities = new() { Replay = true } })).Throws<NotSupportedException>();
        await Assert.That(() => Transport(new() { Capacity = 0 })).Throws<ArgumentException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Unset_callback_disposition_faults_without_committing_and_retains_delivery(bool defaultValueTask)
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                ConsumptionModel = ConsumptionModel.Log
            },
        });
        var calls = 0;
        var received = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                return defaultValueTask ? default : new ValueTask<Disposition>(default(Disposition));
            }

            received.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        }, "workers");
        await transport.StartAsync(default);
        await Send(transport, 42);
        await Assert.That(async () => await transport.Completion.WaitAsync(s_limit)).Throws<InvalidOperationException>();
        await Assert.That(transport.GetCommittedPosition("orders", "workers", 0)).IsEqualTo(-1);
        await transport.StartAsync(default);
        await Assert.That(await Next(received)).IsEqualTo(42);
        await transport.StopAsync(default);
        await Assert.That(transport.GetCommittedPosition("orders", "workers", 0)).IsEqualTo(0);
    }

    [Test]
    public async Task Invalid_settlement_does_not_complete_a_delivery()
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
        });
        transport.RegisterConsumer("orders", static (_, _) => new(Disposition.DeadLettered(new("invalid"))));
        await transport.StartAsync(default);
        await Send(transport, 1);
        await Assert.That(async () => await transport.Completion.WaitAsync(s_limit)).Throws<InvalidOperationException>();
        await Assert.That(transport.DeadLetters("orders").TryRead(out _)).IsFalse();
    }

    [Test]
    [Arguments(false, 200.0)]
    [Arguments(true, 200.0)]
    [Arguments(false, 0.5)]
    [Arguments(true, 0.5)]
    public async Task Ordered_redelivery_waits_for_elapsed_deadline_despite_timer_precision(bool log, double milliseconds)
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromMilliseconds(milliseconds);
        var due = clock.GetUtcNow() + delay;
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxRedeliveryDelay = delay,
            },
        }, clock);
        var redelivered = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (delivery.Attempt == 1)
            {
                return new(Disposition.Redeliver(delay));
            }

            redelivered.TrySetResult(clock.GetUtcNow());
            return new(Disposition.Complete);
        }, log ? "workers" : null);
        await transport.StartAsync(default);
        await Send(transport, 1, "key");
        var armed = clock.NextTimer();
        await Assert.That(await Task.WhenAny(armed, redelivered.Task).WaitAsync(s_limit)).IsSameReferenceAs(armed);
        await armed;

        if (milliseconds >= 1)
        {
            clock.Advance(delay - TimeSpan.FromMilliseconds(0.5));
            clock.FireTimersEarly();
            var rearmed = clock.NextTimer();
            await Assert.That(await Task.WhenAny(rearmed, redelivered.Task).WaitAsync(s_limit)).IsSameReferenceAs(rearmed);
            await rearmed;
        }

        await Assert.That(redelivered.Task.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await Assert.That(await redelivered.Task.WaitAsync(s_limit)).IsGreaterThanOrEqualTo(due);
        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Ordered_redelivery_deadline_survives_timer_chunks_and_restart(bool log)
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromDays(2);
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxRedeliveryDelay = delay,
            },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (Value(delivery) == 1 && delivery.Attempt == 1)
            {
                return new(Disposition.Redeliver(delay));
            }

            values.Writer.TryWrite(Value(delivery) * 10 + delivery.Attempt);
            return new(Disposition.Complete);
        }, log ? "workers" : null);
        await transport.StartAsync(default);
        await Send(transport, 1, "key");
        await Send(transport, 2, "key");
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromDays(1));
        await clock.NextTimer();
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        await transport.StopAsync(default).WaitAsync(s_limit);
        clock.Advance(TimeSpan.FromHours(12));
        await transport.StartAsync(default);
        await clock.NextTimer();
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(12));
        await Assert.That(await Next(values)).IsEqualTo(12);
        await Assert.That(await Next(values)).IsEqualTo(21);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Native_scheduling_accepts_immediately_and_delivers_earlier_due_messages_first()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(clock: clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
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
            Capabilities = new()
            {
                OrderedByKey = true,
                MaxDeliveryDelay = TimeSpan.FromHours(1)
            },
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

        await transport.StopAsync(default).WaitAsync(s_limit);
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
            Capabilities = new()
            {
                OrderedByKey = true,
                MaxDeliveryDelay = TimeSpan.FromHours(1)
            },
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
            await transport.StopAsync(default).WaitAsync(s_limit);
            // All admission credits remain available even if stop left a wake-up in the channel.
            for (var j = 0; j < partitions; j++)
            {
                await Send(transport, j, "key", at: clock.GetUtcNow().AddMinutes(1)).AsTask().WaitAsync(s_limit);
            }

            using var cancellation = new CancellationTokenSource();
            var blocked = Send(transport, -1, "key", at: clock.GetUtcNow().AddMinutes(1), cancellationToken: cancellation.Token);
            await Assert.That(blocked.IsCompleted).IsFalse();
            cancellation.Cancel();
            await Assert.That(async () => await blocked).Throws<OperationCanceledException>();
            clock.Advance(TimeSpan.FromMinutes(1));
            await transport.StartAsync(default);
            var seen = new bool[partitions];

            for (var j = 0; j < partitions; j++)
            {
                seen[await Next(values)] = true;
            }

            await transport.StopAsync(default).WaitAsync(s_limit);
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
            Capabilities = new()
            {
                MaxDeliveryDelay = TimeSpan.FromDays(3)
            },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
        await Send(transport, 1, at: clock.GetUtcNow().AddDays(2));
        await transport.StartAsync(default);
        await clock.NextTimer();
        await transport.StopAsync(default).WaitAsync(s_limit);
        clock.Advance(TimeSpan.FromHours(12)); // The stopped run's timer must not control the next run.
        await transport.StartAsync(default);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromDays(1)); // Deadlines longer than one day re-arm without delivering early.
        await clock.NextTimer();
        await Assert.That(values.Reader.TryRead(out _)).IsFalse();
        clock.Advance(TimeSpan.FromHours(12));
        await Assert.That(await Next(values)).IsEqualTo(1);
        await transport.StopAsync(default).WaitAsync(s_limit);
        await Assert.That(clock.TimersCreated).IsEqualTo(2);
    }

    [Test]
    public async Task Unordered_queue_redelivery_does_not_stall_ready_messages_with_a_full_buffer()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capacity = 2
        }, clock);
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
        await entered.Task.WaitAsync(s_limit);
        await Send(transport, 2);
        release.SetResult();
        await Assert.That(await Next(values)).IsEqualTo(21);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(await Next(values)).IsEqualTo(12);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Unordered_queue_rejects_unschedulable_manual_redelivery_before_settling()
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromSeconds(10);
        clock.Advance(DateTimeOffset.MaxValue - clock.GetUtcNow() - delay);
        await using var transport = Transport(clock: clock);
        var completed = Signal();
        transport.RegisterConsumer("orders", async (delivery, _) =>
        {
            await Assert.That(() => delivery.Redeliver(TimeSpan.MaxValue)).Throws<InvalidOperationException>()
                .WithMessageContaining("MaxRedeliveryDelay");
            clock.Advance(TimeSpan.FromTicks(1));
            await Assert.That(() => delivery.Redeliver(delay)).Throws<InvalidOperationException>()
                .WithMessageContaining("clock");
            delivery.Complete();
            completed.SetResult();
            return Disposition.SettledByHandler;
        });

        await transport.StartAsync(default);
        await Send(transport, 1);
        var first = await Task.WhenAny(completed.Task, transport.Completion).WaitAsync(s_limit);
        await first;
        await transport.StopAsync(default);
        await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
    }

    [Test]
    public async Task Unordered_queue_rejects_a_returned_redelivery_beyond_the_clock_range()
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromSeconds(10);
        clock.Advance(DateTimeOffset.MaxValue - clock.GetUtcNow() - delay);
        await using var transport = Transport(clock: clock);
        transport.RegisterConsumer("orders", (_, _) =>
        {
            clock.Advance(TimeSpan.FromTicks(1));
            return new(Disposition.Redeliver(delay));
        });

        await transport.StartAsync(default);
        await Send(transport, 1);
        await Assert.That(async () => await transport.Completion.WaitAsync(s_limit)).Throws<InvalidOperationException>()
            .WithMessageContaining("clock");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Unordered_queue_redelivery_accepts_the_last_representable_deadline(bool manual)
    {
        var clock = new InMemoryTestClock();
        var delay = TimeSpan.FromSeconds(10);
        clock.Advance(DateTimeOffset.MaxValue - clock.GetUtcNow() - delay);
        await using var transport = Transport(clock: clock);
        var redelivered = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            if (delivery.Attempt == 1)
            {
                if (manual)
                {
                    delivery.Redeliver(delay);
                    clock.Advance(TimeSpan.FromSeconds(1));
                    return new(Disposition.SettledByHandler);
                }

                return new(Disposition.Redeliver(delay));
            }

            redelivered.SetResult(clock.GetUtcNow());
            return new(Disposition.Complete);
        });

        await Assert.That(transport.GetCapabilities("orders").MaxRedeliveryDelay).IsEqualTo(delay);
        await transport.StartAsync(default);
        await Send(transport, 1);
        await clock.NextTimer();
        clock.Advance(DateTimeOffset.MaxValue - clock.GetUtcNow());
        await Assert.That(await redelivered.Task.WaitAsync(s_limit)).IsEqualTo(DateTimeOffset.MaxValue);
        await transport.StopAsync(default);
    }

    [Test]
    public async Task Scheduling_and_redelivery_respect_exact_capability_boundaries()
    {
        var clock = new InMemoryTestClock();
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                MaxDeliveryDelay = TimeSpan.FromMinutes(1),
                MaxRedeliveryDelay = TimeSpan.FromMinutes(1)
            },
        }, clock);
        await Send(transport, 1, at: clock.GetUtcNow().AddMinutes(1));
        await Assert.That(async () => await Send(transport, 2, at: clock.GetUtcNow().AddMinutes(1).AddTicks(1))).Throws<InvalidOperationException>();
        transport.RegisterConsumer("orders", static (_, _) => new(Disposition.Redeliver(TimeSpan.FromMinutes(1).Add(TimeSpan.FromTicks(1)))));
        await transport.StartAsync(default);
        await clock.NextTimer();
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(async () => await transport.Completion.WaitAsync(s_limit)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Dispose_unblocks_sends_waiting_for_native_capacity()
    {
        await using var transport = Transport(new()
        {
            Capacity = 1
        });
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

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                }
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
        await entered.Task.WaitAsync(s_limit);
        using var cancellation = new CancellationTokenSource();
        var stop = transport.StopAsync(cancellation.Token);
        cancellation.Cancel();

        try
        {
            await Assert.That(async () => await stop.WaitAsync(s_limit)).Throws<OperationCanceledException>();
            await unwinding.Task.WaitAsync(s_limit);
            await Assert.That(transport.Completion.IsCompleted).IsFalse();
            await Assert.That(() => transport.StartAsync(default)).Throws<InvalidOperationException>();
        }
        finally
        {
            release.TrySetResult();
        }

        await transport.Completion.WaitAsync(s_limit);
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

            try
            {
                await transport.StopAsync(default).WaitAsync(s_limit);
            }
            catch
            {
                // Unblock a stranded read so a failure cannot hang asynchronous disposal.
                for (var receiver = 0; receiver < 32; receiver++)
                {
                    await Send(transport, receiver);
                }

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

        async ValueTask<bool> ReadAndRelayWakeup()
        {
            var message = await reader.ReadAsync(stop.Token);
            return reader.IsWakeup(message, stop.Token);
        }

        var first = ReadAndRelayWakeup();
        var second = ReadAndRelayWakeup();
        stop.Cancel();
        var bothCompleted = first.IsCompleted && second.IsCompleted;
        // Release the old implementation's stranded reader before asserting.
        channel.Writer.TryWrite(new(true));
        await Assert.That(await first).IsTrue();
        await Assert.That(await second).IsTrue();
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
        await using var transport = Transport(new()
        {
            Capacity = 1
        });
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer(
            "orders",
            (delivery, _) =>
            {
                values.Writer.TryWrite(Value(delivery));
                return new(Disposition.Complete);
            },
            concurrency: 32
        );

        for (var iteration = 0; iteration < 32; iteration++)
        {
            await Send(transport, iteration).AsTask().WaitAsync(s_limit);
            await transport.StartAsync(default);
            await Assert.That(await Next(values)).IsEqualTo(iteration);
            await transport.StopAsync(default).WaitAsync(s_limit);
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
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxBatchSize = 4,
                MaxDeliveryDelay = TimeSpan.FromHours(1)
            },
        }, clock);
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer(
            "orders",
            (delivery, _) =>
            {
                values.Writer.TryWrite(Value(delivery));
                return new(Disposition.Complete);
            },
            log ? "workers" : null
        );
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

        for (var value = 1; value <= 4; value++)
        {
            await Assert.That(await Next(values)).IsEqualTo(value);
        }

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

            if (!ExecutionContext.IsFlowSuppressed())
            {
                throw new InvalidOperationException("Caller suppression was restored early.");
            }
        }

        await start;
        await Send(transport, 1);
        await Assert.That(await seen.Task.WaitAsync(s_limit)).IsNull();
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
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxDeliveryDelay = TimeSpan.FromHours(1)
            },
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
            await entered.Task.WaitAsync(s_limit);
            // Fill the partition, plus the group queue and fan-out pending slot for logs.
            var last = log ? 8 : 4;

            for (var i = 2; i <= last; i++)
            {
                await Send(transport, i, "a").AsTask().WaitAsync(s_limit);
            }

            await Send(transport, 20, "a", clock.GetUtcNow().AddMinutes(1));
            await clock.NextTimer();
            await Send(transport, 21, "b", clock.GetUtcNow().AddMinutes(2));
            await clock.NextTimer();
            clock.Advance(TimeSpan.FromMinutes(2));
            await Assert.That(await Next(values)).IsEqualTo(21);
            var stop = transport.StopAsync(default);
            release.SetResult();
            await stop.WaitAsync(s_limit);
            await transport.StartAsync(default);

            for (var i = 1; i <= last; i++)
            {
                await Assert.That(await Next(values)).IsEqualTo(i);
            }

            await Assert.That(await Next(values)).IsEqualTo(20);
        }
        finally
        {
            release.TrySetResult();
        }

        await transport.StopAsync(default);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_batch_sends_preserve_key_order(bool log)
    {
        await using var transport = Transport(new()
        {
            Capabilities = new()
            {
                ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                OrderedByKey = true,
                MaxBatchSize = 3
            },
        });
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer(
            "orders",
            (delivery, _) =>
            {
                values.Writer.TryWrite(Value(delivery));
                return new(Disposition.Complete);
            },
            log ? "workers" : null
        );
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
        await using var transport = Transport(new()
        {
            Capacity = 1
        });
        var messages = new OutgoingMessage[] { new("orders", BitConverter.GetBytes(1)), new("orders", BitConverter.GetBytes(2)) };
        using var cancellation = new CancellationTokenSource();
        var batch = transport.SendBatchAsync(messages, cancellation.Token);
        await Assert.That(batch.IsCompleted).IsFalse();
        cancellation.Cancel();
        await Assert.That(async () => await batch).Throws<OperationCanceledException>();
        var values = Channel.CreateUnbounded<int>();
        transport.RegisterConsumer("orders", (delivery, _) =>
        {
            values.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
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
        first.RegisterConsumer("orders", (delivery, _) =>
        {
            firstValues.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
        second.RegisterConsumer("orders", (delivery, _) =>
        {
            secondValues.Writer.TryWrite(Value(delivery));
            return new(Disposition.Complete);
        });
        await first.StartAsync(default);
        await second.StartAsync(default);
        await Send(first, 1);
        await Send(second, 2);
        await Assert.That(await Next(firstValues)).IsEqualTo(1);
        await Assert.That(await Next(secondValues)).IsEqualTo(2);
    }

    [Test]
    public async Task Hosted_startup_can_retry_after_a_later_consumer_fails_composition()
    {
        var received = Channel.CreateUnbounded<int>();
        int compositionAttempts = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", local =>
            {
                local.Options.Configure(options => options.Endpoints.Add("events", new()
                {
                    Capabilities = new()
                    {
                        ConsumptionModel = ConsumptionModel.Log,
                        DeadLetter = true,
                    },
                }));
                local.Send<InMemoryOrder>("events");
                local.Consume("events", consumer =>
                {
                    consumer.Options.Group = "worker";
                    consumer.Handler<InMemoryOrderHandler>();
                });
                local.Consume("orders", consumer =>
                {
                    consumer.Handler<InMemoryOrderHandler>();
                    consumer.ConfigurePipeline(_ =>
                    {
                        if (++compositionAttempts == 1)
                        {
                            throw new InvalidOperationException("orders composition failed");
                        }
                    });
                });
            });
        });
        using IHost host = builder.Build();
        var transport = (InMemoryTransport)host.Services.GetRequiredService<ITransport>();

        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>()
            .WithMessageContaining("orders composition failed");
        await Assert.That(() => transport.GetCommittedPosition("events", "worker", 0))
            .Throws<InvalidOperationException>().WithMessageContaining("unknown group");

        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(42));

        await Assert.That(await Next(received)).IsEqualTo(42);
        await host.StopAsync();
        await Assert.That(compositionAttempts).IsEqualTo(2);
        await Assert.That(transport.GetCommittedPosition("events", "worker", 0)).IsEqualTo(0);
    }

    [Test]
    public async Task Hosted_startup_can_retry_cancellation_before_registering_receivers()
    {
        using var cancellation = new CancellationTokenSource();
        var received = Channel.CreateUnbounded<int>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", local =>
            {
                local.Options.Configure(options => options.Endpoints.Add("orders", new()
                {
                    Capabilities = new()
                    {
                        ConsumptionModel = ConsumptionModel.Log,
                        DeadLetter = true,
                    },
                }));
                local.Send<InMemoryOrder>("orders");
                local.Consume("orders", consumer =>
                {
                    consumer.Options.Group = "billing";
                    consumer.Handler<InMemoryOrderHandler>();
                    consumer.ConfigurePipeline(_ => cancellation.Cancel());
                });
            });
        });
        using IHost host = builder.Build();
        var transport = (InMemoryTransport)host.Services.GetRequiredService<ITransport>();

        await Assert.That(() => host.StartAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(() => transport.GetCommittedPosition("orders", "billing", 0))
            .Throws<InvalidOperationException>().WithMessageContaining("unknown group");

        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(42));

        await Assert.That(await Next(received)).IsEqualTo(42);
        await host.StopAsync();
        await Assert.That(transport.GetCommittedPosition("orders", "billing", 0)).IsEqualTo(0);
    }

    [Test]
    public async Task Hosted_startup_can_retry_after_a_later_named_transport_fails_composition()
    {
        var received = Channel.CreateUnbounded<int>();
        int compositionAttempts = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("first", first => first.Consume("events", consumer => consumer.Handler<InMemoryOrderHandler>()));
            relayer.InMemory("second", second =>
            {
                second.Send<InMemoryOrder>("orders");
                second.Consume("orders", consumer =>
                {
                    consumer.Handler<InMemoryOrderHandler>();
                    consumer.ConfigurePipeline(_ =>
                    {
                        if (++compositionAttempts == 1)
                        {
                            throw new InvalidOperationException("orders composition failed");
                        }
                    });
                });
            });
        });
        using IHost host = builder.Build();
        var transport = host.Services.GetServices<ITransport>().OfType<InMemoryTransport>()
            .Single(transport => transport.Name == "first");

        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>()
            .WithMessageContaining("orders composition failed");
        var firstRun = transport.Completion;
        await Assert.That(firstRun.IsCompleted).IsFalse();

        await host.StartAsync();
        await Assert.That(transport.Completion).IsSameReferenceAs(firstRun);
        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(42));

        await Assert.That(await Next(received)).IsEqualTo(42);
        await host.StopAsync();
        await Assert.That(compositionAttempts).IsEqualTo(2);
    }

    [Test]
    [Arguments("orders", true)]
    [Arguments("outgoing", false)]
    public async Task Hosted_startup_checks_custom_cloud_events_headers_only_on_consumed_endpoints(
        string destination, bool consumed)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(Channel.CreateUnbounded<int>());
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", local =>
            {
                local.Send<InMemoryOrder>(destination, route => route.SendOptions = new()
                {
                    HeaderNames = new("events_"),
                });
                local.Consume("orders", consumer => consumer.Handler<InMemoryOrderHandler>());
            });
        });
        using IHost host = builder.Build();

        if (consumed)
        {
            var error = await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains("orders").And.Contains("events_type").And.Contains("ce_type");
        }
        else
        {
            await host.StartAsync();
            using var scope = host.Services.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new InMemoryOrder(42));
            await host.StopAsync();
        }
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
            foreach (var service in provider.GetServices<IHostedService>())
            {
                await service.StartAsync(default);
            }
        }).Throws<InvalidOperationException>().WithMessageContaining("DeadLetterEndpoint");
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

        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>())
        {
            await lifecycle.StartingAsync(default);
        }

        foreach (var service in hosted)
        {
            await service.StartAsync(default);
        }

        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new InMemoryOrder(-1));
        await sender.SendAsync(new InMemoryOrder(42));
        await Assert.That(await Next(received)).IsEqualTo(42);
        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        await Assert.That(native.DeadLetters("orders").TryRead(out _)).IsFalse();

        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(default);
        }
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
        if (message.Id < 0)
        {
            throw new InvalidOperationException("poison order");
        }

        received.Writer.TryWrite(message.Id);
        return default;
    }
}

public sealed class InMemoryFollowUpOrderHandler(ISender sender) : IHandler<InMemoryOrder>
{
    public ValueTask HandleAsync(InMemoryOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        return message.Id == 1 ? sender.SendAsync(new InMemoryOrder(2), cancellationToken) : ValueTask.CompletedTask;
    }
}

public sealed class NativeSettlementFaultOrderHandler(Channel<int> received, InvalidOperationException error) : IHandler<InMemoryOrder>
{
    public ValueTask HandleAsync(InMemoryOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Id >= 0)
        {
            received.Writer.TryWrite(message.Id);
            return default;
        }

        received.Writer.TryWrite(context.Attempt);
        var delivery = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Message;

        if (message.Id == -2)
        {
            delivery.DeadLetter(new("manual"));
        }
        else
        {
            delivery.Complete();
        }

        throw error;
    }
}

public sealed class ManualSettlementOrderHandler(Channel<int> received) : IHandler<InMemoryOrder>
{
    public ValueTask HandleAsync(InMemoryOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        var settlement = context.Features.GetRequired<ISettlementFeature>();

        if (message.Id == 1 && context.Attempt == 1)
        {
            settlement.Retry(TimeSpan.Zero);
        }
        else if (message.Id == 2)
        {
            settlement.DeadLetter("manual order");
        }
        else
        {
            settlement.Complete();
        }

        received.Writer.TryWrite(message.Id * 10 + context.Attempt);
        return default;
    }
}
