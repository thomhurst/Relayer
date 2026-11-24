using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsExecutionTests
{
    [Test]
    [Arguments(".dead", null, ".dead", false, true)]
    [Arguments(".*", null, ".dead", false, true)]
    [Arguments(".>", null, ".dead.failure", false, true)]
    [Arguments(".created", ".dead.*", ".dead.failure", false, true)]
    [Arguments(null, null, ".dead", false, true)]
    [Arguments(".created", null, ".dead", false, false)]
    [Arguments(".created", ".updated", ".dead", false, false)]
    [Arguments(".*", null, ".dead.failure", false, false)]
    [Arguments(".>", null, "", false, false)]
    [Arguments(null, null, "_dead", true, false)]
    [Arguments(">", null, "_dead", true, false)]
    public async Task Hosted_start_rejects_dead_letter_subjects_the_native_consumer_reads(
        string? filter, string? secondFilter, string deadSuffix, bool separateStream, bool rejects)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("events");
        var root = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var sendSubject = root + ".created";
        var deadSubject = root + deadSuffix;
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateStreamAsync(new(stream, [root, root + ".>"]), token);

        if (separateStream)
        {
            await js.CreateStreamAsync(new(stream + "_dead", [deadSubject]), token);
        }

        await js.CreateConsumerAsync(stream, new("worker")
        {
            FilterSubject = secondFilter is null ? Filter(filter) : null,
            FilterSubjects = secondFilter is null ? null : [Filter(filter)!, root + secondFilter]
        }, token);

        var host = CreateDeadLetterValidationHost(broker.Options(), stream, sendSubject, deadSubject);
        await using var lifetime = (IAsyncDisposable)host;

        if (rejects)
        {
            var error = await Assert.That(() => host.StartAsync(token)).Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains($"{stream}/worker");
            await Assert.That(error.Message).Contains(deadSubject);
            await Assert.That(error.Message).Contains("Narrow");
        }
        else
        {
            await host.StartAsync(token);
            await host.StopAsync(token);
        }

        string? Filter(string? suffix)
        {
            return suffix is null or ">" ? suffix : root + suffix;
        }
    }

    [Test]
    [Arguments(".dead", ".created", ".created", "can receive")]
    [Arguments(".*", ".created", ".created", "can receive")]
    [Arguments(".dead", ".archive", ".dead", null)]
    [Arguments(".dead", ".$archive", ".dead", null)]
    [Arguments(".updated", ".created", ".created", null)]
    [Arguments(".*", ".{{Wildcard(1)}}", ".created", "cannot validate")]
    [Arguments(".*", ".$1", ".created", "cannot validate")]
    [Arguments(".dead", ".{{Random(8)}}", ".created", "cannot validate")]
    [Arguments(null, ".archive.>", ".created", "cannot validate")]
    public async Task Hosted_start_validates_native_dead_letter_subject_transforms(
        string? sourceSuffix, string destinationSuffix, string filterSuffix, string? expectedError)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var stream = isolation.GetIsolatedName("events");
        var root = isolation.GetIsolatedName("orders");
        var sendSubject = root + ".created";
        var deadSubject = root + ".dead";
        var destination = root + destinationSuffix;
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateStreamAsync(new(stream, [root + ".>"])
        {
            SubjectTransform = new()
            {
                Src = sourceSuffix is null ? null : root + sourceSuffix,
                Dest = destination
            }
        }, token);
        await js.CreateConsumerAsync(stream, new("worker")
        {
            FilterSubject = root + filterSuffix
        }, token);

        var host = CreateDeadLetterValidationHost(broker.Options(), stream, sendSubject, deadSubject);
        await using var lifetime = (IAsyncDisposable)host;

        if (expectedError is null)
        {
            await host.StartAsync(token);
            await host.StopAsync(token);
            return;
        }

        var error = await Assert.That(() => host.StartAsync(token)).Throws<InvalidOperationException>();
        await Assert.That(error!.Message).Contains($"{stream}/worker");
        await Assert.That(error.Message).Contains(deadSubject);
        await Assert.That(error.Message).Contains(destination);
        await Assert.That(error.Message).Contains(expectedError);
    }

    private static IHost CreateDeadLetterValidationHost(NatsOpts options, string stream, string sendSubject, string deadSubject)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.None;
                transport.Options.Configure(transportOptions =>
                {
                    transportOptions.ConnectionOptions = options;
                    transportOptions.Consumers.Add(new(stream, "worker"));
                });
                transport.Send<ExecutionMessage>(sendSubject);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = stream;
                    consumer.Failures(new()
                    {
                        RetryDelays = [],
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        return builder.Build();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Hosted_startup_retry_registers_one_configured_worker_per_durable(bool failNativeStart)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var isolation = TestContext.Current!.Isolation;
        var stream = isolation.GetIsolatedName("startup_retry");
        var subject = isolation.GetIsolatedName("orders");
        var deadLetter = isolation.GetIsolatedName("dead");
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateStreamAsync(new(stream, [subject]), token);
        await js.CreateStreamAsync(new(stream + "_dead", [deadLetter]), token);
        await js.CreateConsumerAsync(stream, new("first"), token);

        if (!failNativeStart)
        {
            await js.CreateConsumerAsync(stream, new("second"), token);
        }

        var compositionAttempts = 0;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.None;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Consumers.Add(new(stream, "first"));
                    options.Consumers.Add(new(stream, "second"));
                });

                foreach (var durable in new[] { "first", "second" })
                {
                    transport.Consume(durable, consumer =>
                    {
                        consumer.Options.Stream = stream;
                        consumer.Failures(new()
                        {
                            DeadLetterEndpoint = deadLetter
                        });
                        consumer.Handler<ConfirmedAckHandler>();

                        if (durable == "second")
                        {
                            consumer.ConfigurePipeline(_ =>
                            {
                                if (++compositionAttempts == 1 && !failNativeStart)
                                {
                                    throw new InvalidOperationException("Transient composition failure.");
                                }
                            });
                        }
                    });
                }
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        var transport = host.Services.GetRequiredKeyedService<NatsNativeTransport>("execution");

        if (failNativeStart)
        {
            await Assert.That(() => host.StartAsync(token)).Throws<NatsJSApiException>();
            await Assert.That(transport.Consumers.Count).IsEqualTo(2);
            await Assert.That(transport.Consumers.All(worker => worker.Completion.IsCompletedSuccessfully)).IsTrue();
            await js.CreateConsumerAsync(stream, new("second"), token);
        }
        else
        {
            await Assert.That(() => host.StartAsync(token)).Throws<InvalidOperationException>()
                .WithMessageContaining("Transient composition failure.");
            await Assert.That(transport.Consumers).IsEmpty();
        }

        var composed = transport.Consumers.ToArray();
        await host.StartAsync(token);
        await Assert.That(transport.Consumers.Count).IsEqualTo(2);
        await Assert.That(transport.Consumers.All(worker => worker.Receive is not null && !worker.Completion.IsCompleted)).IsTrue();
        await Assert.That(compositionAttempts).IsEqualTo(failNativeStart ? 1 : 2);

        if (failNativeStart)
        {
            await Assert.That(transport.Consumers[0]).IsSameReferenceAs(composed[0]);
            await Assert.That(transport.Consumers[1]).IsSameReferenceAs(composed[1]);
        }

        await host.StopAsync(token);
    }

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
    public async Task Stop_drains_later_consumers_while_earlier_handler_is_blocked()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("stop_events");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("stop_orders");
        var firstSubject = subject + ".first";
        var secondSubject = subject + ".second";
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateStreamAsync(new(stream, [firstSubject, secondSubject]), token);
        await js.CreateConsumerAsync(stream, new("first")
        {
            FilterSubject = firstSubject
        }, token);
        await js.CreateConsumerAsync(stream, new("second")
        {
            FilterSubject = secondSubject
        }, token);

        await using var provider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Consumers.Add(new(stream, "first"));
            options.Consumers.Add(new(stream, "second"));
        });
        var transport = (NatsNativeTransport)provider.GetRequiredService<ITransport>();
        var settings = new NatsJSConsumeOpts { MaxMsgs = 1, DrainOnCancel = true };
        var first = transport.CreateConsumer(stream, "first", settings, 1);
        var second = transport.CreateConsumer(stream, "second", settings, 1);
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        first.Receive = async (_, _) =>
        {
            firstEntered.TrySetResult();
            await releaseFirst.Task.WaitAsync(token);
            return Disposition.Complete;
        };
        second.Receive = (_, _) =>
        {
            secondEntered.TrySetResult();
            return ValueTask.FromResult(Disposition.Complete);
        };

        transport.Consumers.AddRange([first, second]);
        await transport.StartAsync(token);
        var body = new byte[] { 0 };
        var serializer = NatsRawSerializer<byte[]>.Default;
        Task? stopping = null;

        try
        {
            (await js.PublishAsync(firstSubject, body, serializer, cancellationToken: token)).EnsureSuccess();
            (await js.PublishAsync(secondSubject, body, serializer, cancellationToken: token)).EnsureSuccess();
            await Task.WhenAll(firstEntered.Task, secondEntered.Task).WaitAsync(token);

            stopping = transport.StopAsync(token);
            // Wait for the native drain fence before publishing fresh work to the second subject.
            await second.Completion.WaitAsync(TimeSpan.FromSeconds(10), token);
            await Assert.That(stopping.IsCompleted).IsFalse();
            var published = await js.PublishAsync(secondSubject, body, serializer, cancellationToken: token);
            published.EnsureSuccess();

            // The publish ACK can arrive before JetStream updates the consumer's pending count.
            var reader = await js.GetConsumerAsync(stream, "second", token);
            var next = await reader.NextAsync(serializer, new()
            {
                Expires = TimeSpan.FromSeconds(5)
            }, token);
            await Assert.That(next).IsNotNull();
            next!.EnsureSuccess();
            var metadata = next.Metadata!.Value;
            await Assert.That(metadata.Sequence.Stream).IsEqualTo(published.Seq);
            await Assert.That(metadata.NumDelivered).IsEqualTo(1UL);
            await next.AckAsync(new()
            {
                DoubleAck = true
            }, token);
        }
        finally
        {
            releaseFirst.TrySetResult();

            if (stopping is not null)
            {
                await stopping;
            }
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Restart_after_cancelled_offline_stop_replaces_the_drained_worker(bool drainCompleted)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        await using var broker = await NatsTopologyTests.Broker.StartAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("restart_events");
        var subject = TestContext.Current!.Isolation.GetIsolatedName("restart_orders");
        await using var provider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new(stream)
            {
                Configuration = new(stream, [subject])
            });
            options.Consumers.Add(new(stream, "worker")
            {
                Configuration = new("worker")
                {
                    FilterSubject = subject
                }
            });
        });
        var adapter = (NatsNativeTransport)provider.GetRequiredService<Relayer.Transports.ITransport>();
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var worker = adapter.CreateConsumer(stream, "worker", new()
        {
            MaxMsgs = 1,
            DrainOnCancel = true
        }, 1);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Receive = async (delivery, processing) =>
        {
            if (delivery.Message.Data.Span[0] == 0)
            {
                entered.TrySetResult();
                // Keep native drain incomplete until StopAsync has returned cancellation.
                await release.Task.WaitAsync(token);
                processing.ThrowIfCancellationRequested();
            }
            else
            {
                recovered.TrySetResult();
            }

            return Disposition.Complete;
        };

        adapter.Consumers.Add(worker);

        try
        {
            await adapter.StartAsync(token);
            (await js.PublishAsync(subject, new byte[] { 0 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
            await entered.Task.WaitAsync(token);
            await broker.SetConnectedAsync(false, token);
            using var stop = new CancellationTokenSource();
            var stopping = adapter.StopAsync(stop.Token);
            stop.Cancel();
            await Assert.That(async () => await stopping).Throws<OperationCanceledException>();
            await Assert.That(worker.Completion.IsCompleted).IsFalse();
            await broker.SetConnectedAsync(true, token);

            if (drainCompleted)
            {
                release.TrySetResult();
                // Observe the native worker reaching its terminal state before the restart.
                try
                {
                    await worker.Completion.WaitAsync(token);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                }
            }
            else
            {
                // A canceled restart must not lose the unfinished shutdown state.
                using var start = new CancellationTokenSource();
                var starting = adapter.StartAsync(start.Token);
                await Assert.That(starting.IsCompleted).IsFalse();
                start.Cancel();
                await Assert.That(async () => await starting).Throws<OperationCanceledException>();
                release.TrySetResult();
            }

            await adapter.StartAsync(token);
            await Assert.That(worker.Completion.IsCompleted).IsFalse();
            (await js.PublishAsync(subject, new byte[] { 4 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
            await Task.WhenAny(recovered.Task, worker.Completion).WaitAsync(token);

            if (worker.Completion.IsCompleted)
            {
                await worker.Completion;
            }

            await recovered.Task.WaitAsync(token);
            await adapter.StopAsync(token);
        }
        finally
        {
            release.TrySetResult();
        }
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
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events)
                    {
                        Configuration = new(events, [orders, deadSubject])
                    });
                    options.Consumers.Add(new(events, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = orders
                        }
                    });
                });
                transport.Send<ExecutionMessage>(orders);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Options.Settings = new()
                    {
                        MaxMsgs = 1,
                        Expires = TimeSpan.FromSeconds(1),
                        DrainOnCancel = true
                    };
                    consumer.Failures(new()
                    {
                        RetryDelays = new[] { TimeSpan.Zero },
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(events, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
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
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(new ExecutionMessage(42),
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

        await copy.AckAsync(new()
        {
            DoubleAck = true
        }, token);
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(2UL);
        await Assert.That(control.Attempts).IsEqualTo(2);
    }

    [Test]
    public async Task Harness_dispatch_retains_typed_poison_copy_identity_without_a_send_route()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("events");
        var orders = TestContext.Current!.Isolation.GetIsolatedName("orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("dead");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("execution", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(stream)
                    {
                        Configuration = new(stream, [orders, deadSubject])
                    });
                    options.Consumers.Add(new(stream, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = orders
                        }
                    });
                });
                transport.Send<ExecutionMessage>(orders);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = stream;
                    consumer.Failures(new()
                    {
                        RetryDelays = [],
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ConfirmedAckHandler>();
                });
            });
        });
        builder.Services.AddRelayerHarness();

        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var dead = await js.CreateConsumerAsync(stream, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
        var poison = new ExecutionMessage(13); // ConfirmedAckHandler throws for values other than 42.
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(poison, cancellationToken: token);

        var copy = await dead.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await copy!.AckAsync(new()
        {
            DoubleAck = true
        }, token);
        var harness = host.Services.GetRequiredService<RelayerHarness>();
        await harness.WaitForIdleAsync(token);
        await host.StopAsync(token);

        var dispatched = harness.Dispatched;
        var original = dispatched.Single(item => item.Destination == orders);
        var deadLetter = dispatched.Single(item => item.Destination == deadSubject);
        await Assert.That(original.MessageId).IsNotNull();
        await Assert.That(original.WireType).IsNotNull();
        await Assert.That(copy.Headers!["ce-id"].ToString()).IsEqualTo(original.MessageId);
        await Assert.That(copy.Headers["ce-type"].ToString()).IsEqualTo(original.WireType);
        await Assert.That(deadLetter.MessageId).IsEqualTo(original.MessageId);
        await Assert.That(deadLetter.WireType).IsEqualTo(original.WireType);
        await Assert.That(harness.GetDispatched<ExecutionMessage>()).IsEquivalentTo(new[] { poison, poison });
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
                    options.Streams.Add(new(events)
                    {
                        Configuration = new(events, separateDeadStream ? [orders] : [orders, deadSubject])
                    });

                    if (separateDeadStream)
                    {
                        options.Streams.Add(new(deadStream)
                        {
                            Configuration = new(deadStream, [deadSubject])
                        });
                    }

                    options.Consumers.Add(new(events, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = orders
                        }
                    });
                });
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(deadStream, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
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
        {
            await Assert.That(header.Key.StartsWith("Nats-Expected-", StringComparison.OrdinalIgnoreCase)).IsFalse();
        }

        await Assert.That(copy.Headers.ContainsKey("Nats-Msg-Id")).IsFalse();
        await copy.AckAsync(new()
        {
            DoubleAck = true
        }, token);
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
            relayer.Nats("rollup", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events)
                    {
                        Configuration = new(events, [orders])
                        {
                            AllowRollupHdrs = true
                        }
                    });
                    options.Streams.Add(new(deadStream)
                    {
                        Configuration = new(deadStream, [deadSubject])
                        {
                            AllowRollupHdrs = true
                        }
                    });
                    options.Consumers.Add(new(events, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = orders
                        }
                    });
                });
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        await js.CreateConsumerAsync(deadStream, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
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
        await previous.AckAsync(new()
        {
            DoubleAck = true
        }, token);
        var copy = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(poison);
        await Assert.That(copy.Headers!["application"].ToString()).IsEqualTo("preserved-value");
        await Assert.That((await js.GetStreamAsync(deadStream, cancellationToken: token)).Info.State.Messages).IsEqualTo(2L);
        await Assert.That(copy.Headers.ContainsKey("Nats-Rollup")).IsFalse();
        await copy.AckAsync(new()
        {
            DoubleAck = true
        }, token);
        // Graceful stop waits for the original delivery's confirmed ACK after the durable copy.
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(1UL);
    }

    [Test]
    public async Task Poison_copy_uses_dead_letter_stream_retention()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var events = TestContext.Current!.Isolation.GetIsolatedName("ttl_events");
        var deadStream = events + "_dead";
        var orders = TestContext.Current!.Isolation.GetIsolatedName("ttl_orders");
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("ttl_dead");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new ExecutionControl());
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("ttl", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(events)
                    {
                        Configuration = new(events, [orders])
                        {
                            AllowMsgTTL = true
                        }
                    });
                    options.Streams.Add(new(deadStream)
                    {
                        Configuration = new(deadStream, [deadSubject])
                    });
                    options.Consumers.Add(new(events, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = orders
                        }
                    });
                });
                transport.Send<ExecutionMessage>(orders);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Failures(new()
                    {
                        RetryDelays = [],
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var reader = await js.CreateConsumerAsync(deadStream, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
        var message = new ExecutionMessage(42);
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(message,
            options => options.Nats(headers =>
            {
                headers["Nats-TTL"] = "10m";
                headers["ce-id"] = "ttl-message";
                headers["custom"] = "native";
            }), token);

        var copy = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, new()
        {
            Expires = TimeSpan.FromSeconds(5)
        }, token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(JsonSerializer.SerializeToUtf8Bytes(message, NatsExecutionJson.Default.ExecutionMessage));
        await Assert.That(copy.Headers!.ContainsKey("Nats-TTL")).IsFalse();
        await Assert.That(copy.Headers["ce-id"].ToString()).IsEqualTo("ttl-message");
        await Assert.That(copy.Headers["custom"].ToString()).IsEqualTo("native");
        await Assert.That(copy.Headers[FailureHeaders.Attempts].ToString()).IsEqualTo("1");
        using var history = JsonDocument.Parse(copy.Headers[FailureHeaders.History].ToString());
        await Assert.That(history.RootElement.GetProperty("OriginalMessageId").GetString()).IsEqualTo("ttl-message");
        await Assert.That(history.RootElement.GetProperty("Attempts")[0].GetProperty("Message").GetString())
            .IsEqualTo("Expected in-process retry.");

        // Graceful stop waits for the original delivery's confirmed ACK after the durable copy.
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(settled.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(settled.Info.AckFloor.ConsumerSeq).IsEqualTo(1UL);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Poison_copy_drops_native_schedule_controls(bool purgeSchedule)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var stream = TestContext.Current!.Isolation.GetIsolatedName("schedule_events");
        var deadStream = stream + "_dead";
        var schedule = TestContext.Current!.Isolation.GetIsolatedName("schedule_definition");
        var target = schedule + ".target";
        var deadSubject = schedule + ".dead";
        var source = purgeSchedule ? target : schedule;
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new ExecutionControl());
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("schedule", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options();
                    options.Streams.Add(new(stream)
                    {
                        Configuration = new(stream, [schedule, target])
                        {
                            AllowMsgSchedules = true,
                            AllowMsgTTL = true
                        }
                    });
                    options.Streams.Add(new(deadStream)
                    {
                        Configuration = new(deadStream, [deadSubject])
                    });
                    options.Consumers.Add(new(stream, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = source
                        }
                    });
                });
                transport.Send<ExecutionMessage>(source);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = stream;
                    consumer.Failures(new()
                    {
                        RetryDelays = [],
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ExecutionHandler>();
                });
            });
        });
        var host = builder.Build();
        await using var lifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var reader = await js.CreateConsumerAsync(deadStream, new("dead_reader")
        {
            FilterSubject = deadSubject
        }, token);
        var message = new ExecutionMessage(42);
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(message,
            options => options.Nats(headers =>
            {
                headers["ce-id"] = "scheduled-message";
                headers["custom"] = "native";

                if (purgeSchedule)
                {
                    headers["Nats-Schedule-Next"] = "purge";
                    headers["Nats-Scheduler"] = schedule;
                }
                else
                {
                    headers["Nats-Schedule"] = $"@at {DateTimeOffset.UtcNow.AddHours(1):O}";
                    headers["Nats-Schedule-Target"] = target;
                    headers["nats-schedule-ttl"] = "10m";
                }
            }), token);

        var copy = await reader.NextAsync(NatsRawSerializer<byte[]>.Default, new()
        {
            Expires = TimeSpan.FromSeconds(5)
        }, token);
        await Assert.That(copy).IsNotNull();
        await Assert.That(copy!.Data).IsEquivalentTo(JsonSerializer.SerializeToUtf8Bytes(message, NatsExecutionJson.Default.ExecutionMessage));
        var headers = copy.Headers!;
        await Assert.That(headers.ContainsKey("Nats-Schedule")).IsFalse();
        await Assert.That(headers.ContainsKey("Nats-Schedule-Target")).IsFalse();
        await Assert.That(headers.ContainsKey("nats-schedule-ttl")).IsFalse();
        await Assert.That(headers.ContainsKey("Nats-Schedule-Next")).IsFalse();
        await Assert.That(headers.ContainsKey("Nats-Scheduler")).IsFalse();
        await Assert.That(headers["ce-id"].ToString()).IsEqualTo("scheduled-message");
        await Assert.That(headers["custom"].ToString()).IsEqualTo("native");
        await Assert.That(headers[FailureHeaders.Attempts].ToString()).IsEqualTo("1");
        await Assert.That((await js.GetStreamAsync(deadStream, cancellationToken: token)).Info.State.Messages).IsEqualTo(1L);

        // Graceful stop waits for the original delivery's confirmed ACK after the durable copy.
        await host.StopAsync(token);
        var settled = await js.GetConsumerAsync(stream, "worker", token);
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
        var deadSubject = TestContext.Current!.Isolation.GetIsolatedName("ack_dead");
        var capture = new AckFailureCapture();
        using var logs = LoggerFactory.Create(logging => logging.SetMinimumLevel(LogLevel.Debug).AddProvider(capture));
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(logs);
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(NatsExecutionJson.Default);
            relayer.Nats("ack_failure", transport =>
            {
                transport.Options.Configure(options =>
                {
                    options.ConnectionOptions = broker.Options() with
                    {
                        LoggerFactory = logs
                    };
                    options.Streams.Add(new(events)
                    {
                        Configuration = new(events, [subject, deadSubject])
                    });
                    options.Consumers.Add(new(events, "worker")
                    {
                        Configuration = new("worker")
                        {
                            FilterSubject = subject,
                            AckWait = TimeSpan.FromMilliseconds(200)
                        }
                    });
                });
                transport.Send<ExecutionMessage>(subject);
                transport.Consume("worker", consumer =>
                {
                    consumer.Options.Stream = events;
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = deadSubject
                    });
                    consumer.Handler<ConfirmedAckHandler>();
                });
            });
        });
        services.AddRelayerHarness();
        await using var provider = services.BuildServiceProvider();
        var harness = provider.GetRequiredService<RelayerHarness>();
        var adapter = provider.GetRequiredKeyedService<NatsNativeTransport>("ack_failure");
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        var worker = adapter.CreateConsumer(events, "worker", new()
        {
            MaxMsgs = 1,
            DrainOnCancel = true,
            Expires = TimeSpan.FromSeconds(1)
        }, 2);
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var processErrors = new ConcurrentQueue<string?>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.process.duration")
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "error.type")
                {
                    processErrors.Enqueue((string?)tag.Value);
                    return;
                }
            }

            processErrors.Enqueue(null);
        });
        listener.Start();

        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer("ack_failure"),
            static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<ExecutionMessage, ConfirmedAckHandler>())
            .Settle(worker.SettleContextAsync)
            .Telemetry(instrumentation.CreateEndpoint(new()
            {
                System = "nats",
                Destination = events,
                Subscription = "worker"
            }))
            .Build(2);
        var receive = new ReceivePipeline<NatsDelivery, NatsBinding>(pipeline, new("ce-"), 2);
        var failedAckCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redeliveryReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        bool matched = false;
        var consumed = harness.WaitForConsumedAsync<ExecutionMessage>(_ =>
        {
            matched = true;
            return true;
        }, token);
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
                    var message = delivery.Message;
                    var failed = new NatsJSMsg<ReadOnlyMemory<byte>>(new NatsMsg<ReadOnlyMemory<byte>>(
                        subject, subject + ".no_ack_receiver", message.Size, message.Headers, message.Data, message.Connection),
                        new NatsJSContext(message.Connection!));
                    var disposition = await receive.ProcessAsync(new(failed, delivery.Consumer), processing);
                    await Assert.That(disposition.Kind).IsEqualTo(DispositionKind.SettledByHandler);
                    // The redelivery cannot process until this callback releases it.
                    await Assert.That(matched).IsFalse();
                    IReadOnlyList<HarnessFailure> failures = harness.Failed;
                    await Assert.That(failures.Count).IsEqualTo(1);
                    await Assert.That(failures[0].Operation).IsEqualTo(HarnessOperation.Consume);
                    await Assert.That(failures[0].Error).IsSameReferenceAs(await capture.Failure.Task.WaitAsync(processing));
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
                    var disposition = await receive.ProcessAsync(delivery, processing);
                    await Assert.That(disposition.Kind).IsEqualTo(DispositionKind.SettledByHandler);
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
        adapter.Consumers.Add(worker);
        await adapter.StartAsync(token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(new ExecutionMessage(42), token);

        try
        {
            await WaitForStageAsync(failedAckCompleted.Task, "failed ACK callback");
            await Assert.That(capture.Failure.Task.IsCompletedSuccessfully).IsTrue();
            await Assert.That(await capture.Failure.Task).IsTypeOf<NatsNoRespondersException>();
            await WaitForStageAsync(redelivered.Task, "confirmed native redelivery");
            await Assert.That(await consumed).IsEqualTo(new ExecutionMessage(42));
            await adapter.StopAsync(token);

            if (callbackFailure.Task.IsCompleted)
            {
                await callbackFailure.Task;
            }

            var consumer = await js.GetConsumerAsync(events, "worker", token);
            await Assert.That(consumer.Info.NumAckPending).IsEqualTo(0L);
            // At-least-once delivery permits more redeliveries when a runner is delayed past AckWait.
            await Assert.That(calls).IsGreaterThanOrEqualTo(2);
            string?[] errors = processErrors.ToArray();
            await Assert.That(errors.Length).IsEqualTo(calls);
            await Assert.That(errors[0]).IsEqualTo("_OTHER");
            await Assert.That(errors.Skip(1).All(error => error is null)).IsTrue();
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
            catch (Exception diagnosticError)
            {
                consumerState = diagnosticError.ToString();
            }

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
            {
                throw new InvalidOperationException($"Worker stopped before {name}.");
            }
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
                options.Streams.Add(new(events)
                {
                    Configuration = new(events, [subject])
                });
                options.Consumers.Add(new(events, "worker")
                {
                    Configuration = new("worker")
                    {
                        AckWait = TimeSpan.FromMilliseconds(200)
                    }
                });
            });
            transport.Send<ExecutionMessage>(subject);
        }));
        await using var provider = services.BuildServiceProvider();
        var adapter = (NatsNativeTransport)provider.GetRequiredService<Relayer.Transports.ITransport>();
        await adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
        await using var native = new NatsConnection(broker.Options());
        var js = new NatsJSContext(native);
        var worker = adapter.CreateConsumer(events, "worker", new()
        {
            MaxMsgs = 1,
            DrainOnCancel = true,
            Expires = TimeSpan.FromSeconds(1)
        }, 2);
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
                    if (unsupportedSettlement)
                    {
                        return Disposition.Redeliver(TimeSpan.Zero);
                    }
                    // A real failure-copy publication to a subject outside every stream fails.
                    // Leave the original unacknowledged so native AckWait can redeliver it.
                    (await js.PublishAsync(missing, delivery.Message.Data,
                        NatsRawSerializer<ReadOnlyMemory<byte>>.Default, cancellationToken: processing)).EnsureSuccess();
                    throw new InvalidOperationException("Expected native publication to fail.");
                }
                finally
                {
                    initialAttemptFinished.TrySetResult();
                }
            }

            redeliveryReceived.TrySetResult();
            await initialAttemptFinished.Task.WaitAsync(processing);
            // Confirm settlement before signalling: stopping the pull loop must not race
            // the assertion, and native at-least-once delivery may include queued duplicates.
            await worker.SettleAsync(delivery, Disposition.Complete, processing);

            if (delivery.Message.Metadata!.Value.NumDelivered > 1)
            {
                redelivered.TrySetResult();
            }

            return Disposition.SettledByHandler;
        };
        adapter.Consumers.Add(worker);
        await adapter.StartAsync(token);
        (await js.PublishAsync(subject, new byte[] { 42 }, NatsRawSerializer<byte[]>.Default, cancellationToken: token)).EnsureSuccess();
        await Task.WhenAny(redelivered.Task, worker.Completion).WaitAsync(token);

        if (worker.Completion.IsCompleted)
        {
            await worker.Completion;
        }

        await redelivered.Task.WaitAsync(token);
        await adapter.StopAsync(token);
        var consumer = await js.GetConsumerAsync(events, "worker", token);
        await Assert.That(consumer.Info.NumAckPending).IsEqualTo(0L);
        await Assert.That(calls).IsGreaterThanOrEqualTo(2);
    }

    private sealed class AckFailureCapture : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<string> _entries = new();

        internal TaskCompletionSource<Exception> Failure
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ILogger CreateLogger(string categoryName)
        {
            return this;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Dispose()
        {
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _entries.Enqueue($"{logLevel} {eventId.Name}: {formatter(state, exception)} {exception}");

            if (eventId.Name == "AckFailed" && exception is not null)
            {
                Failure.TrySetResult(exception);
            }
        }

        public override string ToString()
        {
            return string.Join(Environment.NewLine, _entries);
        }
    }
}

[Message("nats.execution")]
public sealed record ExecutionMessage(int Value);

public sealed class ExecutionControl
{
    public int Attempts;

    public TaskCompletionSource Completed
    {
        get;
    } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class ExecutionHandler(ExecutionControl control) : IHandler<ExecutionMessage>
{
    public ValueTask HandleAsync(ExecutionMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var native = context.Nats();

        if (message.Value != 42 || native.Message.Headers!["custom"].ToString() != "native")
        {
            throw new InvalidOperationException("Native payload/header mapping changed.");
        }

        if (++control.Attempts == 1)
        {
            throw new InvalidOperationException("Expected in-process retry.");
        }

        control.Completed.TrySetResult();
        return default;
    }
}

public sealed class ConfirmedAckHandler : IHandler<ExecutionMessage>
{
    public ValueTask HandleAsync(ExecutionMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Value != 42)
        {
            throw new InvalidOperationException("Unexpected payload in confirmed ACK test.");
        }

        return default;
    }
}

[JsonSerializable(typeof(ExecutionMessage))]
internal partial class NatsExecutionJson : JsonSerializerContext;
