using System.Diagnostics;
using System.Text;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Tests;

// These call sites are the low-level send seam. Transport-specific builders can reuse RouteBuilder<TMessage, TRouteOptions>.
public class SenderTests
{
    [Test]
    public async Task Routes_by_message_to_the_selected_named_transport_and_writes_the_envelope()
    {
        var first = new RecordingTransport("first");
        var second = new RecordingTransport("second");
        using var provider = CreateProvider(builder =>
        {
            builder.DeclareRoute<OrderPlaced>("second", "orders", options: new() { Source = "/billing", HeaderNames = new("cloudEvents:") });
            builder.DeclareRoute<OrderCancelled>("first", "cancellations");
        }, first, second);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var id = Guid.NewGuid();
        await sender.SendAsync(new OrderPlaced(id));
        await sender.SendAsync(new OrderCancelled(id));

        await Assert.That(second.Destination).IsEqualTo("orders");
        await Assert.That(first.Destination).IsEqualTo("cancellations");
        await Assert.That(second.Body).Contains(id.ToString());
        await Assert.That(second.Headers["cloudEvents:type"]).IsEqualTo("tests.order-placed;v=2");
        await Assert.That(second.Headers["cloudEvents:source"]).IsEqualTo("/billing");
        await Assert.That(second.Headers["cloudEvents:specversion"]).IsEqualTo("1.0");
        await Assert.That(second.Headers["content-type"]).IsEqualTo("application/json");
        await Assert.That(Guid.Parse(second.Headers["cloudEvents:id"]).Version).IsEqualTo(7);
        await Assert.That(second.Headers.ContainsKey("ce_type")).IsFalse();
    }

    [Test]
    public async Task Unknown_route_fails_with_an_actionable_message()
    {
        using var provider = CreateProvider(_ => { });
        using var scope = provider.CreateScope();
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("Send<TMessage>");
    }

    [Test]
    public async Task Applies_headers_schedule_ordering_and_native_configuration_without_leaking_options()
    {
        var transport = new RecordingTransport("events");
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders", route => route.OrderingKey(static _ => "route-key")), transport);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var deliverAt = DateTimeOffset.UtcNow.AddMinutes(3);
        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()), options =>
        {
            options.Headers["tenant"] = "tenant-1";
            options.DeliverAt = deliverAt;
            options.OrderingKey = "override";
            options.ConfigureNative<NativeOutgoing>(static native => native.Value = 42);
        });
        var firstId = transport.Headers["ce_id"];
        await Assert.That(transport.Headers["tenant"]).IsEqualTo("tenant-1");
        await Assert.That(transport.DeliverAt).IsEqualTo(deliverAt);
        await Assert.That(transport.OrderingKey).IsEqualTo("override");
        await Assert.That(transport.NativeValue).IsEqualTo(42);

        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
        await Assert.That(transport.OrderingKey).IsEqualTo("route-key");
        await Assert.That(transport.DeliverAt).IsNull();
        await Assert.That(transport.NativeValue).IsEqualTo(0);
        await Assert.That(transport.Headers.ContainsKey("tenant")).IsFalse();
        await Assert.That(transport.Headers["ce_id"] == firstId).IsFalse();
    }

    [Test]
    public async Task Native_configuration_for_the_wrong_type_fails_through_the_sender()
    {
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), new RecordingTransport("events"));
        using var scope = provider.CreateScope();
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()),
            options => options.ConfigureNative<StringBuilder>(static _ => { })))
            .Throws<InvalidOperationException>().WithMessageContaining("native outgoing type");
    }

    [Test]
    public async Task Middleware_runs_in_registration_order_and_can_change_options()
    {
        var calls = new List<string>();
        var transport = new RecordingTransport("events");
        var services = Services(transport);
        services.AddSingleton<ISendMiddleware>(new RecordingMiddleware("one", calls));
        services.AddSingleton<ISendMiddleware>(new RecordingMiddleware("two", calls));
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.DeclareRoute<OrderPlaced>("events", "orders");
        });
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()));

        await Assert.That(calls).IsEquivalentTo(new[] { "one-before", "two-before", "two-after", "one-after" });
        await Assert.That(string.Join(",", calls)).IsEqualTo("one-before,two-before,two-after,one-after");
        await Assert.That(transport.Headers["middleware"]).IsEqualTo("two");
    }

    [Test]
    public async Task Middleware_retry_reuses_identity_without_appending_another_serialized_body()
    {
        var transport = new RecordingTransport("events") { FailOnce = true };
        using var provider = CreateProvider(builder =>
        {
            builder.DeclareRoute<OrderPlaced>("events", "orders");
            builder.UseSendMiddleware<RetryOnceMiddleware>();
        }, transport);
        using var scope = provider.CreateScope();
        var id = Guid.NewGuid();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(id));
        await Assert.That(transport.Body).IsEqualTo("{\"OrderId\":\"" + id + "\"}");
        await Assert.That(transport.Count).IsEqualTo(2);
        await Assert.That(transport.Headers["ce_id"]).IsEqualTo(transport.FailedMessageId!);
    }

    [Test]
    public async Task Times_out_a_cancelable_transport_and_recovers_after_cancellation()
    {
        var transport = new RecordingTransport("events") { WaitForCancellation = true };
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders", options: new() { Timeout = TimeSpan.FromSeconds(1) }), transport);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await Assert.That(async () => await sender.SendAsync(new OrderPlaced(Guid.NewGuid())).AsTask().WaitAsync(TimeSpan.FromSeconds(10)))
            .Throws<TimeoutException>().WithMessageContaining("route timeout");
        await Assert.That(transport.ObservedToken.IsCancellationRequested).IsTrue();
        transport.WaitForCancellation = false;
        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
        await Assert.That(transport.ObservedToken.IsCancellationRequested).IsFalse();
    }

    [Test]
    public async Task Caller_cancellation_stays_cancellation_and_does_not_become_a_timeout()
    {
        var transport = new RecordingTransport("events") { WaitForCancellation = true };
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), transport);
        using var scope = provider.CreateScope();
        using var cancellation = new CancellationTokenSource();
        var send = scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()), cancellation.Token);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        await Assert.That(async () => await send).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Already_cancelled_send_never_enters_the_transport()
    {
        var transport = new RecordingTransport("events");
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), transport);
        using var scope = provider.CreateScope();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var send = scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()), cancellation.Token);
        await Assert.That(send.IsCanceled).IsTrue();
        await Assert.That(async () => await send)
            .Throws<OperationCanceledException>();
        await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
    }

    [Test]
    public async Task Active_scoped_outbox_enlists_automatically_and_explicit_direct_send_bypasses_it()
    {
        var transport = new RecordingTransport("events");
        var services = Services(transport);
        services.AddScoped<IOutboxEnlistment, TestOutbox>();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.DeclareRoute<OrderPlaced>("events", "orders");
        });
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var firstScope = provider.CreateScope();
        using var secondScope = provider.CreateScope();
        var outbox = (TestOutbox)firstScope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        outbox.IsActive = true;
        var sender = firstScope.ServiceProvider.GetRequiredService<ISender>();
        var id = Guid.NewGuid();
        var deliverAt = DateTimeOffset.UtcNow.AddMinutes(5);
        await sender.SendAsync(new OrderPlaced(id), options =>
        {
            options.Headers["tenant"] = "1";
            options.DeliverAt = deliverAt;
            options.OrderingKey = "outbox-key";
        });
        await Assert.That(outbox.Body).Contains(id.ToString());
        await Assert.That(outbox.Headers["tenant"]).IsEqualTo("1");
        await Assert.That(outbox.Headers["ce_type"]).IsEqualTo("tests.order-placed;v=2");
        await Assert.That(outbox.DeliverAt).IsEqualTo(deliverAt);
        await Assert.That(outbox.OrderingKey).IsEqualTo("outbox-key");
        await sender.SendViaOutboxAsync(new OrderPlaced(Guid.NewGuid()));

        await Assert.That(outbox.Count).IsEqualTo(2);
        await Assert.That(outbox.TransportName).IsEqualTo("events");
        await Assert.That(outbox.Destination).IsEqualTo("orders");
        await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
        await sender.SendNowAsync(new OrderPlaced(Guid.NewGuid()));
        await secondScope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()));
        await Assert.That(transport.Count).IsEqualTo(2);
        await Assert.That(outbox.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Required_outbox_without_an_active_transaction_fails()
    {
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), new RecordingTransport("events"));
        using var scope = provider.CreateScope();
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendViaOutboxAsync(new OrderPlaced(Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("active transaction");
    }

    [Test]
    public async Task Concurrent_sends_keep_borrowed_buffers_and_native_options_until_each_send_completes()
    {
        var transport = new GatedTransport();
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("gated", "orders"), transport);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var first = sender.SendAsync(new OrderPlaced(firstId), options =>
        {
            options.Headers["tenant"] = "first";
            options.ConfigureNative<NativeOutgoing>(static native => native.Value = 1);
        });
        var second = sender.SendAsync(new OrderPlaced(secondId), options =>
        {
            options.Headers["tenant"] = new string('x', 10000);
            options.ConfigureNative<NativeOutgoing>(static native => native.Value = 2);
        });
        try
        {
            var firstCopy = Encoding.UTF8.GetString(transport.Messages[0].Body.Span);
            await Assert.That(firstCopy).Contains(firstId.ToString());
            await Assert.That(Encoding.UTF8.GetString(transport.Messages[1].Body.Span)).Contains(secondId.ToString());
            var native = new NativeOutgoing();
            transport.Messages[0].ApplyNative(native);
            await Assert.That(native.Value).IsEqualTo(1);
            transport.Messages[1].ApplyNative(native);
            await Assert.That(native.Value).IsEqualTo(2);
            await Assert.That(transport.Messages[0].Headers.ToArray().Single(header => header.Key == "tenant").Value.ToString()).IsEqualTo("first");
        }
        finally
        {
            transport.Release.TrySetResult();
            await first;
            await second;
        }
        await Assert.That(transport.CompletedBodies[0]).Contains(firstId.ToString());
        await Assert.That(transport.CompletedBodies[1]).Contains(secondId.ToString());
    }

    [Test]
    public async Task Completed_send_unregisters_caller_cancellation_and_configuration_failure_does_not_poison_the_pool()
    {
        var transport = new RecordingTransport("events");
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), transport);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        using var cancellation = new CancellationTokenSource();
        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()), cancellation.Token);
        cancellation.Cancel();
        await Assert.That(async () => await sender.SendAsync(new OrderPlaced(Guid.NewGuid()), static options =>
        {
            options.Headers["leak"] = "bad";
            throw new InvalidOperationException("configuration failure");
        })).Throws<InvalidOperationException>().WithMessageContaining("configuration failure");
        await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
        await Assert.That(transport.Headers.ContainsKey("leak")).IsFalse();
        await Assert.That(transport.ObservedToken.IsCancellationRequested).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Direct_send_has_zero_steady_state_allocations_with_the_default_serializer(bool scalarKey)
    {
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("sink", "orders", route =>
        {
            if (scalarKey) route.OrderingKey(static message => message.OrderId);
        }), new SinkTransport());
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var message = new OrderPlaced(Guid.NewGuid());
        for (var i = 0; i < 1000; i++) await sender.SendAsync(message);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await sender.SendAsync(message);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Overlapping_asynchronous_sends_retain_the_peak_pool_and_allocate_zero_bytes_after_warmup()
    {
        // Cross the first pool segment, and complete before consumption to exercise both lifetimes separately.
        var concurrency = Environment.ProcessorCount + 1;
        var transport = new ManualCompletionTransport(concurrency);
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("manual", "orders"), transport);
        using var scope = provider.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var message = new OrderPlaced(Guid.NewGuid());
        var pending = new ValueTask[concurrency];
        for (var batch = 0; batch < 20; batch++)
        {
            transport.BeginBatch();
            for (var i = 0; i < concurrency; i++) pending[i] = sender.SendAsync(message);
            transport.CompleteBatch();
            for (var i = 0; i < concurrency; i++) await pending[i];
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var batch = 0; batch < 20; batch++)
        {
            transport.BeginBatch();
            for (var i = 0; i < concurrency; i++) pending[i] = sender.SendAsync(message);
            transport.CompleteBatch();
            for (var i = 0; i < concurrency; i++) await pending[i];
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Route_builders_freeze_with_configuration()
    {
        RouteBuilder<OrderPlaced, SendRouteOptions>? route = null;
        IRelayerBuilder? captured = null;
        new ServiceCollection().AddRelayer(builder =>
        {
            captured = builder;
            builder.DeclareRoute<OrderPlaced>("events", "orders", value => route = value);
        });
        await Assert.That(() => route!.OrderingKey(static _ => "late")).Throws<InvalidOperationException>();
        await Assert.That(() => captured!.DeclareRoute<OrderCancelled>("events", "other")).Throws<InvalidOperationException>();
        await Assert.That(() => captured!.UseSendMiddleware<RetryOnceMiddleware>()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Outbox_rejects_native_callbacks_before_enlistment()
    {
        var services = Services(new RecordingTransport("events"));
        services.AddScoped<IOutboxEnlistment, TestOutbox>();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.DeclareRoute<OrderPlaced>("events", "orders");
        });
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var outbox = (TestOutbox)scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        outbox.IsActive = true;
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()),
            options => options.ConfigureNative<NativeOutgoing>(static _ => { })))
            .Throws<InvalidOperationException>().WithMessageContaining("cannot be persisted");
        await Assert.That(outbox.Count).IsEqualTo(0);
    }

    [Test]
    [Arguments("unknown")]
    [Arguments("duplicate-route")]
    [Arguments("duplicate-transport")]
    [Arguments("timeout")]
    [Arguments("identity")]
    [Arguments("source")]
    [Arguments("headers")]
    [Arguments("mode")]
    public async Task Invalid_routes_fail_at_host_startup(string error)
    {
        var transport = new RecordingTransport("events");
        var services = Services(transport);
        if (error == "duplicate-transport")
        {
            services.AddSingleton<ITransport>(new RecordingTransport("events"));
        }
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<SenderTransportOptions, SendRouteOptions, SenderTransportOptions>(error == "unknown" ? "missing" : "events", broker =>
            {
                broker.Send<OrderPlaced>("orders", route => route.SendOptions =
                    new()
                    {
                        Timeout = error == "timeout" ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(30),
                        Source = error == "source" ? " " : "/",
                        HeaderNames = error == "headers" ? null! : new(),
                        EnvelopeMode = error == "mode" ? (MessageEnvelopeMode)99 : MessageEnvelopeMode.CloudEvents
                    });
                if (error == "duplicate-route") broker.Send<OrderPlaced>("other");
                if (error == "identity") broker.Send<string>("other");
            });
        });
        using var provider = services.BuildServiceProvider();
        Func<Task> start = async () =>
        {
            foreach (var service in provider.GetServices<IHostedService>()) await service.StartAsync(default);
        };
        if (error is not ("unknown" or "duplicate-transport"))
            await Assert.That(start).Throws<OptionsValidationException>();
        else
            await Assert.That(start).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments("orders.standard", "string")]
    [Arguments("orders.standard", "scalar")]
    [Arguments("orders.standard", "none")]
    [Arguments("orders.fifo", "string")]
    [Arguments("orders.fifo", "scalar")]
    [Arguments("orders.fifo", "none")]
    public async Task Startup_validates_ordering_against_the_destination_capabilities(string destination, string key)
    {
        var transport = new RecordingTransport("events") { UnorderedEndpoint = "orders.standard" };
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", destination, route =>
        {
            if (key == "string") route.OrderingKey(static _ => "customer");
            if (key == "scalar") route.OrderingKey(static message => message.OrderId);
        }), transport);
        Func<Task> start = async () =>
        {
            foreach (var service in provider.GetServices<IHostedService>()) await service.StartAsync(default);
        };

        if (destination == "orders.standard" && key != "none")
        {
            await Assert.That(start).Throws<OptionsValidationException>()
                .WithMessageContaining("Send route 'tests.order-placed;v=2' on transport 'events' to endpoint 'orders.standard' declares an OrderingKey, but the endpoint does not support OrderedByKey. Configure an endpoint with native ordering or remove the OrderingKey declaration.");
            await Assert.That(transport.Count).IsEqualTo(0);
        }
        else
        {
            await start();
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
            await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
        }

        await Assert.That(transport.CapabilityEndpoints).IsEquivalentTo(new[] { destination });
    }

    [Test]
    public async Task Raw_mode_preserves_custom_headers_and_writes_no_envelope()
    {
        var transport = new RecordingTransport("events");
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders", options: new() { EnvelopeMode = MessageEnvelopeMode.Raw }), transport);
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()), options => options.Headers["ce_type"] = "legacy");
        await Assert.That(transport.Headers["ce_type"]).IsEqualTo("legacy");
        await Assert.That(transport.Headers.ContainsKey("ce_id")).IsFalse();
    }

    [Test]
    public async Task Reserved_headers_cannot_replace_message_identity_or_creation_context()
    {
        using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), new RecordingTransport("events"));
        using var scope = provider.CreateScope();
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new OrderPlaced(Guid.NewGuid()),
            options => options.Headers["ce_id"] = "replacement"))
            .Throws<InvalidOperationException>().WithMessageContaining("cannot override");
    }

    [Test]
    public async Task Creation_context_does_not_leak_from_a_previous_send()
    {
        var previous = Activity.Current;
        Activity.Current = null;
        try
        {
            var transport = new RecordingTransport("events");
            using var provider = CreateProvider(builder => builder.DeclareRoute<OrderPlaced>("events", "orders"), transport);
            using var scope = provider.CreateScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            using (var activity = new Activity("producer").SetIdFormat(ActivityIdFormat.W3C).Start())
            {
                activity.TraceStateString = "vendor=value";
                await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
                await Assert.That(transport.Headers["traceparent"]).IsEqualTo(activity.Id!);
                await Assert.That(transport.Headers["tracestate"]).IsEqualTo("vendor=value");
            }
            await sender.SendAsync(new OrderPlaced(Guid.NewGuid()));
            await Assert.That(transport.Headers.ContainsKey("traceparent")).IsFalse();
            await Assert.That(transport.Headers.ContainsKey("tracestate")).IsFalse();
        }
        finally
        {
            Activity.Current = previous;
        }
    }

    private static ServiceProvider CreateProvider(Action<IRelayerBuilder> configure, params ITransport[] transports)
    {
        var services = Services(transports);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            configure(builder);
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ServiceCollection Services(params ITransport[] transports)
    {
        var services = new ServiceCollection();
        foreach (var transport in transports) services.AddSingleton(transport);
        return services;
    }

    private sealed class NativeOutgoing { public int Value { get; set; } }

    private sealed class GatedTransport : ITransport
    {
        public string Name => "gated";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public List<OutgoingMessage> Messages { get; } = [];
        public string[] CompletedBodies { get; } = new string[2];
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            var index = Messages.Count;
            Messages.Add(message);
            await Release.Task.WaitAsync(cancellationToken);
            CompletedBodies[index] = Encoding.UTF8.GetString(message.Body.Span);
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SinkTransport : ITransport
    {
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ManualCompletionTransport(int concurrency) : ITransport
    {
        private readonly Completion[] _completions = Enumerable.Range(0, concurrency).Select(static _ => new Completion()).ToArray();
        private int _next;
        public string Name => "manual";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public void BeginBatch()
        {
            _next = 0;
            foreach (var completion in _completions) completion.Reset();
        }
        public void CompleteBatch()
        {
            // Reverse completion also prevents reliance on send order.
            for (var i = _next - 1; i >= 0; i--) _completions[i].Complete();
        }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => _completions[_next++].Task;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class Completion : IValueTaskSource
        {
            private ManualResetValueTaskSourceCore<bool> _source;
            public ValueTask Task => new(this, _source.Version);
            public void Reset() => _source.Reset();
            public void Complete() => _source.SetResult(true);
            public void GetResult(short token) => _source.GetResult(token);
            public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
                _source.OnCompleted(continuation, state, token, flags);
        }
    }

    private sealed class RecordingTransport(string name) : ITransport
    {
        public string Name => name;
        public string? UnorderedEndpoint { get; init; }
        public List<string> CapabilityEndpoints { get; } = [];
        public TransportCapabilities GetCapabilities(string endpoint)
        {
            CapabilityEndpoints.Add(endpoint);
            return new() { MaxDeliveryDelay = TimeSpan.MaxValue, OrderedByKey = endpoint != UnorderedEndpoint };
        }
        public string? Destination { get; private set; }
        public string? Body { get; private set; }
        public string? OrderingKey { get; private set; }
        public DateTimeOffset? DeliverAt { get; private set; }
        public Dictionary<string, string> Headers { get; } = [];
        public int NativeValue { get; private set; }
        public int Count { get; private set; }
        public bool WaitForCancellation { get; set; }
        public bool FailOnce { get; set; }
        public string? FailedMessageId { get; private set; }
        public CancellationToken ObservedToken { get; private set; }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            ObservedToken = cancellationToken;
            Entered.TrySetResult();
            if (WaitForCancellation) return new(Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken));
            var native = new NativeOutgoing();
            message.ApplyNative(native);
            NativeValue = native.Value;
            Destination = message.Destination;
            Body = Encoding.UTF8.GetString(message.Body.Span);
            OrderingKey = message.OrderingKey?.ToString();
            DeliverAt = message.DeliverAt;
            Headers.Clear();
            foreach (var header in message.Headers.Span) Headers.Add(header.Key, header.Value.ToString());
            Count++;
            if (FailOnce)
            {
                FailOnce = false;
                FailedMessageId = Headers["ce_id"];
                throw new InvalidOperationException("retryable send failure");
            }
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RecordingMiddleware(string name, List<string> calls) : ISendMiddleware
    {
        public async ValueTask InvokeAsync(SendContext context, SendDelegate next)
        {
            calls.Add(name + "-before");
            context.Options.Headers["middleware"] = name;
            await next(context);
            calls.Add(name + "-after");
        }
    }

    public sealed class RetryOnceMiddleware : ISendMiddleware
    {
        public async ValueTask InvokeAsync(SendContext context, SendDelegate next)
        {
            try { await next(context); }
            catch (InvalidOperationException) { await next(context); }
        }
    }

    private sealed class TestOutbox : IOutboxEnlistment
    {
        public bool IsActive { get; set; }
        public int Count { get; private set; }
        public string? TransportName { get; private set; }
        public string? Destination { get; private set; }
        public string? Body { get; private set; }
        public string? OrderingKey { get; private set; }
        public DateTimeOffset? DeliverAt { get; private set; }
        public Dictionary<string, string> Headers { get; } = [];
        public ValueTask EnlistAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken)
        {
            Count++;
            TransportName = transportName;
            Destination = message.Destination;
            Body = Encoding.UTF8.GetString(message.Body.Span);
            OrderingKey = message.OrderingKey?.ToString();
            DeliverAt = message.DeliverAt;
            Headers.Clear();
            foreach (var header in message.Headers.Span) Headers.Add(header.Key, header.Value.ToString());
            return ValueTask.CompletedTask;
        }
    }
}

internal static class SenderTestRoutes
{
    internal static void DeclareRoute<TMessage>(this IRelayerBuilder builder, string transportName, string destination,
        Action<RouteBuilder<TMessage, SendRouteOptions>>? configure = null, SendRouteOptions? options = null) =>
        builder.AddTransport<SenderTransportOptions, SendRouteOptions, SenderTransportOptions>(transportName,
            transport => transport.Send<TMessage>(destination, route =>
            {
                route.SendOptions = options ?? new();
                configure?.Invoke(route);
            }));
}

public sealed class SenderTransportOptions;
