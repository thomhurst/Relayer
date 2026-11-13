using Relayer.Pipeline;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.InMemory;
using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer.Tests;

public class TenancyTests
{
    [Test]
    public async Task Restores_UTF8_before_dependencies_and_transaction_boundary_then_clears_pooled_scope()
    {
        await using var provider = Provider();
        var seen = new List<string>();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            var id = context.Services.GetRequiredService<TenantContext>().Id;
            if (id.Text is not null) throw new InvalidOperationException("Tenant was decoded.");
            seen.Add(id.ToString());
            return default;
        };
        var boundary = new Boundary();
        var receive = Receive(provider, boundary: boundary);
        await receive.ProcessAsync(new(new("東京"u8.ToArray())));
        await receive.ProcessAsync(new(default));
        await Assert.That(seen).IsEquivalentTo(new[] { "東京", "" });
        await Assert.That(boundary.Seen).IsEquivalentTo(seen);
        await Assert.That(provider.GetRequiredService<Control>().ConstructorTenants).IsEquivalentTo(seen);
    }

    [Test]
    public async Task Concurrent_async_deliveries_keep_tenants_isolated()
    {
        await using var provider = Provider();
        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            var tenant = context.Services.GetRequiredService<TenantContext>();
            var original = tenant.Id.ToString();
            if (Interlocked.Increment(ref started) == 2) both.SetResult();
            await both.Task;
            await Assert.That(tenant.Id.ToString()).IsEqualTo(original);
            await context.Services.GetRequiredService<ISender>().SendNowAsync(new Payload());
        };
        var receive = Receive(provider);
        await Task.WhenAll(receive.ProcessAsync(new(new("north"))).AsTask(), receive.ProcessAsync(new(new("south"))).AsTask());
        await Assert.That(provider.GetRequiredService<Sink>().Tenants.Order().SequenceEqual(new[] { "north", "south" })).IsTrue();
    }

    [Test]
    public async Task Retry_restores_tenant_and_discards_failed_attempt_sends()
    {
        await using var provider = Provider();
        var attempts = new List<string>();
        provider.GetRequiredService<Control>().Invoke = async context =>
        {
            var tenant = context.Services.GetRequiredService<TenantContext>();
            attempts.Add(tenant.Id.ToString());
            if (context.Attempt == 1) tenant.Id = new("changed");
            await context.Services.GetRequiredService<ISender>().SendNowAsync(new Payload());
            if (context.Attempt == 1) throw new InvalidOperationException("retry");
        };
        await Receive(provider, retry: true).ProcessAsync(new(new("original")));
        await Assert.That(attempts).IsEquivalentTo(new[] { "original", "original" });
        await Assert.That(provider.GetRequiredService<Sink>().Tenants).IsEquivalentTo(new[] { "original" });
    }

    [Test]
    public async Task Direct_outbox_and_reply_sends_preserve_tenant_and_reject_overrides()
    {
        await using var provider = Provider(outbox: true);
        await using (var scope = provider.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = new("東京"u8.ToArray());
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendNowAsync(new Payload());
            await sender.SendAsync(new Payload());
            await sender.SendViaOutboxAsync(new Payload());
            await Assert.That(async () => await sender.SendNowAsync(new Payload(), o => o.Headers[TenantContext.HeaderName] = "other"))
                .Throws<InvalidOperationException>().WithMessageContaining("scoped tenant");
            await sender.SendNowAsync(new Payload());
        }
        provider.GetRequiredService<Control>().Invoke = context => context.ReplyAsync(new Payload());
        await Receive(provider).ProcessAsync(new(new("reply-tenant")));
        await Assert.That(provider.GetRequiredService<Sink>().Tenants).IsEquivalentTo(new[] { "東京", "東京" });
        await Assert.That(provider.GetRequiredService<Outbox>().Tenants).IsEquivalentTo(new[] { "東京", "東京", "reply-tenant" });
        using var clean = provider.CreateScope();
        await Assert.That(clean.ServiceProvider.GetRequiredService<TenantContext>().Id.IsEmpty).IsTrue();
        await clean.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new Payload());
        await Assert.That(provider.GetRequiredService<Sink>().Tenants.Last()).IsEqualTo("");
    }

    [Test]
    public async Task Registration_is_idempotent_and_provider_owned_and_rejects_batches()
    {
        await using var provider = Provider();
        await using var other = Provider();
        using var scope = provider.CreateScope();
        using var otherScope = other.CreateScope();
        scope.ServiceProvider.GetRequiredService<TenantContext>().Id = new("first");
        await Assert.That(otherScope.ServiceProvider.GetRequiredService<TenantContext>().Id.IsEmpty).IsTrue();
        var definition = new ConsumerDefinition<object>("source", new(), [typeof(BatchHandler)], 1, 2, TimeSpan.FromSeconds(1));
        await Assert.That(() => Builder(provider).BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, BatchHandler>(), definition))
            .Throws<InvalidOperationException>().WithMessageContaining("single-message");
        await Assert.That(() => new ServiceCollection().AddRelayerTenancy(0)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Without_registration_explicit_headers_work_and_receive_keeps_headers_lazy()
    {
        await using var provider = Provider(tenancy: false);
        using var scope = provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new Payload(), o => o.Headers[TenantContext.HeaderName] = "manual");
        await Assert.That(provider.GetRequiredService<Sink>().Tenants).IsEquivalentTo(new[] { "manual" });
        var incoming = new Incoming(new("ignored"));
        await Receive(provider).ProcessAsync(incoming);
        await Assert.That(incoming.TenantReads).IsEqualTo(0);
    }

    [Test]
    public async Task Scoped_request_client_and_reply_use_the_same_tenant_headers()
    {
        using var host = new Microsoft.Extensions.Hosting.HostBuilder().ConfigureServices(services =>
        {
            services.AddRelayerTenancy();
            services.AddRelayer(r =>
            {
                r.AddMessageTypesFromThisAssembly();
                r.InMemory("local", t =>
                {
                    t.Request<RpcRequest, RpcResponse>("tenant-requests", "tenant-replies");
                    t.Send<RpcResponse>("tenant-replies");
                    t.Consume("tenant-requests", c => c.Handler<TenantEchoHandler>());
                });
            });
        }).Build();
        await host.StartAsync();
        try
        {
            using var scope = host.Services.CreateScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = new("requester");
            var response = await scope.ServiceProvider.GetRequiredService<IRequestClient<RpcRequest, RpcResponse>>()
                .RequestAsync(new(42), TimeSpan.FromSeconds(10));
            await Assert.That(response.Value).IsEqualTo(42);
        }
        finally { await host.StopAsync(); }
    }

    public sealed class TenantEchoHandler(TenantContext tenant) : IHandler<RpcRequest>
    {
        public ValueTask HandleAsync(RpcRequest message, MessageContext context, CancellationToken cancellationToken)
        {
            if (tenant.Id.ToString() != "requester") throw new InvalidOperationException("Request tenant missing.");
            return context.ReplyAsync(new RpcResponse(message.Value), cancellationToken);
        }
    }

    [Test]
    public async Task Handler_failure_disposes_and_clears_tenant_context()
    {
        await using var provider = Provider();
        TenantContext? captured = null;
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            captured = context.Services.GetRequiredService<TenantContext>();
            return ValueTask.FromException(new ApplicationException("failed"));
        };
        await Assert.That(async () => await Receive(provider).ProcessAsync(new(new("failed-tenant"))))
            .Throws<ApplicationException>();
        await Assert.That(() => captured!.Id).Throws<ObjectDisposedException>();
        using var scope = provider.CreateScope();
        await Assert.That(scope.ServiceProvider.GetRequiredService<TenantContext>().Id.IsEmpty).IsTrue();
    }

    [Test]
    public async Task Tenant_receive_allocations_match_equivalent_native_DI_work()
    {
        await using var provider = Provider();
        provider.GetRequiredService<Control>().RecordConstructors = false;
        var receive = Receive(provider);
        var incoming = new Incoming(new("tenant"u8.ToArray()));
        static void NativeScope(ServiceProvider provider, Incoming incoming)
        {
            using var scope = provider.CreateScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = incoming.Tenant;
            _ = scope.ServiceProvider.GetRequiredService<Handler>();
            _ = provider.GetRequiredService<IMessageSerializer>().Deserialize<Payload>(ReadOnlySequence<byte>.Empty);
        }
        for (var i = 0; i < 1000; i++)
        {
            NativeScope(provider, incoming);
            await receive.ProcessAsync(incoming);
        }
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) NativeScope(provider, incoming);
        var nativeBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) await receive.ProcessAsync(incoming);
        // End the measured region before entering assertion infrastructure (including
        // first-use generic initialization), just as for the native control above.
        var receiveBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        await Assert.That(receiveBytes).IsEqualTo(nativeBytes);
    }

    private static ServiceProvider Provider(bool outbox = false, bool tenancy = true)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddSingleton<Sink>();
        services.AddSingleton<ITransport>(static p => p.GetRequiredService<Sink>());
        services.AddSingleton<IMessageSerializer, Serializer>();
        if (tenancy) { services.AddRelayerTenancy(2); services.AddRelayerTenancy(2); }
        if (outbox)
        {
            services.AddSingleton<Outbox>();
            services.AddScoped<IOutboxEnlistment>(static p => p.GetRequiredService<Outbox>());
        }
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>().Handler<BatchHandler>();
            r.AddTransport<object, object>("sink", t => t.Send<Payload>("messages"));
        });
        services.AddScoped(static p => new Handler(p.GetRequiredService<Control>(), p.GetService<TenantContext>()));
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ConsumePipelineBuilder Builder(ServiceProvider p) => new(p.GetRequiredService<IServiceScopeFactory>(),
        p.GetRequiredService<MessageTypeRegistry>(), p.GetRequiredService<IMessageSerializer>(), static (_, error, _) => ValueTask.FromException(error));

    private static ReceivePipeline<Incoming, Binding> Receive(ServiceProvider p, bool retry = false, Boundary? boundary = null)
    {
        var builder = retry ? new ConsumePipelineBuilder(p.GetRequiredService<IServiceScopeFactory>(), p.GetRequiredService<MessageTypeRegistry>(),
            p.GetRequiredService<IMessageSerializer>(), new FailurePipeline(p, "sink", "messages", "tenant-consumer", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 })) : Builder(p);
        builder.Handler(p.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>());
        if (boundary is not null) builder.Use(boundary);
        return new(builder.Build(2), new(), 2);
    }

    [Message("tests.tenant")]
    public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        private readonly Control _control;
        public Handler(Control control, TenantContext? tenant = null)
        {
            _control = control;
            if (control.RecordConstructors)
                lock (control.ConstructorTenants) control.ConstructorTenants.Add(tenant?.Id.ToString() ?? "");
        }
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => _control.Invoke(context);
    }
    public sealed class BatchHandler : IBatchHandler<Payload>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> message, CancellationToken cancellationToken) => ValueTask.FromResult(BatchResult.Success);
    }
    public sealed class Control
    {
        public bool RecordConstructors { get; set; } = true;
        public List<string> ConstructorTenants { get; } = [];
        public Func<MessageContext, ValueTask> Invoke { get; set; } = static _ => default;
    }
    private sealed class Boundary : IConsumeTransactionMiddleware
    {
        internal List<string> Seen { get; } = [];
        public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            Seen.Add(context.Services.GetRequiredService<TenantContext>().Id.ToString());
            return next(context);
        }
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _message = new();
        public string ContentType => "application/json";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_message;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => destination.Write("{}"u8);
    }
    private sealed class Incoming(WireString tenant)
    {
        internal WireString Tenant { get; } = tenant;
        internal int TenantReads;
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => ReadOnlySequence<byte>.Empty;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            if (name == TenantContext.HeaderName) message.TenantReads++;
            value = name switch
            {
                TenantContext.HeaderName => message.Tenant,
                "ce_type" => new("tests.tenant;v=1"), "ce_id" => new("id"),
                "ce_replyto" => new("messages"), "ce_correlationid" => new("correlation"), _ => default
            };
            return !value.IsEmpty;
        }
    }
    private static string ReadTenant(OutgoingMessage message)
    {
        foreach (var header in message.Headers.Span)
            if (header.Key == TenantContext.HeaderName) return header.Value.ToString();
        return "";
    }
    private sealed class Outbox : IOutboxEnlistment
    {
        public bool IsActive => true;
        internal List<string> Tenants { get; } = [];
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            Tenants.Add(ReadTenant(record.Message));
            return default;
        }
    }
    private sealed class Sink : ITransport
    {
        public string Name => "sink";
        internal List<string> Tenants { get; } = [];
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            lock (Tenants) Tenants.Add(ReadTenant(message));
            return default;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
