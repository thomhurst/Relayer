using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class TenantStoreSelectionTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Scoped_native_factories_isolate_business_and_outbox_units(bool separateDatabases)
    {
        await using var stores = new TenantStores();
        await stores.Start(separateDatabases);
        await using (var provider = Services(stores))
        {
            // Hold every transaction open together. Each scope must retain its own native store.
            var remaining = stores.Count;
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var units = new Task[stores.Count];
            for (var i = 0; i < units.Length; i++) units[i] = Save(stores[i].Id);
            await Task.WhenAll(units);
            for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 2, 2, 0);

            async Task Save(string tenant)
            {
                await using var scope = provider.CreateAsyncScope();
                scope.ServiceProvider.GetRequiredService<AuthorizedTenant>().Id = tenant;
                var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
                var sender = scope.ServiceProvider.GetRequiredService<ISender>();
                await using (var transaction = await db.Database.BeginTransactionAsync())
                {
                    db.Add(new Order { Id = 1 });
                    await sender.SendAsync(new Probe(1));
                    await db.SaveChangesAsync();
                    if (Interlocked.Decrement(ref remaining) == 0) release.TrySetResult();
                    await release.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    await transaction.CommitAsync();
                }
                // An unsaved implicit unit and a saved explicit rollback cannot leak either table.
                using (db.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(2));
                await using (var transaction = await db.Database.BeginTransactionAsync())
                {
                    db.Add(new Order { Id = 2 });
                    await sender.SendAsync(new Probe(2));
                    await db.SaveChangesAsync();
                    await transaction.RollbackAsync();
                }
                db.ChangeTracker.Clear();
                using (db.EnlistRelayerOutbox())
                {
                    db.Add(new Order { Id = 2 });
                    await sender.SendAsync(new Probe(3));
                    await db.SaveChangesAsync();
                }
            }
        }
        // The sources belong to the application, not to a scope or a provider using UseNpgsql(source).
        for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 2, 2, 0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Explicit_per_tenant_consumers_isolate_inbox_identity_and_rollback(bool separateDatabases)
    {
        await using var stores = new TenantStores();
        await stores.Start(separateDatabases);
        var sharedId = Guid.CreateVersion7().ToString("D");
        var failedId = Guid.CreateVersion7().ToString("D");
        var providers = new ServiceProvider[stores.Count];
        try
        {
            var pipelines = new ConsumePipeline[stores.Count];
            for (var i = 0; i < stores.Count; i++)
            {
                // A tenant-specific endpoint knows its store even in the composition scope.
                providers[i] = Services(stores, stores[i].Id);
                pipelines[i] = Pipeline(providers[i]);
            }
            await Task.WhenAll(pipelines.Select(pipeline => Deliver(pipeline, sharedId)));
            await Task.WhenAll(pipelines.Select(pipeline => Deliver(pipeline, sharedId)));
            for (var i = 0; i < stores.Count; i++)
            {
                await Verify(stores[i], 1, 1, 1);
                await Assert.That(providers[i].GetRequiredService<Control>().Calls).IsEqualTo(1);
            }

            providers[0].GetRequiredService<Control>().Fail = true;
            await Assert.That(async () => await Deliver(pipelines[0], failedId)).Throws<InvalidOperationException>();
            for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 1, 1, 1);
            providers[0].GetRequiredService<Control>().Fail = false;
            await Deliver(pipelines[0], failedId);
            await Deliver(pipelines[0], failedId);
            await Verify(stores[0], 2, 2, 2);
            for (var i = 1; i < stores.Count; i++) await Verify(stores[i], 1, 1, 1);
        }
        finally
        {
            foreach (var provider in providers)
                if (provider is not null) await provider.DisposeAsync();
        }
        await Verify(stores[0], 2, 2, 2);
    }

    [Test]
    public async Task Missing_or_unknown_authorized_tenant_never_selects_a_default_store()
    {
        await using var stores = new TenantStores();
        await stores.Start(false);
        await using var provider = Services(stores);
        foreach (var tenant in new string?[] { null, "unknown" })
        {
            await using var scope = provider.CreateAsyncScope();
            scope.ServiceProvider.GetRequiredService<AuthorizedTenant>().Id = tenant;
            await Assert.That(() => scope.ServiceProvider.GetRequiredService<OrdersContext>())
                .Throws<InvalidOperationException>();
        }
        for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 0, 0, 0);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Incoming_tenants_select_authorized_stores_without_a_startup_fallback(bool separateDatabases, bool nativeRetries)
    {
        await using var stores = new TenantStores();
        await stores.Start(separateDatabases);
        await using var provider = Services(stores, incomingTenants: true, nativeRetries: nativeRetries);
        // Composition cannot resolve this application's DbContext with an unset tenant.
        var pipeline = Pipeline(provider);
        var control = provider.GetRequiredService<Control>();
        await Assert.That(control.Authorizations).IsEqualTo(0);
        var id = Guid.CreateVersion7().ToString("D");
        var remaining = stores.Count;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.AfterSave = async context =>
        {
            var selected = context.Services.GetRequiredService<AuthorizedTenant>().Id;
            await Assert.That(context.Services.GetRequiredService<TenantContext>().Id.ToString()).IsEqualTo(selected);
            if (Interlocked.Decrement(ref remaining) == 0) release.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(30));
        };
        await Task.WhenAll(Enumerable.Range(0, stores.Count).Select(i => Deliver(pipeline, id, stores[i].Id)));
        control.AfterSave = null;
        for (var i = 0; i < stores.Count; i++)
        {
            await Deliver(pipeline, id, stores[i].Id);
            await Verify(stores[i], 1, 1, 1);
        }
        await Assert.That(control.Calls).IsEqualTo(3);
        var rollback = Guid.CreateVersion7().ToString("D");
        control.Fail = true;
        await Assert.That(async () => await Deliver(pipeline, rollback, stores[0].Id)).Throws<InvalidOperationException>();
        for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 1, 1, 1);
        control.Fail = false;
        await Deliver(pipeline, rollback, stores[0].Id);
        await Verify(stores[0], 2, 2, 2);
        for (var i = 1; i < stores.Count; i++) await Verify(stores[i], 1, 1, 1);
    }

    [Test]
    public async Task Incoming_tenant_is_authorized_again_before_each_native_retry_resolves_handler_dependencies()
    {
        await using var stores = new TenantStores();
        await stores.Start(false);
        await using var provider = Services(stores, incomingTenants: true, nativeRetries: true);
        var pipeline = Pipeline(provider);
        var control = provider.GetRequiredService<Control>();
        var scopes = new HashSet<IServiceProvider>();
        control.AfterSave = async context =>
        {
            scopes.Add(context.Services);
            await Assert.That(context.Services.GetRequiredService<AuthorizedTenant>().Id).IsEqualTo(stores[1].Id);
            if (scopes.Count == 1)
                await context.Services.GetRequiredService<OrdersContext>().Database.ExecuteSqlRawAsync(
                    "DO $conflict$ BEGIN RAISE EXCEPTION 'retry tenant' USING ERRCODE = '40001'; END $conflict$");
        };
        await Deliver(pipeline, Guid.CreateVersion7().ToString("D"), stores[1].Id);
        await Assert.That(scopes.Count).IsEqualTo(2);
        await Assert.That(control.Calls).IsEqualTo(2);
        // Strategy context, two transaction/handler scopes, and native unknown-outcome verification.
        await Assert.That(control.Authorizations).IsEqualTo(4);
        await Verify(stores[0], 0, 0, 0);
        await Verify(stores[1], 1, 1, 1);
        await Verify(stores[2], 0, 0, 0);
    }

    [Test]
    public async Task Missing_or_unauthorized_incoming_tenants_fail_before_handlers_or_durable_work()
    {
        await using var stores = new TenantStores();
        await stores.Start(false);
        await using var provider = Services(stores, incomingTenants: true);
        var pipeline = Pipeline(provider);
        foreach (var tenant in new string?[] { null, "unknown", "forbidden" })
            await Assert.That(async () => await Deliver(pipeline, Guid.CreateVersion7().ToString("D"), tenant))
                .Throws<InvalidOperationException>();
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(0);
        for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 0, 0, 0);
    }

    [Test]
    public async Task Selected_tenant_context_configuration_is_validated_before_opening_a_transaction()
    {
        await using var stores = new TenantStores();
        await stores.Start(false);
        await using var provider = Services(stores, incomingTenants: true);
        var pipeline = Pipeline(provider, explicitOptions: new());
        await Assert.That(async () => await Deliver(pipeline, Guid.CreateVersion7().ToString("D"), stores[0].Id))
            .Throws<InvalidOperationException>().WithMessageContaining("Configure schema and prefix on AddRelayerOutbox");
        await Assert.That(provider.GetRequiredService<Control>().Calls).IsEqualTo(0);
        for (var i = 0; i < stores.Count; i++) await Verify(stores[i], 0, 0, 0);
    }

    private static ServiceProvider Services(TenantStores stores, string? fixedTenant = null,
        bool incomingTenants = false, bool nativeRetries = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(stores);
        if (incomingTenants) services.AddRelayerTenancy();
        services.AddScoped(provider =>
        {
            var tenant = incomingTenants ? provider.GetRequiredService<TenantContext>().Id.ToString() : fixedTenant;
            // Authorization belongs to the application and precedes native resource selection.
            if (incomingTenants)
            {
                if (string.IsNullOrEmpty(tenant) || tenant is "unknown" or "forbidden")
                    throw new InvalidOperationException("The incoming tenant is not authorized.");
                Interlocked.Increment(ref provider.GetRequiredService<Control>().Authorizations);
            }
            return new AuthorizedTenant { Id = tenant };
        });
        services.AddScoped<TenantStoreResolver>();
        services.AddSingleton<Control>();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddSingleton<ITransport, Transport>();
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddDbContext<OrdersContext>((provider, options) =>
        {
            var store = provider.GetRequiredService<TenantStoreResolver>().Resolve();
            options.UseNpgsql(store.Source, native =>
            {
                if (nativeRetries) native.EnableRetryOnFailure(3, TimeSpan.Zero, null);
            }).AddRelayerOutbox(provider, new() { Schema = store.Schema });
        });
        services.AddRelayer(relayer =>
        {
            relayer.MessageTypes.Add<Probe>("tenant.store.probe");
            relayer.Handler<Handler>();
            relayer.AddTransport<Empty, Empty, Empty>("events", transport => transport.Send<Probe>("orders"));
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ConsumePipeline Pipeline(ServiceProvider provider, EntityFrameworkCoreOutboxOptions? explicitOptions = null) =>
        new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(),
                static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .EntityFrameworkCore<OrdersContext>("orders", options: explicitOptions)
            .Build();

    private static async Task Deliver(ConsumePipeline pipeline, string id, string? tenant = null)
    {
        using var context = pipeline.RentContext(new ConsumeMessage
        {
            MessageType = new("tenant.store.probe", 1), MessageId = id,
            Body = new ReadOnlySequence<byte>(new byte[] { 1 }),
            Headers = tenant is null ? null : new TenantHeaders(tenant)
        }, default);
        await pipeline.ProcessAsync(context);
    }

    private static async Task Verify(TenantStore store, long business, long outbox, long inbox)
    {
        await using var connection = await store.Source.OpenConnectionAsync();
        await Assert.That(await Count("orders")).IsEqualTo(business);
        await Assert.That(await Count("relayer_outbox")).IsEqualTo(outbox);
        await Assert.That(await Count("relayer_inbox")).IsEqualTo(inbox);
        async Task<long> Count(string table)
        {
            await using var command = new NpgsqlCommand($"SELECT count(*) FROM {table}", connection);
            return (long)(await command.ExecuteScalarAsync())!;
        }
    }

    // These are application-owned selection types, not a second Relayer store abstraction.
    public sealed class AuthorizedTenant { public string? Id { get; set; } }
    private sealed class TenantStoreResolver(AuthorizedTenant tenant, TenantStores stores)
    {
        internal TenantStore Resolve()
        {
            for (var i = 0; i < stores.Count; i++)
                if (stores[i].Id == tenant.Id) return stores[i];
            throw new InvalidOperationException("An authorized tenant store must be selected before resolving the DbContext.");
        }
    }
    private sealed record TenantStore(string Id, NpgsqlDataSource Source, string Schema);
    public sealed class OrdersContext(DbContextOptions<OrdersContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder builder) =>
            builder.Entity<Order>().ToTable("orders").HasKey(order => order.Id);
    }
    public sealed class Order { public int Id { get; set; } }
    public sealed class Control
    {
        public int Calls;
        public int Authorizations;
        public bool Fail;
        public Func<MessageContext, Task>? AfterSave;
    }
    public sealed class Handler(OrdersContext db, ISender sender, Control control) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref control.Calls);
            db.Add(new Order { Id = call });
            await sender.SendAsync(message, cancellationToken);
            await db.SaveChangesAsync(cancellationToken);
            if (control.AfterSave is { } afterSave) await afterSave(context);
            if (control.Fail) throw new InvalidOperationException("Roll back this tenant's complete unit.");
        }
    }
    [Message("tenant.store.probe")]
    public sealed record Probe(int Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)new Probe(1);
    }
    private sealed class TenantHeaders(string tenant) : MessageHeaders
    {
        public override bool TryGetValue(string key, out WireString value)
        {
            value = key == TenantContext.HeaderName ? new(tenant) : default;
            return key == TenantContext.HeaderName;
        }
    }
    private sealed class Transport : ITransport
    {
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
            ValueTask.FromException(new InvalidOperationException("Tenant sends must be persisted in their outbox."));
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private sealed class TenantStores : IAsyncDisposable
    {
        private readonly PostgreSqlFixture _fixture = new();
        private readonly List<TenantStore> _stores = [];
        internal int Count => _stores.Count;
        internal TenantStore this[int index] => _stores[index];
        internal async Task Start(bool separateDatabases)
        {
            await _fixture.StartAsync();
            await using var admin = new NpgsqlConnection(_fixture.ConnectionString);
            await admin.OpenAsync();
            for (var i = 0; i < 3; i++)
            {
                var id = "tenant_" + i;
                var schema = separateDatabases ? "public" : id;
                var settings = new NpgsqlConnectionStringBuilder(_fixture.ConnectionString) { SearchPath = schema };
                if (separateDatabases)
                {
                    await using var create = new NpgsqlCommand($"CREATE DATABASE {id}", admin);
                    await create.ExecuteNonQueryAsync();
                    settings.Database = id;
                }
                var store = new TenantStore(id, NpgsqlDataSource.Create(settings.ConnectionString), schema);
                _stores.Add(store);
                await using var connection = await store.Source.OpenConnectionAsync();
                await using var command = new NpgsqlCommand(PostgreSqlOutboxSchema.CreateScript(new() { Schema = schema }) +
                    $"CREATE TABLE {schema}.orders (\"Id\" integer PRIMARY KEY)", connection);
                await command.ExecuteNonQueryAsync();
            }
        }
        public async ValueTask DisposeAsync()
        {
            try
            {
                foreach (var store in _stores) await store.Source.DisposeAsync();
            }
            finally { await _fixture.DisposeAsync(); }
        }
    }
}
