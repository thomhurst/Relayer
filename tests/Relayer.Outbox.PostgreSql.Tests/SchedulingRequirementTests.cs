using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class SchedulingRequirementTests
{
    [Test]
    public async Task Ef_default_capabilities_validate_once_per_options_version_and_reload_for_new_scopes()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        { ["TablePrefix"] = "relayer_", ["Lanes"] = "32" }).Build();
        var validations = new CountingValidator();
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox(configuration);
        services.AddSingleton<IValidateOptions<EntityFrameworkCoreOutboxOptions>>(validations);
        services.AddSingleton(new PostgreSqlSchedulingRegistration(null, "relayer_"));
        services.AddSingleton(new PostgreSqlSchedulingRegistration(null, "relayer_", 32));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var first = provider.CreateScope();
        var original = first.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(original.SupportsScheduling).IsTrue();
        await Assert.That(original.SupportsOrderedScheduling).IsTrue();
        using (var sameVersion = provider.CreateScope())
        {
            var next = sameVersion.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            await Assert.That(next.SupportsScheduling).IsTrue();
            await Assert.That(next.SupportsOrderedScheduling).IsTrue();
        }
        await Assert.That(validations.Count).IsEqualTo(1);

        configuration["Lanes"] = "16";
        configuration.Reload();
        using (var changedLanes = provider.CreateScope())
        {
            var next = changedLanes.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            await Assert.That(next.SupportsScheduling).IsTrue();
            await Assert.That(next.SupportsOrderedScheduling).IsFalse();
        }
        await Assert.That(validations.Count).IsEqualTo(2);
        configuration["TablePrefix"] = "other_";
        configuration.Reload();
        using (var changedStore = provider.CreateScope())
        {
            var next = changedStore.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            await Assert.That(next.SupportsScheduling).IsFalse();
            await Assert.That(next.SupportsOrderedScheduling).IsFalse();
        }
        await Assert.That(validations.Count).IsEqualTo(3);
        await Assert.That(original.SupportsScheduling).IsTrue();
        await Assert.That(original.SupportsOrderedScheduling).IsTrue();
    }

    [Test]
    public async Task Ef_selected_context_overrides_cached_default_capabilities()
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        services.AddSingleton(new PostgreSqlSchedulingRegistration(null, "relayer_"));
        services.AddSingleton(new PostgreSqlSchedulingRegistration(null, "relayer_", 32));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(executor.SupportsScheduling).IsTrue();
        await Assert.That(executor.SupportsOrderedScheduling).IsTrue();
        var options = new DbContextOptionsBuilder().UseNpgsql("Host=unused")
            .AddRelayerOutbox(scope.ServiceProvider, new() { TablePrefix = "other_" });
        using var context = new DbContext(options.Options);
        using (context.EnlistRelayerOutbox())
        {
            await Assert.That(executor.IsActive).IsTrue();
            await Assert.That(executor.SupportsScheduling).IsFalse();
            await Assert.That(executor.SupportsOrderedScheduling).IsFalse();
            var record = new OutboxRecord(Guid.NewGuid(), "events", "schedule.probe;v=1",
                new("orders", default, deliverAt: DateTimeOffset.UtcNow.AddDays(1)), DateTimeOffset.UtcNow);
            await Assert.That(async () => await executor.EnlistAsync(record, default))
                .Throws<InvalidOperationException>().WithMessageContaining("selected DbContext");
        }
        await Assert.That(executor.IsActive).IsFalse();
        await Assert.That(executor.SupportsScheduling).IsTrue();
        await Assert.That(executor.SupportsOrderedScheduling).IsTrue();
    }

    private sealed class CountingValidator : IValidateOptions<EntityFrameworkCoreOutboxOptions>
    {
        internal int Count { get; private set; }
        public ValidateOptionsResult Validate(string? name, EntityFrameworkCoreOutboxOptions options)
        {
            Count++;
            return ValidateOptionsResult.Success;
        }
    }

    [Test]
    [Arguments(32, false)]
    [Arguments(16, true)]
    public async Task Ordered_schedule_requires_the_native_ordered_worker_with_the_writer_lane_count(int workerLanes, bool mismatch)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlOutbox();
        services.AddRelayerPostgreSqlMaintenance("unordered", source);
        services.AddRelayerPostgreSqlOrderedDispatch("ordered", source,
            new PostgreSqlOrderedDispatchOptions { Lanes = workerLanes });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(executor.SupportsScheduling).IsTrue();
        await Assert.That(executor.SupportsOrderedScheduling).IsEqualTo(!mismatch);
    }

    [Test]
    [Arguments(false, false, false, false)]
    [Arguments(false, true, false, true)]
    [Arguments(false, true, true, false)]
    [Arguments(true, false, false, false)]
    [Arguments(true, true, false, true)]
    [Arguments(true, true, true, false)]
    public async Task Enlistment_requires_a_matching_due_row_worker(bool ef, bool worker, bool mismatch, bool valid)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        if (ef) services.AddRelayerEntityFrameworkCoreOutbox();
        else services.AddRelayerPostgreSqlOutbox();
        // Enlistment requires due-row recovery even when no direct dispatcher is installed.
        if (worker) services.AddRelayerPostgreSqlMaintenance("due", source,
            new PostgreSqlMaintenanceOptions { TablePrefix = mismatch ? "other_" : "relayer_" });
        services.AddSingleton<ITransport>(new Transport());
        services.AddSingleton<IMessageSerializer, Serializer>();
        Configure(services);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var start = async () =>
        {
            foreach (var lifecycle in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
                await lifecycle.StartingAsync(default);
        };
        if (valid) await start();
        else await Assert.That(start).Throws<OptionsValidationException>().WithMessageContaining("matching due-row dispatch worker");
    }

    [Test]
    [Arguments(-1)]
    [Arguments(1)]
    public async Task Declared_fallback_persists_future_schedule_then_actual_worker_dispatches_when_due(int creationOffsetDays)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var transport = new Transport();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlOutbox();
        services.AddRelayerPostgreSqlMaintenance("due", source, new PostgreSqlMaintenanceOptions { RecoveryDelay = TimeSpan.Zero });
        services.AddSingleton<ITransport>(transport);
        services.AddSingleton<IMessageSerializer, Serializer>();
        Configure(services);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        // Validate before opening any transaction or starting a dispatcher.
        foreach (var lifecycle in provider.GetServices<IHostedService>().OfType<IHostedLifecycleService>())
            await lifecycle.StartingAsync(default);
        await using (var scope = provider.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
            await using var transaction = await outbox.BeginTransactionAsync(source);
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(42),
                options => options.DeliverAt = DateTimeOffset.UtcNow.AddDays(1));
            await transaction.CommitAsync();
        }
        // Simulate an enqueue clock behind or ahead of PostgreSQL without changing either clock.
        await using (var skew = source.CreateCommand("UPDATE relayer_outbox SET created_at = clock_timestamp() + @offset"))
        {
            skew.Parameters.AddWithValue("offset", TimeSpan.FromDays(creationOffsetDays));
            await Assert.That(await skew.ExecuteNonQueryAsync()).IsEqualTo(1);
        }
        var worker = provider.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>("due");
        await Assert.That(await worker.SweepAsync(default)).IsEqualTo(0);
        await Assert.That(transport.Count).IsEqualTo(0);
        // RecoveryDelay also compares host-originated created_at with the database clock.
        // Advance both eligibility timestamps so clock skew cannot hide a due row.
        await using (var due = source.CreateCommand("UPDATE relayer_outbox SET created_at = clock_timestamp() - interval '1 day', deliver_at = clock_timestamp() - interval '1 day'"))
            await Assert.That(await due.ExecuteNonQueryAsync()).IsEqualTo(1);
        await Assert.That(await worker.SweepAsync(default)).IsEqualTo(1);
        await Assert.That(transport.Count).IsEqualTo(1);
        await Assert.That(transport.LastBody).IsEqualTo((byte)42);
        await Assert.That(transport.LastDeliverAt).IsNull();
        await worker.ReleaseAsync(default);
    }

    private static void Configure(IServiceCollection services) => services.AddRelayer(builder =>
    {
        builder.Infrastructure().MessageTypes.Add<Probe>("schedule.probe");
        builder.AddTransport<Empty, Empty>("events", transport => transport.Send<Probe>("orders", route =>
            route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromDays(1),
                TransactionRequirement = SendTransactionRequirement.Outbox }));
    });

    [Message("schedule.probe")]
    public sealed record Probe(byte Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            destination.GetSpan(1)[0] = ((Probe)(object)message!).Value;
            destination.Advance(1);
        }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }
    private sealed class Transport : ITransport
    {
        public string Name => "events";
        internal int Count { get; private set; }
        internal byte LastBody { get; private set; }
        internal DateTimeOffset? LastDeliverAt { get; private set; }
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Count++;
            LastBody = message.Body.Span[0];
            LastDeliverAt = message.DeliverAt;
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
