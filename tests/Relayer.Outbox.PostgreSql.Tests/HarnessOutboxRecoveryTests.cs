using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public sealed class HarnessOutboxRecoveryTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Recovered_rows_cannot_settle_a_local_commit(bool recoverFirst, bool retainSent)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var database = new PostgreSqlFixture();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source, new()
        {
            RetainSentMessages = retainSent
        });

        if (recoverFirst)
        {
            using var previous = CreateHost(observe: false);
            await CommitAsync(previous.Services, source, 1, timeout.Token);
        }

        using var host = CreateHost(observe: true);
        await host.StartAsync(timeout.Token);
        var provider = host.Services;
        var harness = provider.GetRequiredService<RelayerHarness>();
        using var worker = new PostgreSqlMaintenanceWorker(source, new()
        {
            RecoveryDelay = TimeSpan.Zero,
            RetainSentMessages = retainSent
        }, provider.GetServices<ITransport>(), NullLogger<PostgreSqlMaintenanceWorker>.Instance,
            instrumentation: provider.GetRequiredService<RelayerInstrumentation>(), storeName: "store");
        var store = new PostgreSqlOutboxStore(source);

        if (recoverFirst)
        {
            await Assert.That(await worker.SweepAsync(timeout.Token)).IsEqualTo(1);
            await harness.WaitForConsumedAsync<Probe>(static probe => probe.Value == 1, timeout.Token);
            await harness.WaitForIdleAsync(timeout.Token);
            // An empty pass wraps the worker's fixed recovery boundary before the next commit.
            await Assert.That(await worker.SweepAsync(timeout.Token)).IsEqualTo(0);
            await Assert.That(harness.Dispatched.Count).IsEqualTo(1);
            await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, timeout.Token)).IsEqualTo(0L);
        }

        await CommitAsync(provider, source, 2, timeout.Token);
        await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, timeout.Token)).IsEqualTo(1L);
        var idle = harness.WaitForIdleAsync(timeout.Token);
        await Assert.That(idle.IsCompleted).IsFalse();
        await Assert.That(() => harness.Reset()).Throws<InvalidOperationException>();

        await Assert.That(await worker.SweepAsync(timeout.Token)).IsEqualTo(1);
        await harness.WaitForConsumedAsync<Probe>(static probe => probe.Value == 2, timeout.Token);
        await idle;
        await Assert.That(harness.Dispatched.Count).IsEqualTo(recoverFirst ? 2 : 1);
        await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, timeout.Token)).IsEqualTo(0L);
        harness.Reset();
        await host.StopAsync(timeout.Token);
    }

    private static IHost CreateHost(bool observe)
    {
        var host = Host.CreateApplicationBuilder();
        var services = host.Services;
        services.AddRelayerPostgreSqlOutbox();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Probe>("harness.recovery");
            builder.UseJsonSerializer(HarnessRecoveryJsonContext.Default);
            builder.InMemory("local", transport =>
            {
                transport.Send<Probe>("orders");
                transport.Consume("orders", consumer => consumer.Handler<ProbeHandler>());
            });
        });

        if (observe)
        {
            services.AddRelayerHarness();
        }

        return host.Build();
    }

    private static async Task CommitAsync(IServiceProvider provider, NpgsqlDataSource source, int value, CancellationToken cancellationToken)
    {
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var transaction = await outbox.BeginTransactionAsync(source, cancellationToken: cancellationToken);
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(value), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public sealed record Probe(int Value);

    public sealed class ProbeHandler : IHandler<Probe>
    {
        public ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(HarnessOutboxRecoveryTests.Probe))]
internal sealed partial class HarnessRecoveryJsonContext : JsonSerializerContext;
