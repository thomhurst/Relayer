using Relayer.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Relayer.HealthChecks;

namespace Relayer.Tests;

public class OutboxStoreHealthTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 1, 12, 0, 0, TimeSpan.Zero);

    [Test]
    [Arguments(0L, HealthStatus.Healthy)]
    [Arguments(9L, HealthStatus.Healthy)]
    [Arguments(10L, HealthStatus.Degraded)]
    [Arguments(19L, HealthStatus.Degraded)]
    [Arguments(20L, HealthStatus.Unhealthy)]
    [Arguments(-1L, HealthStatus.Unhealthy)]
    public async Task Store_counts_feed_inclusive_health_thresholds(long count, HealthStatus expected)
    {
        var store = new TestStore { Count = count };
        var services = CreateServices("orders");
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => store);
        services.AddRelayerOutboxHealthProvider("orders");
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var entry = (await Check(provider)).Entries["relayer.outbox"];

        await Assert.That(entry.Status).IsEqualTo(expected);
        await Assert.That(store.ObservedNow).IsEqualTo(Now);
        if (count >= 0)
        {
            var detail = (HealthCheckResult)entry.Data["orders"];
            await Assert.That((long)detail.Data["pending_count"]).IsEqualTo(count);
        }
    }

    [Test]
    public async Task Adapter_reads_current_time_and_forwards_the_callers_token()
    {
        var store = new TestStore();
        var clock = new TestClock { Now = Now };
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => store);
        services.AddRelayerOutboxHealthProvider("orders");
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var adapter = scope.ServiceProvider.GetRequiredKeyedService<IOutboxHealthProvider>("orders");
        using var cancellation = new CancellationTokenSource();

        await adapter.GetPendingCountAsync(cancellation.Token);
        await Assert.That(store.ObservedNow).IsEqualTo(Now);
        await Assert.That(store.ObservedToken).IsEqualTo(cancellation.Token);
        clock.Now = Now.AddMinutes(1);
        await adapter.GetPendingCountAsync(cancellation.Token);
        await Assert.That(store.ObservedNow).IsEqualTo(clock.Now);
    }

    [Test]
    public async Task Store_names_and_scopes_stay_independent_and_stores_are_disposed()
    {
        var stores = new List<TestStore>();
        var services = CreateServices("orders", "billing");
        foreach (var name in new[] { "orders", "billing" })
        {
            services.AddKeyedScoped<IOutboxStore>(name, (_, _) =>
            {
                var store = new TestStore { Count = name == "orders" ? 10 : 20 };
                stores.Add(store);
                return store;
            });
            services.AddRelayerOutboxHealthProvider(name);
        }
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        for (var poll = 0; poll < 2; poll++)
        {
            var entry = (await Check(provider)).Entries["relayer.outbox"];
            await Assert.That(((HealthCheckResult)entry.Data["orders"]).Status).IsEqualTo(HealthStatus.Degraded);
            await Assert.That(((HealthCheckResult)entry.Data["billing"]).Status).IsEqualTo(HealthStatus.Unhealthy);
        }

        await Assert.That(stores.Count).IsEqualTo(4);
        foreach (var store in stores) await Assert.That(store.Disposed).IsTrue();
    }

    [Test]
    public async Task Missing_store_cannot_hide_behind_another_key()
    {
        var services = CreateServices("orders", "Orders");
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => new TestStore());
        services.AddRelayerOutboxHealthProvider("orders");
        services.AddRelayerOutboxHealthProvider("Orders");
        await using var provider = services.BuildServiceProvider();

        var entry = (await Check(provider)).Entries["relayer.outbox"];

        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(((HealthCheckResult)entry.Data["orders"]).Status).IsEqualTo(HealthStatus.Healthy);
        var missing = (HealthCheckResult)entry.Data["Orders"];
        await Assert.That(missing.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(missing.Exception).IsTypeOf<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Registered_adapter_missing_from_expected_names_fails_health(bool includeOtherStore)
    {
        var services = includeOtherStore ? CreateServices("billing") : CreateServices();
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => new TestStore());
        services.AddRelayerOutboxHealthProvider("orders");
        services.AddRelayerOutboxHealthProvider("orders");
        if (includeOtherStore)
        {
            services.AddKeyedScoped<IOutboxStore>("billing", (_, _) => new TestStore());
            services.AddRelayerOutboxHealthProvider("billing");
        }
        await using var provider = services.BuildServiceProvider();

        var entry = (await Check(provider)).Entries["relayer.outbox"];

        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(((HealthCheckResult)entry.Data["orders"]).Description!).Contains("OutboxNames");
        if (includeOtherStore)
            await Assert.That(((HealthCheckResult)entry.Data["billing"]).Status).IsEqualTo(HealthStatus.Healthy);
    }

    [Test]
    public async Task Unavailable_count_and_original_database_error_are_preserved()
    {
        var error = new IOException("database unavailable");
        var services = CreateServices("unavailable", "failed");
        services.AddKeyedScoped<IOutboxStore>("unavailable", (_, _) => new TestStore { Count = null });
        services.AddKeyedScoped<IOutboxStore>("failed", (_, _) => new TestStore { Error = error });
        services.AddRelayerOutboxHealthProvider("unavailable");
        services.AddRelayerOutboxHealthProvider("failed");
        await using var provider = services.BuildServiceProvider();

        var entry = (await Check(provider)).Entries["relayer.outbox"];

        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(((HealthCheckResult)entry.Data["unavailable"]).Description!).Contains("unavailable");
        await Assert.That(((HealthCheckResult)entry.Data["failed"]).Exception).IsSameReferenceAs(error);
        await Assert.That(entry.Exception).IsSameReferenceAs(error);
    }

    [Test]
    public async Task Cancellation_reaches_an_inflight_store_query()
    {
        var entered = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new TestStore { Query = WaitForCancellation };
        var services = new ServiceCollection();
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => store);
        services.AddRelayerOutboxHealthProvider("orders");
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var adapter = scope.ServiceProvider.GetRequiredKeyedService<IOutboxHealthProvider>("orders");
        using var cancellation = new CancellationTokenSource();

        var query = adapter.GetPendingCountAsync(cancellation.Token).AsTask();
        var observed = await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();

        await Assert.That(observed).IsEqualTo(cancellation.Token);
        await Assert.That(async () => await query).Throws<OperationCanceledException>();

        async ValueTask<long?> WaitForCancellation(CancellationToken token)
        {
            entered.SetResult(token);
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            return 0;
        }
    }

    [Test]
    public async Task Already_canceled_adapter_call_does_not_query_store()
    {
        var store = new TestStore();
        var services = new ServiceCollection();
        services.AddKeyedScoped<IOutboxStore>("orders", (_, _) => store);
        services.AddRelayerOutboxHealthProvider("orders");
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.That(async () => await scope.ServiceProvider.GetRequiredKeyedService<IOutboxHealthProvider>("orders")
            .GetPendingCountAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(store.Calls).IsEqualTo(0);
    }

    [Test]
    public async Task Repeated_registration_preserves_custom_adapter_and_registers_other_names()
    {
        var services = new ServiceCollection();
        var custom = new CustomProvider();
        services.AddKeyedSingleton<IOutboxHealthProvider>("orders", custom);
        services.AddRelayerOutboxHealthProvider("orders");
        services.AddRelayerOutboxHealthProvider("billing");
        services.AddRelayerOutboxHealthProvider("billing");
        services.AddKeyedScoped<IOutboxStore>("billing", (_, _) => new TestStore());
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        await Assert.That(scope.ServiceProvider.GetRequiredKeyedService<IOutboxHealthProvider>("orders"))
            .IsSameReferenceAs(custom);
        await Assert.That(scope.ServiceProvider.GetKeyedServices<IOutboxHealthProvider>("billing").Count()).IsEqualTo(1);
        await Assert.That(await scope.ServiceProvider.GetRequiredKeyedService<IOutboxHealthProvider>("billing")
            .GetPendingCountAsync(default)).IsEqualTo(0L);
    }

    [Test]
    public async Task Registration_rejects_missing_services_and_blank_store_names()
    {
        await Assert.That(() => OutboxHealthServiceCollectionExtensions.AddRelayerOutboxHealthProvider(null!, "orders"))
            .Throws<ArgumentNullException>();
        foreach (var name in new[] { null, "", " " })
            await Assert.That(() => new ServiceCollection().AddRelayerOutboxHealthProvider(name!)).Throws<ArgumentException>();
    }

    private static ServiceCollection CreateServices(params string[] names)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new TestClock { Now = Now });
        services.AddRelayer(_ => { });
        services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
        {
            OutboxNames = names, OutboxDegradedThreshold = 10, OutboxUnhealthyThreshold = 20
        });
        return services;
    }

    private static Task<Microsoft.Extensions.Diagnostics.HealthChecks.HealthReport> Check(IServiceProvider services) =>
        services.GetRequiredService<HealthCheckService>().CheckHealthAsync();

    private sealed class TestClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; }
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private sealed class CustomProvider : IOutboxHealthProvider
    {
        public ValueTask<long?> GetPendingCountAsync(CancellationToken cancellationToken) => ValueTask.FromResult<long?>(7);
    }

    private sealed class TestStore : IOutboxStore, IDisposable
    {
        public long? Count { get; init; } = 0;
        public Exception? Error { get; init; }
        public Func<CancellationToken, ValueTask<long?>>? Query { get; init; }
        public DateTimeOffset ObservedNow { get; private set; }
        public CancellationToken ObservedToken { get; private set; }
        public int Calls { get; private set; }
        public bool Disposed { get; private set; }

        public ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
        {
            Calls++;
            ObservedNow = now;
            ObservedToken = cancellationToken;
            if (Error is not null) return ValueTask.FromException<long?>(Error);
            return Query?.Invoke(cancellationToken) ?? ValueTask.FromResult(Count);
        }

        public void Dispose() => Disposed = true;
        public ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
            CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
            CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
            CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now,
            CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
            CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
