using Relayer.Transports;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.HealthChecks;

namespace Relayer.Tests;

public class HealthCheckTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 1, 12, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task Registration_is_idempotent_and_works_before_AddRelayer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerHealthChecks();
        services.AddRelayerHealthChecks();
        services.AddRelayer(_ => { });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var report = await Check(provider);
        await Assert.That(report.Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That(report.Entries.Count).IsEqualTo(3);
        await Assert.That(report.Entries.Keys.Order().ToArray())
            .IsEquivalentTo(new[] { "relayer.consumer", "relayer.outbox", "relayer.transport" });
    }

    [Test]
    public async Task Missing_providers_fail_all_configured_resources()
    {
        await using var provider = Create(new RelayerHealthCheckOptions { OutboxNames = ["orders"] });
        var report = await Check(provider);
        foreach (var entry in report.Entries.Values)
        {
            await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
            await Assert.That(entry.Description!).Contains("No keyed I");
        }
        await Assert.That(report.Entries["relayer.transport"].Description!).Contains("events");
        await Assert.That(report.Entries["relayer.consumer"].Description!).Contains("orders");
    }

    [Test]
    [Arguments(HealthStatus.Healthy)]
    [Arguments(HealthStatus.Degraded)]
    [Arguments(HealthStatus.Unhealthy)]
    public async Task Native_connectivity_status_and_details_are_preserved(HealthStatus status)
    {
        var exception = status == HealthStatus.Unhealthy ? new IOException("native failure") : null;
        var native = new HealthCheckResult(status, "native probe", exception,
            new Dictionary<string, object> { ["native"] = 7 });
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<ITransportHealthProbe>("events", new TransportProbe(_ => ValueTask.FromResult(native))));
        var entry = (await Check(provider)).Entries["relayer.transport"];
        await Assert.That(entry.Status).IsEqualTo(status);
        await Assert.That(entry.Description!).Contains("events: native probe");
        await Assert.That(entry.Exception).IsEqualTo(exception);
        var detail = (HealthCheckResult)entry.Data["events"];
        await Assert.That((int)detail.Data["native"]).IsEqualTo(7);
    }

    [Test]
    public async Task Worst_transport_status_wins_without_hiding_other_failures()
    {
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<ITransportHealthProbe>("events",
                new TransportProbe(_ => ValueTask.FromResult(HealthCheckResult.Degraded("warming up")))),
            extra: builder => builder.AddTransport<EmptyOptions, EmptyOptions, EmptyOptions>("missing", _ => { }));
        var entry = (await Check(provider)).Entries["relayer.transport"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Description!).Contains("warming up");
        await Assert.That(entry.Description!).Contains("missing");
        await Assert.That(entry.Data.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(true, 3600, HealthStatus.Healthy)]
    [Arguments(false, 299, HealthStatus.Healthy)]
    [Arguments(false, 300, HealthStatus.Unhealthy)]
    [Arguments(false, 301, HealthStatus.Unhealthy)]
    [Arguments(false, -1, HealthStatus.Unhealthy)]
    public async Task Consumer_idle_progress_and_stall_threshold(bool idle, int ageSeconds, HealthStatus expected)
    {
        IConsumerDefinition? observed = null;
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<IConsumerHealthProvider>("events", new ConsumerProvider((consumer, _) =>
            {
                observed = consumer;
                return ValueTask.FromResult<ConsumerHealthState?>(new(idle, Now.AddSeconds(-ageSeconds)));
            })));
        var entry = (await Check(provider)).Entries["relayer.consumer"];
        await Assert.That(entry.Status).IsEqualTo(expected);
        await Assert.That(observed).IsSameReferenceAs(provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
    }

    [Test]
    public async Task Consumer_snapshots_distinguish_declarations_on_same_source()
    {
        var calls = new List<IConsumerDefinition>();
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<IConsumerHealthProvider>("events", new ConsumerProvider((consumer, _) =>
            {
                calls.Add(consumer);
                return ValueTask.FromResult<ConsumerHealthState?>(new(false, Now));
            })), secondConsumer: true);
        var entry = (await Check(provider)).Entries["relayer.consumer"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That(calls.Count).IsEqualTo(2);
        await Assert.That(ReferenceEquals(calls[0], calls[1])).IsFalse();
        await Assert.That(entry.Data.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Unavailable_consumer_state_is_unhealthy()
    {
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<IConsumerHealthProvider>("events", new ConsumerProvider((_, _) =>
                ValueTask.FromResult<ConsumerHealthState?>(null))));
        var entry = (await Check(provider)).Entries["relayer.consumer"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Description!).Contains("unavailable");
    }

    [Test]
    [Arguments(-1L, HealthStatus.Unhealthy)]
    [Arguments(0L, HealthStatus.Healthy)]
    [Arguments(9L, HealthStatus.Healthy)]
    [Arguments(10L, HealthStatus.Degraded)]
    [Arguments(19L, HealthStatus.Degraded)]
    [Arguments(20L, HealthStatus.Unhealthy)]
    public async Task Outbox_thresholds_are_inclusive(long count, HealthStatus expected)
    {
        await using var provider = Create(BacklogOptions(), services =>
            services.AddKeyedSingleton<IOutboxHealthProvider>("store", new OutboxProvider(_ => ValueTask.FromResult<long?>(count))));
        var entry = (await Check(provider)).Entries["relayer.outbox"];
        await Assert.That(entry.Status).IsEqualTo(expected);
        if (count >= 0)
            await Assert.That((long)((HealthCheckResult)entry.Data["store"]).Data["pending_count"]).IsEqualTo(count);
    }

    [Test]
    public async Task Unavailable_backlog_is_unhealthy()
    {
        await using var provider = Create(BacklogOptions(), services =>
            services.AddKeyedSingleton<IOutboxHealthProvider>("store", new OutboxProvider(_ => ValueTask.FromResult<long?>(null))));
        var entry = (await Check(provider)).Entries["relayer.outbox"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Description!).Contains("unavailable");
    }

    [Test]
    public async Task Registered_outbox_cannot_silently_pass_without_declared_stores()
    {
        await using var provider = Create(configure: services => services.AddScoped<IOutboxEnlistment, UnusedOutbox>());
        var entry = (await Check(provider)).Entries["relayer.outbox"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Description!).Contains("OutboxNames");
    }

    [Test]
    [Arguments("transport")]
    [Arguments("consumer")]
    [Arguments("outbox")]
    public async Task Probe_exceptions_and_unrelated_cancellation_are_failures(string kind)
    {
        foreach (var error in new Exception[] { new IOException("probe exploded"), new OperationCanceledException("native timeout") })
        {
            await using var provider = Create(BacklogOptions(), services => RegisterFailure(services, kind, error));
            var entry = (await Check(provider)).Entries[$"relayer.{kind}"];
            await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
            await Assert.That(entry.Exception).IsSameReferenceAs(error);
            await Assert.That(entry.Description!).Contains(error.Message);
        }
    }

    [Test]
    public async Task Adapter_construction_failure_is_reported_with_resource_name()
    {
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<ITransportHealthProbe>("events", (_, _) => throw new IOException("client unavailable")));
        var entry = (await Check(provider)).Entries["relayer.transport"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Description!).Contains("events: Probe failed: client unavailable");
    }

    [Test]
    [Arguments("transport")]
    [Arguments("consumer")]
    [Arguments("outbox")]
    public async Task Cancellation_reaches_each_adapter_and_propagates(string kind)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        async ValueTask<T> Wait<T>(CancellationToken token)
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            throw new InvalidOperationException("Unreachable");
        }
        await using var provider = Create(BacklogOptions(), services =>
        {
            services.AddKeyedSingleton<ITransportHealthProbe>("events", new TransportProbe(Wait<HealthCheckResult>));
            services.AddKeyedSingleton<IConsumerHealthProvider>("events", new ConsumerProvider((_, token) => Wait<ConsumerHealthState?>(token)));
            services.AddKeyedSingleton<IOutboxHealthProvider>("store", new OutboxProvider(Wait<long?>));
        });
        var registration = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations
            .Single(item => item.Name == $"relayer.{kind}");
        var check = registration.Factory(provider);
        var task = check.CheckHealthAsync(new HealthCheckContext { Registration = registration }, cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        await Assert.That(async () => await task).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Already_canceled_check_does_not_call_adapter()
    {
        var calls = 0;
        await using var provider = Create(configure: services =>
            services.AddKeyedSingleton<ITransportHealthProbe>("events", new TransportProbe(_ =>
            {
                calls++;
                return ValueTask.FromResult(HealthCheckResult.Healthy());
            })));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await Check(provider, cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(calls).IsEqualTo(0);
    }

    [Test]
    [Arguments(0, 10L, 20L)]
    [Arguments(-1, 10L, 20L)]
    [Arguments(1, 0L, 20L)]
    [Arguments(1, 20L, 20L)]
    [Arguments(1, 20L, 10L)]
    public async Task Invalid_thresholds_fail_options_validation(int timeoutSeconds, long degraded, long unhealthy)
    {
        await using var provider = Create(new RelayerHealthCheckOptions
        {
            ConsumerStallTimeout = TimeSpan.FromSeconds(timeoutSeconds),
            OutboxDegradedThreshold = degraded,
            OutboxUnhealthyThreshold = unhealthy
        });
        await Assert.That(() => provider.GetRequiredService<IOptions<RelayerHealthCheckOptions>>().Value)
            .Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Invalid_outbox_names_fail_options_validation()
    {
        foreach (var names in new IReadOnlyList<string>[] { ["store", "store"], [" "], null! })
        {
            await using var provider = Create(new RelayerHealthCheckOptions { OutboxNames = names });
            await Assert.That(() => provider.GetRequiredService<IOptions<RelayerHealthCheckOptions>>().Value)
                .Throws<OptionsValidationException>();
        }
    }

    [Test]
    public async Task Invalid_options_fail_at_host_startup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayerHealthChecks(new RelayerHealthCheckOptions { ConsumerStallTimeout = TimeSpan.Zero });
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Options_bind_configuration_and_validate_bound_values()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConsumerStallTimeout"] = "00:00:12",
            ["OutboxDegradedThreshold"] = "3",
            ["OutboxUnhealthyThreshold"] = "8",
            ["OutboxNames:0"] = "store"
        }).Build();
        var services = new ServiceCollection();
        services.AddRelayerHealthChecks(configuration);
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<RelayerHealthCheckOptions>>().Value;
        await Assert.That(options.ConsumerStallTimeout).IsEqualTo(TimeSpan.FromSeconds(12));
        await Assert.That(options.OutboxDegradedThreshold).IsEqualTo(3L);
        await Assert.That(options.OutboxUnhealthyThreshold).IsEqualTo(8L);
        await Assert.That(options.OutboxNames.Single()).IsEqualTo("store");
    }

    [Test]
    public async Task Healthy_adapters_cover_every_configured_resource()
    {
        await using var provider = Create(BacklogOptions(), services =>
        {
            services.AddKeyedSingleton<ITransportHealthProbe>("events",
                new TransportProbe(_ => ValueTask.FromResult(HealthCheckResult.Healthy("connected"))));
            services.AddKeyedSingleton<IConsumerHealthProvider>("events",
                new ConsumerProvider((_, _) => ValueTask.FromResult<ConsumerHealthState?>(new(false, Now))));
            services.AddKeyedSingleton<IOutboxHealthProvider>("store", new OutboxProvider(_ => ValueTask.FromResult<long?>(0)));
        });
        await Assert.That((await Check(provider)).Status).IsEqualTo(HealthStatus.Healthy);
    }

    [Test]
    public async Task Missing_outbox_does_not_hide_behind_a_healthy_store()
    {
        await using var provider = Create(new RelayerHealthCheckOptions { OutboxNames = ["available", "missing"] }, services =>
            services.AddKeyedSingleton<IOutboxHealthProvider>("available", new OutboxProvider(_ => ValueTask.FromResult<long?>(0))));
        var entry = (await Check(provider)).Entries["relayer.outbox"];
        await Assert.That(entry.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(entry.Data.Count).IsEqualTo(2);
        await Assert.That(entry.Description!).Contains("missing");
    }

    [Test]
    public async Task Bound_options_reload_and_health_uses_current_thresholds()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OutboxDegradedThreshold"] = "10", ["OutboxUnhealthyThreshold"] = "20", ["OutboxNames:0"] = "store"
        }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayer(_ => { });
        services.AddRelayerHealthChecks(configuration);
        services.AddKeyedSingleton<IOutboxHealthProvider>("store", new OutboxProvider(_ => ValueTask.FromResult<long?>(5)));
        await using var provider = services.BuildServiceProvider();
        await Assert.That((await Check(provider)).Status).IsEqualTo(HealthStatus.Healthy);
        configuration["OutboxDegradedThreshold"] = "5";
        configuration.Reload();
        await Assert.That((await Check(provider)).Status).IsEqualTo(HealthStatus.Degraded);
    }

    [Test]
    [Arguments("ConsumerStallTimeout", "bad")]
    [Arguments("OutboxDegradedThreshold", "bad")]
    [Arguments("OutboxDegradedThreshold", "0")]
    [Arguments("OutboxUnhealthyThreshold", "1")]
    [Arguments("OutboxNames:0", " ")]
    [Arguments("OutboxNames", "store")]
    public async Task Invalid_bound_values_fail_validation(string key, string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [key] = value }).Build();
        var services = new ServiceCollection();
        services.AddRelayerHealthChecks(configuration);
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetRequiredService<IOptions<RelayerHealthCheckOptions>>().Value)
            .Throws<OptionsValidationException>();
    }

    private static RelayerHealthCheckOptions BacklogOptions() => new()
    {
        OutboxNames = ["store"], OutboxDegradedThreshold = 10, OutboxUnhealthyThreshold = 20
    };

    private static ServiceProvider Create(RelayerHealthCheckOptions? options = null,
        Action<IServiceCollection>? configure = null, Action<IRelayerBuilder>? extra = null, bool secondConsumer = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new HealthClock());
        services.AddRelayer(builder =>
        {
            builder.AddTransport<EmptyOptions, EmptyOptions, EmptyOptions>("events", transport =>
            {
                // Health reads declarations without starting the transport or invoking handlers.
                transport.Consume("orders", consumer => consumer.Handler<HealthHandler>());
                if (secondConsumer) transport.Consume("orders", consumer => consumer.Handler<HealthHandler>());
            });
            extra?.Invoke(builder);
        });
        services.AddRelayerHealthChecks(options);
        configure?.Invoke(services);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static Task<Microsoft.Extensions.Diagnostics.HealthChecks.HealthReport> Check(IServiceProvider provider,
        CancellationToken token = default) => provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(token);

    private static void RegisterFailure(IServiceCollection services, string kind, Exception error)
    {
        if (kind == "transport") services.AddKeyedSingleton<ITransportHealthProbe>("events",
            new TransportProbe(_ => ValueTask.FromException<HealthCheckResult>(error)));
        if (kind == "consumer") services.AddKeyedSingleton<IConsumerHealthProvider>("events",
            new ConsumerProvider((_, _) => ValueTask.FromException<ConsumerHealthState?>(error)));
        if (kind == "outbox") services.AddKeyedSingleton<IOutboxHealthProvider>("store",
            new OutboxProvider(_ => ValueTask.FromException<long?>(error)));
    }

    private sealed class HealthClock : TimeProvider { public override DateTimeOffset GetUtcNow() => Now; }
    public sealed class EmptyOptions;
    public sealed class HealthHandler : IHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
    private sealed class TransportProbe(Func<CancellationToken, ValueTask<HealthCheckResult>> check) : ITransportHealthProbe
    {
        public ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken) => check(cancellationToken);
    }
    private sealed class ConsumerProvider(Func<IConsumerDefinition, CancellationToken, ValueTask<ConsumerHealthState?>> read) : IConsumerHealthProvider
    {
        public ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken) => read(consumer, cancellationToken);
    }
    private sealed class OutboxProvider(Func<CancellationToken, ValueTask<long?>> read) : IOutboxHealthProvider
    {
        public ValueTask<long?> GetPendingCountAsync(CancellationToken cancellationToken) => read(cancellationToken);
    }
    private sealed class UnusedOutbox : IOutboxEnlistment
    {
        public bool IsActive => throw new InvalidOperationException("Health must not resolve an enlistment.");
        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken) => throw new NotImplementedException();
    }
}
