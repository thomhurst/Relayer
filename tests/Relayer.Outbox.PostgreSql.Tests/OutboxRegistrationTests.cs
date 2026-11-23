using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OutboxRegistrationTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Keyed_enlistment_coexists_with_the_default_outbox(bool useEntityFrameworkCore)
    {
        var services = new ServiceCollection();
        services.AddKeyedScoped<IOutboxEnlistment, IndependentEnlistment>("independent");

        if (useEntityFrameworkCore)
        {
            services.AddRelayerEntityFrameworkCoreOutbox();
        }
        else
        {
            services.AddRelayerPostgreSqlOutbox();
        }

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxEnlistment>("independent");

        await Assert.That(keyed).IsTypeOf<IndependentEnlistment>();
        await Assert.That(ReferenceEquals(enlistment, keyed)).IsFalse();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task EntityFrameworkCore_outbox_validation_composes_with_application_validation(bool applicationFirst, bool invalidLanes)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = invalidLanes ? "0" : "16",
            ["TablePrefix"] = invalidLanes ? "orders_" : "other_"
        }).Build();
        var services = new ServiceCollection();

        void AddApplicationValidator()
        {
            services.AddOptions<EntityFrameworkCoreOutboxOptions>().Validate(
                options => options.TablePrefix == "orders_", "Use the orders_ prefix.");
        }

        if (applicationFirst)
        {
            AddApplicationValidator();
        }

        services.AddRelayerEntityFrameworkCoreOutbox(configuration);
        services.AddRelayerEntityFrameworkCoreOutbox();

        if (!applicationFirst)
        {
            AddApplicationValidator();
        }

        await using var provider = services.BuildServiceProvider();
        var expectedError = invalidLanes ? "Lanes must be positive." : "Use the orders_ prefix.";
#if NET11_0_OR_GREATER
        await Assert.That(async () => await provider.GetRequiredService<IAsyncStartupValidator>().ValidateAsync(default))
            .Throws<OptionsValidationException>().WithMessageContaining(expectedError);
#else
        await Assert.That(() => provider.GetRequiredService<IStartupValidator>().Validate())
            .Throws<OptionsValidationException>().WithMessageContaining(expectedError);
#endif
        await Assert.That(provider.GetServices<IValidateOptions<EntityFrameworkCoreOutboxOptions>>().Count()).IsEqualTo(2);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task PostgreSql_outbox_validation_composes_with_application_validation(bool applicationFirst, bool invalidLanes)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = invalidLanes ? "0" : "16",
            ["TablePrefix"] = invalidLanes ? "orders_" : "other_"
        }).Build();
        var services = new ServiceCollection();

        void AddApplicationValidator()
        {
            services.AddOptions<PostgreSqlOutboxOptions>().Validate(
                options => options.TablePrefix == "orders_", "Use the orders_ prefix.");
        }

        if (applicationFirst)
        {
            AddApplicationValidator();
        }

        services.AddRelayerPostgreSqlOutbox(configuration);
        services.AddRelayerPostgreSqlOutbox();

        if (!applicationFirst)
        {
            AddApplicationValidator();
        }

        await using var provider = services.BuildServiceProvider();
        var expectedError = invalidLanes ? "Lanes must be positive." : "Use the orders_ prefix.";
#if NET11_0_OR_GREATER
        await Assert.That(async () => await provider.GetRequiredService<IAsyncStartupValidator>().ValidateAsync(default))
            .Throws<OptionsValidationException>().WithMessageContaining(expectedError);
#else
        await Assert.That(() => provider.GetRequiredService<IStartupValidator>().Validate())
            .Throws<OptionsValidationException>().WithMessageContaining(expectedError);
#endif
        await Assert.That(provider.GetServices<IValidateOptions<PostgreSqlOutboxOptions>>().Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Neutral_EF_assembly_has_no_database_client_dependency()
    {
        var references = typeof(EntityFrameworkCoreOutboxOptions).Assembly.GetReferencedAssemblies();
        await Assert.That(references.Any(static reference => reference.Name is "Npgsql" or "Relayer.Outbox.PostgreSql"
            or "Relayer.Outbox.PostgreSql.EntityFrameworkCore")).IsFalse();
        await Assert.That(typeof(EntityFrameworkCoreOutboxExtensions).Assembly.GetName().Name)
            .IsEqualTo("Relayer.Outbox.PostgreSql.EntityFrameworkCore");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Unsupported_provider_is_rejected_when_options_are_configured_or_initialized(bool providerFirst)
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var builder = new DbContextOptionsBuilder();

        if (providerFirst)
        {
            ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(new UnsupportedProvider());
            await Assert.That(() => builder.AddRelayerOutbox(scope.ServiceProvider))
                .Throws<InvalidOperationException>().WithMessageContaining("Configure UseNpgsql");
        }
        else
        {
            builder.AddRelayerOutbox(scope.ServiceProvider);
            ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(new UnsupportedProvider());
            await Assert.That(() =>
                {
                    using var db = new DbContext(builder.Options);
                    _ = db.Model;
                })
                .Throws<InvalidOperationException>()
                .WithMessageContaining("Configure UseNpgsql");
        }
    }

    // A native provider options extension is enough: failure must precede provider service/connection use.
    private sealed class UnsupportedProvider : IDbContextOptionsExtension
    {
        public DbContextOptionsExtensionInfo Info => new ProviderInfo(this);

        public void ApplyServices(IServiceCollection services)
        {
        }

        public void Validate(IDbContextOptions options)
        {
        }

        private sealed class ProviderInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
        {
            public override bool IsDatabaseProvider => true;

            public override string LogFragment => "unsupported";

            public override int GetServiceProviderHashCode()
            {
                return 0;
            }

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            {
            }

            public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            {
                return other is ProviderInfo;
            }
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Composed_registration_preserves_native_configuration_and_scoped_enlistment(bool configureNativeFirst)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new FailingTransport());

        void Native()
        {
            services.AddDbContext<OrdersContext>(options => options.UseNpgsql(source,
                        postgres => postgres.CommandTimeout(73)));
        }

        if (configureNativeFirst)
        {
            Native();
        }

        services.AddRelayer(relayer => relayer.UsePostgreSqlOutbox<OrdersContext>(source,
            new()
            {
                DispatchStoreName = "orders",
                Schema = "messaging",
                TablePrefix = "orders_",
                Lanes = 16,
                MaxDispatchAttempts = 4
            }));

        if (!configureNativeFirst)
        {
            Native();
        }

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await ValidateRecovery(provider);
        var ordered = provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get("orders");
        var maintenance = provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get("orders");
        await Assert.That(ordered.Schema).IsEqualTo("messaging");
        await Assert.That(ordered.TablePrefix).IsEqualTo("orders_");
        await Assert.That(ordered.Lanes).IsEqualTo(16);
        await Assert.That(ordered.MaxDispatchAttempts).IsEqualTo(4);
        await Assert.That(maintenance.Schema).IsEqualTo("messaging");
        await Assert.That(maintenance.TablePrefix).IsEqualTo("orders_");
        await Assert.That(maintenance.MaxDispatchAttempts).IsEqualTo(4);
        var workers = provider.GetServices<IHostedService>().ToArray();
        await Assert.That(workers.OfType<PostgreSqlDirectDispatcher>().Count()).IsEqualTo(1);
        await Assert.That(workers.OfType<PostgreSqlOrderedDispatcher>().Count()).IsEqualTo(1);
        await Assert.That(workers.OfType<PostgreSqlMaintenanceWorker>().Count()).IsEqualTo(1);
        using var scope = provider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        await Assert.That(db.Database.GetCommandTimeout()).IsEqualTo(73);
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(outbox.SupportsScheduling).IsTrue();
        await Assert.That(outbox.SupportsOrderedScheduling).IsTrue();

        using (db.EnlistRelayerOutbox())
        {
            await Assert.That(outbox.IsActive).IsTrue();
        }

        await Assert.That(outbox.IsActive).IsFalse();
        using var other = provider.CreateScope();
        await Assert.That(other.ServiceProvider.GetRequiredService<IOutboxEnlistment>()).IsNotSameReferenceAs(outbox);
    }

    [Test]
    public async Task Composed_registration_keeps_default_and_named_options_independent()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var secondary = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MaxDispatchAttempts"] = "7"
        }).Build();
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.UsePostgreSqlOutbox<OrdersContext>(source,
            new()
            {
                MaxDispatchAttempts = 4
            }));
        services.Configure<EntityFrameworkCoreOutboxOptions>("secondary", secondary);

        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<EntityFrameworkCoreOutboxOptions>>();
        var defaults = monitor.CurrentValue;
        await Assert.That(defaults.MaxDispatchAttempts).IsEqualTo(4);

        var named = monitor.Get("secondary");
        await Assert.That(named.MaxDispatchAttempts).IsEqualTo(7);
        await Assert.That(named).IsNotSameReferenceAs(defaults);
        await Assert.That(defaults.MaxDispatchAttempts).IsEqualTo(4);
        await Assert.That(monitor.CurrentValue.MaxDispatchAttempts).IsEqualTo(4);

        using var scope = provider.CreateScope();
        var snapshot = scope.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>();
        await Assert.That(snapshot.Value.MaxDispatchAttempts).IsEqualTo(4);
        await Assert.That(snapshot.Value).IsNotSameReferenceAs(defaults);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Composed_registration_preserves_native_context_alias(bool configureNativeFirst)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new FailingTransport());

        if (configureNativeFirst)
        {
            services.AddDbContext<IOrdersContext, OrdersContext>(options => options.UseNpgsql(source));
        }

        services.AddRelayer(relayer => relayer.UsePostgreSqlOutbox<OrdersContext>(source));

        if (!configureNativeFirst)
        {
            services.AddDbContext<IOrdersContext, OrdersContext>(options => options.UseNpgsql(source));
        }

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        await Assert.That(scope.ServiceProvider.GetRequiredService<IOrdersContext>()).IsSameReferenceAs(context);
        await Assert.That(scope.ServiceProvider.GetRequiredService<DbContextOptions<OrdersContext>>().HasRelayerOutbox()).IsTrue();
    }

    [Test]
    public async Task Configured_options_combine_sections_and_reload_each_section_in_registration_order()
    {
        var first = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MaxDispatchAttempts"] = "4",
            ["TablePrefix"] = "first_"
        }).Build();
        var second = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Lanes"] = "16",
            ["TablePrefix"] = "second_"
        }).Build();
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox(first)
            .AddRelayerEntityFrameworkCoreOutbox(second);

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var snapshot = scope.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>();
        var monitor = provider.GetRequiredService<IOptionsMonitor<EntityFrameworkCoreOutboxOptions>>();
        var notifications = new List<EntityFrameworkCoreOutboxOptions>();
        using var subscription = monitor.OnChange((options, _) => notifications.Add(options));
        var initial = snapshot.Value;

        await Assert.That(initial.MaxDispatchAttempts).IsEqualTo(4);
        await Assert.That(initial.Lanes).IsEqualTo(16);
        await Assert.That(initial.TablePrefix).IsEqualTo("second_");

        first["MaxDispatchAttempts"] = "7";
        first.Reload();
        second["Lanes"] = "8";
        second.Reload();

        await Assert.That(notifications.Count).IsEqualTo(2);
        await Assert.That(notifications[0].MaxDispatchAttempts).IsEqualTo(7);
        await Assert.That(notifications[0].Lanes).IsEqualTo(16);
        await Assert.That(notifications[1].MaxDispatchAttempts).IsEqualTo(7);
        await Assert.That(notifications[1].Lanes).IsEqualTo(8);
        await Assert.That(monitor.CurrentValue).IsSameReferenceAs(notifications[1]);
        await Assert.That(snapshot.Value).IsSameReferenceAs(initial);
    }

    [Test]
    public async Task Configured_options_snapshots_resolve_fresh_validator_dependencies()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Outbox:TablePrefix"] = "before_",
            ["ExpectedPrefix"] = "before_"
        }).Build();
        var services = new ServiceCollection().AddSingleton<IConfiguration>(configuration)
            .AddTransient<PrefixPolicy>().AddRelayerEntityFrameworkCoreOutbox(configuration.GetSection("Outbox"));
        services.AddOptions<EntityFrameworkCoreOutboxOptions>().Validate<PrefixPolicy>(
            (options, policy) => options.TablePrefix == policy.Prefix, "Use the configured table prefix.");

        await using var provider = services.BuildServiceProvider();
        using var originalScope = provider.CreateScope();
        var original = originalScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>();
        await Assert.That(original.Value.TablePrefix).IsEqualTo("before_");

        configuration["Outbox:TablePrefix"] = "after_";
        configuration["ExpectedPrefix"] = "after_";
        configuration.Reload();

        using var updatedScope = provider.CreateScope();
        var updated = updatedScope.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>();
        await Assert.That(updated.Value.TablePrefix).IsEqualTo("after_");
        await Assert.That(original.Value.TablePrefix).IsEqualTo("before_");
    }

    [Test]
    public async Task Composed_registration_exposes_store_descriptor_and_interceptor_check()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new FailingTransport());
        services.AddDbContext<OrdersContext>(options => options.UseNpgsql(source));
        services.AddRelayer(relayer => relayer.UsePostgreSqlOutbox<OrdersContext>(source,
            new()
            {
                DispatchStoreName = "orders",
                Schema = "messaging",
                TablePrefix = "orders_"
            }));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = provider.GetRequiredKeyedService<PostgreSqlOutboxStoreDescriptor>("orders");
        await Assert.That(store.Name).IsEqualTo("orders");
        await Assert.That(store.DataSource).IsSameReferenceAs(source);
        await Assert.That(store.Schema).IsEqualTo("messaging");
        await Assert.That(store.TablePrefix).IsEqualTo("orders_");
        using var scope = provider.CreateScope();
        var options = scope.ServiceProvider.GetRequiredService<DbContextOptions<OrdersContext>>();
        await Assert.That(options.HasRelayerOutbox()).IsTrue();
        await Assert.That(new DbContextOptionsBuilder().UseNpgsql(source).Options.HasRelayerOutbox()).IsFalse();
    }

    [Test]
    public async Task Composed_registrations_with_matching_options_share_one_store_and_its_workers()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new FailingTransport());
        services.AddDbContext<OrdersContext>(options => options.UseNpgsql(source));
        services.AddDbContext<BillingContext>(options => options.UseNpgsql(source));
        services.AddRelayer(relayer =>
        {
            relayer.UsePostgreSqlOutbox<OrdersContext>(source);
            relayer.UsePostgreSqlOutbox<BillingContext>(source);
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await ValidateRecovery(provider);
        var workers = provider.GetServices<IHostedService>().ToArray();
        await Assert.That(workers.OfType<PostgreSqlDirectDispatcher>().Count()).IsEqualTo(1);
        await Assert.That(workers.OfType<PostgreSqlOrderedDispatcher>().Count()).IsEqualTo(1);
        await Assert.That(workers.OfType<PostgreSqlMaintenanceWorker>().Count()).IsEqualTo(1);
        using var scope = provider.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();

        using (scope.ServiceProvider.GetRequiredService<OrdersContext>().EnlistRelayerOutbox())
        {
            await Assert.That(outbox.IsActive).IsTrue();
        }

        using (scope.ServiceProvider.GetRequiredService<BillingContext>().EnlistRelayerOutbox())
        {
            await Assert.That(outbox.IsActive).IsTrue();
        }
    }

    [Test]
    [Arguments("source")]
    [Arguments("prefix")]
    [Arguments("lanes")]
    public async Task Composed_registration_rejects_a_shared_store_name_with_different_settings(string mismatch)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        await using var other = NpgsqlDataSource.Create("Host=other");
        var services = new ServiceCollection();
        await Assert.That(() => services.AddRelayer(relayer =>
        {
            relayer.UsePostgreSqlOutbox<OrdersContext>(source);
            relayer.UsePostgreSqlOutbox<BillingContext>(mismatch == "source" ? other : source, new()
            {
                TablePrefix = mismatch == "prefix" ? "billing_" : "relayer_",
                Lanes = mismatch == "lanes" ? 16 : 32
            });
        })).Throws<InvalidOperationException>().WithMessageContaining("already composed with a different NpgsqlDataSource or options");
    }

    [Test]
    public async Task Composed_registration_applies_worker_options_with_the_store_settings()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport>(new FailingTransport());
        services.AddDbContext<OrdersContext>(options => options.UseNpgsql(source));
        var maintenance = new PostgreSqlMaintenanceOptions { RecoveryDelay = TimeSpan.FromSeconds(2), SweepInterval = TimeSpan.FromMilliseconds(200), RetainSentMessages = true };
        var ordered = new PostgreSqlOrderedDispatchOptions { PollInterval = TimeSpan.FromMilliseconds(250), BatchSize = 7, RetainSentMessages = true };
        var direct = new PostgreSqlDirectDispatchOptions { QueueCapacity = 64, BatchSize = 5 };
        services.AddRelayer(relayer =>
        {
            relayer.UsePostgreSqlOutbox<OrdersContext>(source, new()
            {
                Schema = "messaging",
                MaxDispatchAttempts = 4
            },
                direct, ordered, maintenance);
            relayer.UsePostgreSqlOutbox<BillingContext>(source, new()
            {
                Schema = "messaging",
                MaxDispatchAttempts = 4
            },
                direct, ordered, maintenance);
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await ValidateRecovery(provider);
        var recovery = provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get("default");
        await Assert.That(recovery.RecoveryDelay).IsEqualTo(TimeSpan.FromSeconds(2));
        await Assert.That(recovery.SweepInterval).IsEqualTo(TimeSpan.FromMilliseconds(200));
        await Assert.That(recovery.RetainSentMessages).IsTrue();
        await Assert.That(recovery.Schema).IsEqualTo("messaging");
        await Assert.That(recovery.MaxDispatchAttempts).IsEqualTo(4);
        var lanes = provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get("default");
        await Assert.That(lanes.PollInterval).IsEqualTo(TimeSpan.FromMilliseconds(250));
        await Assert.That(lanes.BatchSize).IsEqualTo(7);
        await Assert.That(lanes.Schema).IsEqualTo("messaging");
        var dispatch = provider.GetRequiredService<IOptionsMonitor<PostgreSqlDirectDispatchOptions>>().Get("default");
        await Assert.That(dispatch.QueueCapacity).IsEqualTo(64);
        await Assert.That(dispatch.BatchSize).IsEqualTo(5);
    }

    [Test]
    [Arguments("ordered-prefix")]
    [Arguments("ordered-lease")]
    [Arguments("maintenance-schema")]
    [Arguments("maintenance-attempts")]
    [Arguments("retain")]
    public async Task Composed_registration_rejects_worker_options_that_conflict_with_the_store(string conflict)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection();
        await Assert.That(() => services.AddRelayer(relayer => relayer.UsePostgreSqlOutbox<OrdersContext>(source,
            orderedDispatch: new()
            {
                TablePrefix = conflict == "ordered-prefix" ? "other_" : "relayer_",
                LeaseDuration = conflict == "ordered-lease" ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(30),
                RetainSentMessages = conflict == "retain"
            },
            maintenance: new()
            {
                Schema = conflict == "maintenance-schema" ? "other" : null,
                MaxDispatchAttempts = conflict == "maintenance-attempts" ? 3 : 10
            }))).Throws<ArgumentException>();
        await Assert.That(services.Any(static descriptor => descriptor.ServiceType == typeof(PostgreSqlDirectDispatcher))).IsFalse();
    }

    [Test]
    public async Task Composed_registration_rejects_a_shared_store_name_with_different_worker_options()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection();
        await Assert.That(() => services.AddRelayer(relayer =>
        {
            relayer.UsePostgreSqlOutbox<OrdersContext>(source, maintenance: new()
            {
                RecoveryDelay = TimeSpan.FromSeconds(1)
            });
            relayer.UsePostgreSqlOutbox<BillingContext>(source);
        })).Throws<InvalidOperationException>().WithMessageContaining("already composed with a different NpgsqlDataSource or options");
    }

    [Test]
    [Arguments("missing")]
    [Arguments("schema")]
    [Arguments("prefix")]
    [Arguments("source")]
    public async Task Direct_dispatch_without_matching_recovery_fails_before_workers_start(string mismatch)
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        await using var other = NpgsqlDataSource.Create("Host=other");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        builder.Services.AddRelayerPostgreSqlDirectDispatch("orders", source);

        if (mismatch != "missing")
        {
            builder.Services.AddRelayerPostgreSqlMaintenance("recovery",
            mismatch == "source" ? other : source, new PostgreSqlMaintenanceOptions
            {
                Schema = mismatch == "schema" ? "other" : null,
                TablePrefix = mismatch == "prefix" ? "other_" : "relayer_"
            });
        }

        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>()
            .WithMessageContaining("requires recovery");
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("orders").ExecuteTask is null).IsTrue();
    }

    [Test]
    public async Task Direct_dispatch_rejects_conflicting_sent_history_even_when_another_recovery_worker_matches()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options => options.ServicesStartConcurrently = true);
        builder.Services.AddRelayerPostgreSqlDirectDispatch("orders", source,
            new PostgreSqlOutboxOptions { RetainSentMessages = true });
        builder.Services.AddRelayerPostgreSqlMaintenance("matching", source, new PostgreSqlMaintenanceOptions { RetainSentMessages = true });
        builder.Services.AddRelayerPostgreSqlMaintenance("conflicting", source, new PostgreSqlMaintenanceOptions { Schema = "public" });
        using var host = builder.Build();

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        await Assert.That(error!.Message).Contains("orders").And.Contains("conflicting").And.Contains("RetainSentMessages");
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("orders").ExecuteTask is null).IsTrue();
        await Assert.That(host.Services.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>("conflicting").ExecuteTask is null).IsTrue();
    }

    [Test]
    public async Task Recovery_matches_physical_store_with_a_different_worker_name_and_explicit_public_schema()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlMaintenance("recovery", source, new PostgreSqlMaintenanceOptions { Schema = "public" });
        services.AddRelayerPostgreSqlDirectDispatch("orders", source);
        await using var provider = services.BuildServiceProvider();
        await ValidateRecovery(provider);
    }

    [Test]
    public async Task Composed_registration_recovers_a_failed_first_send_from_the_committed_row()
    {
        await using var database = new PostgreSqlFixture();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var transport = new FailingTransport();
        var services = new ServiceCollection().AddLogging();
        services.AddDbContext<OrdersContext>(options => options.UseNpgsql(source));
        services.AddSingleton<ITransport>(transport);
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayer(relayer =>
        {
            relayer.UsePostgreSqlOutbox<OrdersContext>(source);
            relayer.Infrastructure().MessageTypes.Add<Probe>("registration.probe");
            relayer.AddTransport<Empty, Empty>("events", events => events.Send<Probe>("orders"));
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await ValidateRecovery(provider);
        var direct = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("default");
        await direct.StartAsync(default);

        try
        {
            using var scope = provider.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();

            using (db.EnlistRelayerOutbox())
            {
                await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(42));
                await db.SaveChangesAsync();
            }
            // Drain the direct worker deterministically, including recording the failed attempt.
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await direct.StopAsync(timeout.Token);
            await Assert.That(transport.Attempts).IsEqualTo(1);

            await using (var query = source.CreateCommand("SELECT attempts FROM relayer_outbox"))
            {
                await Assert.That((int)(await query.ExecuteScalarAsync())!).IsEqualTo(1);
            }
            // Advance persisted age and retry eligibility without waiting for wall-clock delays.
            await using (var age = source.CreateCommand("UPDATE relayer_outbox SET created_at = clock_timestamp() - interval '1 minute', deliver_at = clock_timestamp() - interval '1 second'"))
            {
                await age.ExecuteNonQueryAsync();
            }

            var recovery = provider.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>("default");
            await Assert.That(await recovery.SweepAsync(default)).IsEqualTo(1);
            await Assert.That(transport.Attempts).IsEqualTo(2);
            await Assert.That(transport.LastValue).IsEqualTo((byte)42);
            await using var remaining = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
            await Assert.That((long)(await remaining.ExecuteScalarAsync())!).IsEqualTo(0L);
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await direct.StopAsync(timeout.Token);
        }
    }

    private static async Task ValidateRecovery(IServiceProvider provider)
    {
        foreach (var validator in provider.GetServices<IHostedService>().OfType<PostgreSqlRecoveryStartupValidator>())
        {
            await validator.StartingAsync(default);
        }
    }

    public interface IOrdersContext;

    public sealed class OrdersContext(DbContextOptions<OrdersContext> options) : DbContext(options), IOrdersContext;

    private sealed class PrefixPolicy(IConfiguration configuration)
    {
        public string Prefix
        {
            get;
        } = configuration["ExpectedPrefix"]!;
    }

    public sealed class BillingContext(DbContextOptions<BillingContext> options) : DbContext(options);

    [Message("registration.probe")]
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

        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class FailingTransport : ITransport
    {
        public string Name => "events";

        public int Attempts
        {
            get; private set;
        }

        public byte LastValue
        {
            get; private set;
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (++Attempts == 1)
            {
                throw new InvalidOperationException("Broker unavailable on first send.");
            }

            LastValue = message.Body.Span[0];
            return default;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return new(new TopologyReport([]));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return default;
        }
    }

    private sealed class IndependentEnlistment : IOutboxEnlistment
    {
        public bool IsActive => false;

        public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }
    }
}
