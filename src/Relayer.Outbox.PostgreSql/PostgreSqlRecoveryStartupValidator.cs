using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

internal sealed record PostgreSqlDispatchRegistration(string Name, NpgsqlDataSource DataSource,
    PostgreSqlOutboxOptions Options, PostgreSqlDispatchKind Kind)
{
    internal bool SharesStore(PostgreSqlDispatchRegistration other)
    {
        return ReferenceEquals(DataSource, other.DataSource) &&
            (Options.Schema ?? "public") == (other.Options.Schema ?? "public") &&
            Options.TablePrefix == other.Options.TablePrefix;
    }
}

internal enum PostgreSqlDispatchKind
{
    Direct,
    Maintenance,
    Ordered
}

// Validate in StartingAsync, before any worker starts, including with concurrent hosted startup.
internal sealed class PostgreSqlRecoveryStartupValidator(IEnumerable<PostgreSqlDispatchRegistration> registrations,
    IEnumerable<PostgreSqlLocalStore> localStores, IServiceProvider provider)
    : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        var stores = registrations.ToArray();

        foreach (var dispatch in stores)
        {
            if (dispatch.Kind != PostgreSqlDispatchKind.Direct)
            {
                continue;
            }

            var recovered = false;

            foreach (var candidate in stores)
            {
                if (candidate.Kind != PostgreSqlDispatchKind.Maintenance || !dispatch.SharesStore(candidate))
                {
                    continue;
                }

                if (dispatch.Options.RetainSentMessages != candidate.Options.RetainSentMessages)
                {
                    throw new OptionsValidationException(dispatch.Name, typeof(PostgreSqlDirectDispatchOptions),
                        [$"PostgreSQL direct dispatch store '{dispatch.Name}' and maintenance worker '{candidate.Name}' must use the same RetainSentMessages setting."]);
                }

                recovered = true;
            }

            if (!recovered)
            {
                throw new OptionsValidationException(dispatch.Name, typeof(PostgreSqlDirectDispatchOptions),
                    [$"PostgreSQL direct dispatch store '{dispatch.Name}' requires recovery. Register AddRelayerPostgreSqlMaintenance with the same NpgsqlDataSource, schema and table prefix, or use UsePostgreSqlOutbox<TContext>."]);
            }
        }

        foreach (var local in localStores)
        {
            var direct = stores.SingleOrDefault(store =>
                store.Kind == PostgreSqlDispatchKind.Direct && store.Name == local.DispatchStoreName)
                ?? throw new InvalidOperationException($"PostgreSQL local transport '{local.Name}' requires direct dispatch store '{local.DispatchStoreName}'.");
            local.ValidateWorker(direct);

            foreach (var worker in stores)
            {
                if (!ReferenceEquals(worker, direct) && local.SharesStore(worker))
                {
                    local.ValidateWorker(worker);
                }
            }

            if (provider.GetService<PostgreSqlOutboxWriterRegistration>()?.DispatchStoreName is { } writerStore &&
                stores.Any(store => store.Kind == PostgreSqlDispatchKind.Direct && store.Name == writerStore && local.SharesStore(store)))
            {
                var options = provider.GetRequiredService<IOptionsMonitor<PostgreSqlOutboxOptions>>().CurrentValue;

                if (local.HasWriterLaneMismatch(options.Schema, options.TablePrefix, options.Lanes))
                {
                    local.ValidateWriterLanes(options.Lanes);
                }
            }
        }

        return Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StartedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StoppedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
