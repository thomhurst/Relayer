using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

internal sealed record PostgreSqlDispatchRegistration(string Name, NpgsqlDataSource DataSource,
    PostgreSqlOutboxOptions Options, bool Recovery = false);

// Validate in StartingAsync, before any worker starts, including with concurrent hosted startup.
internal sealed class PostgreSqlRecoveryStartupValidator(IEnumerable<PostgreSqlDispatchRegistration> registrations)
    : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        var stores = registrations.ToArray();
        foreach (var dispatch in stores)
        {
            if (dispatch.Recovery) continue;
            var recovered = false;
            foreach (var candidate in stores)
                if (candidate.Recovery && ReferenceEquals(dispatch.DataSource, candidate.DataSource) &&
                    (dispatch.Options.Schema ?? "public") == (candidate.Options.Schema ?? "public") &&
                    dispatch.Options.TablePrefix == candidate.Options.TablePrefix)
                {
                    recovered = true;
                    break;
                }
            if (!recovered)
                throw new OptionsValidationException(dispatch.Name, typeof(PostgreSqlDirectDispatchOptions),
                    [$"PostgreSQL direct dispatch store '{dispatch.Name}' requires recovery. Register AddRelayerPostgreSqlMaintenance with the same NpgsqlDataSource, schema and table prefix, or use UsePostgreSqlOutbox<TContext>."]);
        }
        return Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
