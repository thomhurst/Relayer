using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;
using Relayer.HealthChecks;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DatabaseSchemaTests
{
    [Test]
    public async Task Scripts_are_idempotent_and_shared_with_EF_and_health_queries()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        PostgreSqlOutboxOptions[] configurations = [new(), new() { Schema = "custom", TablePrefix = "orders_" },
            new() { Schema = new string('s', 63), TablePrefix = "" },
            new() { Schema = "Case\";--{{schema}}", TablePrefix = "é\".{{outbox}}_" }];
        foreach (var options in configurations)
        {
            var script = PostgreSqlOutboxSchema.CreateScript(options);
            await Execute(connection, script);
            await Execute(connection, script);
            await VerifyCatalog(connection, options);

            // Execute the EF migration operation in its caller-owned transaction as EF would.
            var migration = new MigrationBuilder("Npgsql.EntityFrameworkCore.PostgreSQL");
            migration.CreateRelayerPostgreSqlSchema(options);
            await using (var transaction = await connection.BeginTransactionAsync())
            {
                await Execute(connection, ((SqlOperation)migration.Operations[0]).Sql);
                await transaction.CommitAsync();
            }
            await VerifyRowsAndHealth(connection, options);
            await Execute(connection, script);
            await Assert.That(await Scalar<long>(connection, $"SELECT count(*) FROM {Table(options, "outbox")}")).IsEqualTo(23L);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Insert_sequence_migration_preserves_old_UUID_order_and_continues_after_existing_rows(bool populated)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        var options = new PostgreSqlOutboxOptions { Schema = "quote'\";--{{schema}}", TablePrefix = "{{outbox}}_'" };
        var oldSchema = PostgreSqlOutboxSchema.CreateScript(options)
            .Replace("    seq bigint GENERATED ALWAYS AS IDENTITY,\n", "", StringComparison.Ordinal)
            .Replace("(lane, seq)", "(lane, id)", StringComparison.Ordinal)
            .Replace(" AND lane IS NOT NULL", "", StringComparison.Ordinal)
            .Replace("(lane, ordering_key, seq)", "(lane, ordering_key, id)", StringComparison.Ordinal);
        await Execute(connection, oldSchema);
        await Execute(connection, "DROP INDEX " + Table(options, "outbox_unordered_idx"));
        var table = Table(options, "outbox");
        var insert = $"INSERT INTO {table} (id, transport, destination, message_type, headers, body, created_at) VALUES ";
        const string rest = ", 'events', 'orders', 'probe', ''::bytea, ''::bytea, clock_timestamp())";
        if (populated)
        {
            await Execute(connection, insert + "('01900000-0000-7000-8000-000000000002'" + rest);
            await Execute(connection, insert + "('01900000-0000-7000-8000-000000000001'" + rest);
        }
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await Execute(connection, PostgreSqlOutboxSchema.CreateInsertSequenceMigrationScript(options));
            await transaction.CommitAsync();
        }
        await Execute(connection, insert + "('01900000-0000-7000-8000-000000000003'" + rest);
        await Assert.That(await Scalar<long>(connection, $"SELECT seq FROM {table} WHERE id = '01900000-0000-7000-8000-000000000003'"))
            .IsEqualTo(populated ? 3L : 1L);
        if (populated)
            await Assert.That(await Scalar<string>(connection, $"SELECT string_agg(id::text, ',' ORDER BY seq) FROM {table}"))
                .IsEqualTo("01900000-0000-7000-8000-000000000001,01900000-0000-7000-8000-000000000002,01900000-0000-7000-8000-000000000003");
        await VerifyCatalog(connection, options);
        await Assert.That(await Scalar<string>(connection,
            $"SELECT attidentity::text FROM pg_attribute WHERE attrelid = '{table.Replace("'", "''", StringComparison.Ordinal)}'::regclass AND attname = 'seq'"))
            .IsEqualTo("a");
    }

    private static async Task VerifyCatalog(NpgsqlConnection connection, PostgreSqlOutboxOptions options)
    {
        var expected = new Dictionary<string, string[]>
        {
            ["outbox"] = ["id:uuid", "seq:bigint", "lane:integer", "ordering_key:text", "transport:text", "destination:text", "message_type:text",
                "headers:bytea", "body:bytea", "created_at:timestamp with time zone", "deliver_at:timestamp with time zone",
                "attempts:integer", "last_error:text", "tenant_id:text", "status:text", "claim_token:uuid", "claim_expires_at:timestamp with time zone"],
            ["outbox_dead"] = ["id:uuid", "lane:integer", "ordering_key:text", "transport:text", "destination:text", "message_type:text",
                "headers:bytea", "body:bytea", "created_at:timestamp with time zone", "deliver_at:timestamp with time zone",
                "attempts:integer", "last_error:text", "tenant_id:text", "failed_at:timestamp with time zone"],
            ["inbox"] = ["consumer:text", "message_id:uuid", "processed_at:timestamp with time zone"],
            ["leases"] = ["name:text", "owner:text", "expires_at:timestamp with time zone"]
        };
        foreach (var (table, columns) in expected)
        {
            await using var command = new NpgsqlCommand("""
                SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY column_name)
                FROM information_schema.columns WHERE table_schema = @schema AND table_name = @table
                """, connection);
            command.Parameters.AddWithValue("schema", options.Schema ?? "public");
            command.Parameters.AddWithValue("table", options.TablePrefix + table);
            await Assert.That((string)(await command.ExecuteScalarAsync())!).IsEqualTo(string.Join(',', columns.Order(StringComparer.Ordinal)));
        }
        await using var indexes = new NpgsqlCommand("SELECT indexdef FROM pg_indexes WHERE schemaname = @schema AND tablename = @table", connection);
        indexes.Parameters.AddWithValue("schema", options.Schema ?? "public");
        indexes.Parameters.AddWithValue("table", options.TablePrefix + "outbox");
        await using var reader = await indexes.ExecuteReaderAsync();
        var definitions = new List<string>();
        while (await reader.ReadAsync()) definitions.Add(reader.GetString(0));
        await Assert.That(definitions.Count).IsEqualTo(5);
        await Assert.That(definitions.Any(x => x.Contains("(lane, seq) WHERE ((status = 'pending'::text) AND (lane IS NOT NULL))"))).IsTrue();
        await Assert.That(definitions.Any(x => x.Contains("(id) WHERE ((status = 'pending'::text) AND (lane IS NULL))"))).IsTrue();
        await Assert.That(definitions.Any(x => x.Contains("(deliver_at) WHERE (deliver_at IS NOT NULL)"))).IsTrue();
        await Assert.That(definitions.Any(x => x.Contains("(lane, ordering_key, seq) WHERE (status = 'parked'::text)"))).IsTrue();
    }

    private static async Task VerifyRowsAndHealth(NpgsqlConnection connection, PostgreSqlOutboxOptions options)
    {
        var outbox = Table(options, "outbox");
        const string now = "'2026-11-01T12:00:00Z'::timestamptz";
        var insert = $"INSERT INTO {outbox} (id, transport, destination, message_type, headers, body, created_at";
        const string values = "gen_random_uuid(), 'kafka', 'orders', 'order.placed.v1', decode('0102','hex'), decode('0304','hex'), ";
        await Execute(connection, $"{insert}, deliver_at) VALUES ({values}{now}, {now} + interval '1 second')");
        await Execute(connection, $"{insert}, status) VALUES ({values}{now}, 'parked')");
        await Execute(connection, $"{insert}, claim_token, claim_expires_at) VALUES ({values}{now}, gen_random_uuid(), {now} + interval '1 second')");
        await Execute(connection, $"INSERT INTO {Table(options, "outbox_dead")} (id, transport, destination, message_type, headers, body, created_at, failed_at) VALUES ({values}{now}, {now})");

        var query = $"SELECT count(*) FROM {outbox} WHERE status = 'pending' AND (deliver_at IS NULL OR deliver_at <= @now) AND (claim_expires_at IS NULL OR claim_expires_at <= @now)";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayer(_ => { });
        services.AddKeyedSingleton<IOutboxHealthProvider>("database", new DatabaseCountProvider(connection, query));
        services.AddRelayerHealthChecks(new RelayerHealthCheckOptions
        {
            OutboxNames = ["database"], OutboxDegradedThreshold = 10, OutboxUnhealthyThreshold = 20
        });
        await using var provider = services.BuildServiceProvider();
        await CheckHealth(provider, 0, HealthStatus.Healthy);

        // Both scheduling and claim expiry are inclusive at the supplied UTC instant.
        await Execute(connection, $"{insert}, deliver_at, tenant_id, lane, ordering_key) VALUES ({values}{now}, {now}, 'tenant-a', 7, 'key')");
        await Execute(connection, $"{insert}, claim_token, claim_expires_at) VALUES ({values}{now}, gen_random_uuid(), {now})");
        await CheckHealth(provider, 2, HealthStatus.Healthy);
        await Execute(connection, $"{insert}, claim_token, claim_expires_at) VALUES ({values}{now}, gen_random_uuid(), {now} - interval '1 second')");
        await CheckHealth(provider, 3, HealthStatus.Healthy);
        var current = 3;
        foreach (var (count, status) in new[] { (9, HealthStatus.Healthy), (10, HealthStatus.Degraded), (19, HealthStatus.Degraded), (20, HealthStatus.Unhealthy) })
        {
            await Execute(connection, $"{insert}) SELECT {values}{now} FROM generate_series(1, {count - current})");
            current = count;
            await CheckHealth(provider, count, status);
        }

        foreach (var (columns, data) in new[] { ("attempts", "-1"), ("status", "'dead'"), ("lane", "-1"),
            ("claim_token", "gen_random_uuid()"), ("claim_expires_at", now),
            ("claim_token, claim_expires_at", $"'00000000-0000-0000-0000-000000000000', {now}") })
            await ExpectViolation(connection, $"{insert}, {columns}) VALUES ({values}{now}, {data})", PostgresErrorCodes.CheckViolation);
        await ExpectViolation(connection, $"INSERT INTO {outbox} DEFAULT VALUES", PostgresErrorCodes.NotNullViolation);

        var inboxInsert = $"INSERT INTO {Table(options, "inbox")} VALUES ('consumer', '00000000-0000-7000-8000-000000000001', {now})";
        await Execute(connection, inboxInsert);
        await ExpectViolation(connection, inboxInsert, PostgresErrorCodes.UniqueViolation);
        await Execute(connection, $"INSERT INTO {Table(options, "inbox")} VALUES ('other-consumer', '00000000-0000-7000-8000-000000000001', {now})");
        var leaseInsert = $"INSERT INTO {Table(options, "leases")} VALUES ('lane:7', 'instance-a', {now})";
        await Execute(connection, leaseInsert);
        await ExpectViolation(connection, leaseInsert, PostgresErrorCodes.UniqueViolation);
        await Assert.That(await Scalar<string>(connection, $"SELECT encode(headers, 'hex') || ':' || encode(body, 'hex') || ':' || tenant_id FROM {outbox} WHERE tenant_id = 'tenant-a'"))
            .IsEqualTo("0102:0304:tenant-a");
    }

    private static async Task CheckHealth(ServiceProvider provider, long count, HealthStatus status)
    {
        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        var entry = report.Entries["relayer.outbox"];
        await Assert.That(entry.Status).IsEqualTo(status);
        await Assert.That((long)((HealthCheckResult)entry.Data["database"]).Data["pending_count"]).IsEqualTo(count);
    }

    private sealed class DatabaseCountProvider(NpgsqlConnection connection, string query) : IOutboxHealthProvider
    {
        public async ValueTask<long?> GetPendingCountAsync(CancellationToken cancellationToken)
        {
            await using var command = new NpgsqlCommand(query, connection);
            // A nonzero offset verifies that comparison is by UTC instant.
            command.Parameters.AddWithValue("now", new DateTimeOffset(2026, 11, 1, 13, 0, 0, TimeSpan.FromHours(1)).ToUniversalTime());
            return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
        }
    }

    private static string Table(PostgreSqlOutboxOptions options, string table)
    {
        using var builder = new NpgsqlCommandBuilder();
        return builder.QuoteIdentifier(options.Schema ?? "public") + "." + builder.QuoteIdentifier(options.TablePrefix + table);
    }

    private static async Task Execute(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ExpectViolation(NpgsqlConnection connection, string sql, string sqlState)
    {
        try
        {
            await Execute(connection, sql);
            throw new InvalidOperationException("Expected a PostgreSQL constraint violation.");
        }
        catch (PostgresException exception)
        {
            await Assert.That(exception.SqlState).IsEqualTo(sqlState);
        }
    }

    private static async Task<T> Scalar<T>(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync())!;
    }
}
