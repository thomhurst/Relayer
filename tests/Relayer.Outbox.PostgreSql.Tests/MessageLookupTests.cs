using System.Diagnostics;
using Npgsql;
using Relayer.Tool;

namespace Relayer.Outbox.PostgreSql.Tests;

public class MessageLookupTests
{
    [Test]
    public async Task Reports_all_durable_states_and_receipts_without_mutating_rows()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "lookup\"schema", TablePrefix = "test_" };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        // Existing schemas have no sent-history table. Default API and CLI lookups must still work.
        await using (var drop = source.CreateCommand("DROP TABLE \"lookup\"\"schema\".test_outbox_sent"))
            await drop.ExecuteNonQueryAsync();
        var enabled = new PostgreSqlMessageLookup(source, new() { Schema = options.Schema, TablePrefix = options.TablePrefix, RetainSentMessages = true });
        var schemaError = await Assert.ThrowsAsync<InvalidOperationException>(async () => await enabled.FindAsync(Guid.NewGuid()));
        await Assert.That(schemaError!.Message).Contains("PostgreSqlOutboxSchema.CreateScript");
        var table = "\"lookup\"\"schema\".\"test_outbox\"";
        var now = DateTimeOffset.UtcNow;
        var pending = Guid.NewGuid(); var scheduled = Guid.NewGuid(); var claimed = Guid.NewGuid();
        var follower = Guid.NewGuid(); var parked = Guid.NewGuid(); var dead = Guid.NewGuid(); var absent = Guid.NewGuid();
        await PostgreSqlOutboxStoreTests.Insert(source, options, pending, now, lane: 2);
        await PostgreSqlOutboxStoreTests.Insert(source, options, scheduled, now, lane: 2, deliverAt: now.AddHours(1));
        await PostgreSqlOutboxStoreTests.Insert(source, options, claimed, now, expires: now.AddHours(1));
        await PostgreSqlOutboxStoreTests.Insert(source, options, parked, now, lane: 2, status: "parked");
        await PostgreSqlOutboxStoreTests.Insert(source, options, follower, now, lane: 2);
        await PostgreSqlOutboxStoreTests.Insert(source, options, dead, now);
        await using (var command = source.CreateCommand($"""
            INSERT INTO "lookup""schema".test_outbox_dead (id, transport, destination, message_type, headers, body, created_at, attempts, last_error, failed_at)
            SELECT id, transport, destination, message_type, headers, body, created_at, 10, 'poison', clock_timestamp() FROM {table} WHERE id IN (@dead, @parked);
            DELETE FROM {table} WHERE id = @dead;
            UPDATE {table} SET attempts = 3, last_error = 'retry' WHERE id = @claimed;
            INSERT INTO "lookup""schema".test_leases VALUES ('lane:2', 'worker', clock_timestamp() + interval '1 hour');
            INSERT INTO "lookup""schema".test_inbox VALUES ('consumer-a', @absent, clock_timestamp()), ('consumer-b', @absent, clock_timestamp());
            """))
        {
            command.Parameters.AddWithValue("dead", dead); command.Parameters.AddWithValue("parked", parked);
            command.Parameters.AddWithValue("claimed", claimed); command.Parameters.AddWithValue("absent", absent);
            await command.ExecuteNonQueryAsync();
        }
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(RelayerCommand).Assembly.Location);
        foreach (var argument in new[] { "message", "locate", "--id", scheduled.ToString(), "--schema", options.Schema!, "--prefix", options.TablePrefix })
            start.ArgumentList.Add(argument);
        start.Environment["RELAYER_POSTGRES_CONNECTION_STRING"] = fixture.ConnectionString;
        using (var process = Process.Start(start)!)
        {
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await process.WaitForExitAsync(timeout.Token); }
            finally { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            await Assert.That(process.ExitCode).IsEqualTo(0);
            await Assert.That(await stdout).Contains("State: scheduled");
            await Assert.That(await stderr).IsEqualTo("");
        }
        start.ArgumentList.Add("--sent-history");
        using (var process = Process.Start(start)!)
        {
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await process.WaitForExitAsync(timeout.Token); }
            finally { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            await Assert.That(process.ExitCode).IsEqualTo(1);
            await Assert.That(await stderr).Contains("PostgreSqlOutboxSchema.CreateScript");
            await Assert.That(await stdout).IsEqualTo("");
        }
        var lookup = new PostgreSqlMessageLookup(source, options);
        await Assert.That((await lookup.FindAsync(pending)).State).IsEqualTo("pending");
        var future = await lookup.FindAsync(scheduled);
        await Assert.That(future.State).IsEqualTo("scheduled");
        await Assert.That(future.DeliverAt.HasValue).IsTrue();
        await Assert.That(future.Lane).IsEqualTo(2);
        await Assert.That(future.LaneOwner).IsEqualTo("worker");
        await Assert.That(future.LaneExpiresAt.HasValue).IsTrue();
        await Assert.That(future.EarlierRows).IsEqualTo(1L);
        var active = await lookup.FindAsync(claimed);
        await Assert.That(active.State).IsEqualTo("claimed");
        await Assert.That(active.ClaimToken.HasValue).IsTrue();
        await Assert.That(active.ClaimExpiresAt.HasValue).IsTrue();
        await Assert.That(active.Attempts).IsEqualTo(3);
        await Assert.That(active.LastError).IsEqualTo("retry");
        await Assert.That((await lookup.FindAsync(parked)).State).IsEqualTo("parked");
        await Assert.That((await lookup.FindAsync(follower)).State).IsEqualTo("parked");
        var failed = await lookup.FindAsync(dead);
        await Assert.That(failed.State).IsEqualTo("dead");
        await Assert.That(failed.LastError).IsEqualTo("poison");
        await Assert.That(failed.FailedAt.HasValue).IsTrue();
        var missing = await lookup.FindAsync(absent);
        await Assert.That(missing.State).IsEqualTo("not present");
        await Assert.That(missing.InboxReceipts.Count).IsEqualTo(2);
        await Assert.That(missing.InboxReceipts[0].Consumer).IsEqualTo("consumer-a");
        await using var count = source.CreateCommand($"SELECT count(*) FROM {table}");
        await Assert.That((long)(await count.ExecuteScalarAsync())!).IsEqualTo(5L);
        await using var expire = source.CreateCommand($"UPDATE {table} SET claim_expires_at = clock_timestamp() - interval '1 hour' WHERE id = @id");
        expire.Parameters.AddWithValue("id", claimed);
        await expire.ExecuteNonQueryAsync();
        await Assert.That((await lookup.FindAsync(claimed)).State).IsEqualTo("pending");
    }

    [Test]
    public async Task Lookup_command_validates_options_before_connecting()
    {
        using var output = new StringWriter(); using var error = new StringWriter();
        await Assert.That(await RelayerCommand.RunAsync(["message", "locate", "--id", "invalid"], output, error)).IsEqualTo(1);
        await Assert.That(error.ToString()).Contains("nonempty message UUID");
        error.GetStringBuilder().Clear();
        await Assert.That(await RelayerCommand.RunAsync(["message", "locate", "--unknown", "x"], output, error)).IsEqualTo(1);
        await Assert.That(error.ToString()).Contains("Unknown lookup option");
    }
}
