using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class PostgreSqlConnectionTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Fixture_uses_plaintext_password_authentication_without_encryption_fallback(bool invalidPassword)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        var settings = new NpgsqlConnectionStringBuilder(fixture.ConnectionString);
        if (invalidPassword) settings.Password = "incorrect-password";
        var negotiations = 0;
        var builder = new NpgsqlDataSourceBuilder(settings.ConnectionString)
            .UseNegotiateOptionsCallback(_ => Interlocked.Increment(ref negotiations));
        await using var source = builder.Build();
        if (invalidPassword)
        {
            var failure = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var connection = await source.OpenConnectionAsync();
            });
            await Assert.That(failure!.SqlState).IsEqualTo(PostgresErrorCodes.InvalidPassword);
        }
        else
        {
            await using var connection = await source.OpenConnectionAsync();
            await using var command = new NpgsqlCommand("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", connection);
            await Assert.That((bool)(await command.ExecuteScalarAsync())!).IsFalse();
        }
        await Assert.That(negotiations).IsEqualTo(0);
        await Assert.That(settings.SslMode).IsEqualTo(SslMode.Disable);
        await Assert.That(settings.GssEncryptionMode).IsEqualTo(GssEncryptionMode.Disable);
        await Assert.That(settings.Pooling).IsFalse();
        await Assert.That(settings.Timeout).IsEqualTo(2);
    }

    [Test]
    public async Task Authentication_failure_is_not_replaced_by_a_failed_reconnection()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var settings = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            Port = ((IPEndPoint)listener.LocalEndpoint).Port
        };
        var rejected = RejectAuthenticationAsync(listener, timeout.Token);
        try
        {
            await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
            var failure = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var connection = await source.OpenConnectionAsync(timeout.Token);
            });
            await Assert.That(failure!.SqlState).IsEqualTo(PostgresErrorCodes.InvalidPassword);
            await Assert.That(failure.MessageText).IsEqualTo("original authentication failure");
        }
        finally
        {
            listener.Stop();
            await timeout.CancelAsync();
            try { await rejected; }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
        }
    }

    private static async Task RejectAuthenticationAsync(TcpListener listener, CancellationToken token)
    {
        using var client = await listener.AcceptTcpClientAsync(token);
        await using var stream = client.GetStream();
        var length = new byte[4];
        while (true)
        {
            await stream.ReadExactlyAsync(length, token);
            var payload = new byte[BinaryPrimitives.ReadInt32BigEndian(length) - 4];
            await stream.ReadExactlyAsync(payload, token);
            if (payload.Length == 4 && BinaryPrimitives.ReadInt32BigEndian(payload) is 80877103 or 80877104)
            {
                // Refuse optional SSL/GSS probes, just like the plaintext fixture server.
                await stream.WriteAsync(new byte[] { (byte)'N' }, token);
                continue;
            }
            break; // StartupMessage received; authentication can now fail.
        }
        // Preserve the accepted socket, but make a hidden second connection fail.
        listener.Stop();
        var body = Encoding.UTF8.GetBytes("SFATAL\0C28P01\0Moriginal authentication failure\0\0");
        var error = new byte[5 + body.Length];
        error[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(error.AsSpan(1), body.Length + 4);
        body.CopyTo(error, 5);
        await stream.WriteAsync(error, token);
    }
}
