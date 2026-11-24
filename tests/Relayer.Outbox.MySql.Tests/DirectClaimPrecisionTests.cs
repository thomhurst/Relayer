using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

public sealed class DirectClaimPrecisionTests
{
    [Test]
    [Arguments(false, 19L, 20L, 0)]
    [Arguments(true, 19L, 20L, 0)]
    [Arguments(false, 19L, 20L, 9)]
    [Arguments(true, 19L, 20L, 9)]
    [Arguments(false, 20L, 20L, 9)]
    [Arguments(true, 20L, 20L, 9)]
    [Arguments(false, 863_999_999_999_999_999L, 864_000_000_000_000_000L, 0)]
    [Arguments(true, 863_999_999_999_999_999L, 864_000_000_000_000_000L, 0)]
    public async Task Stored_deadline_rounds_up_from_the_claim_timestamp(bool batch, long durationTicks,
        long expectedTicks, int subMicrosecondTicks)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        using var store = new MySqlOutboxStore(database.Source, database.Options);
        var acquisition = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(9_999_990);
        var now = acquisition.AddTicks(subMicrosecondTicks);
        var expires = now.AddTicks(durationTicks);
        var claims = new OutboxClaim[batch ? 2 : 1];

        for (var i = 0; i < claims.Length; i++)
        {
            claims[i] = new(Guid.NewGuid(), Guid.NewGuid());
            await database.Insert(claims[i].MessageId, acquisition, deliverAt: acquisition);
        }

        if (batch)
        {
            var outcomes = new bool[claims.Length];
            await store.ClaimBatchAsync(claims, outcomes, now, expires, default);
            await Assert.That(outcomes).IsEquivalentTo(new[] { true, true });
        }
        else
        {
            await Assert.That(await store.TryClaimForDispatchAsync(claims[0], now, expires, default)).IsTrue();
        }

        await using var connection = await database.Source.OpenConnectionAsync();

        foreach (var claim in claims)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT claim_expires_at FROM relayer_outbox WHERE id = @id AND claim_token = @token";
            command.Parameters.AddWithValue("id", claim.MessageId.ToByteArray(bigEndian: true));
            command.Parameters.AddWithValue("token", claim.Token.ToByteArray(bigEndian: true));
            await using var reader = await command.ExecuteReaderAsync();
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetDateTime(0).Ticks - acquisition.UtcTicks).IsEqualTo(expectedTicks);
        }
    }
}
