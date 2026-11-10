using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Relayer.Outbox.EntityFrameworkCore;

// EF owns exception unwrapping, the retry budget and exhaustion. The existing consumer wrapper
// supplies a fresh scope/transaction per callback; a database conflict always rolls back.
internal sealed class PostgreSqlConflictExecutionStrategy(DbContext context)
    : ExecutionStrategy(context, maxRetryCount: 3, maxRetryDelay: TimeSpan.Zero)
{
    protected override bool ShouldRetryOn(Exception exception)
        => exception is PostgresException
        {
            SqlState: PostgresErrorCodes.SerializationFailure or PostgresErrorCodes.DeadlockDetected
        };

    protected override bool ShouldVerifySuccessOn(Exception exception) => false;
}
