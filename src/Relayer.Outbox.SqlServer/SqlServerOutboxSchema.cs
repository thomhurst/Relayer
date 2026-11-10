namespace Relayer.Outbox.SqlServer;

/// <summary>Explicit initial schema deployment for SQL Server outbox primitives.</summary>
public static class SqlServerOutboxSchema
{
    /// <summary>Returns idempotent creation SQL; existing definitions are never upgraded.</summary>
    public static string CreateScript(SqlServerOutboxOptions? options = null)
    {
        options ??= new();
        var schema = options.Schema ?? "dbo";
        var quotedSchema = QuoteIdentifier(schema);
        var table = Table(options);
        var inbox = InboxTable(options);
        var index = QuoteIdentifier(options.TablePrefix + "outbox_pending_idx");
        // Dynamic DDL is needed for CREATE SCHEMA. Escape both identifier and SQL literal contexts.
        return $"""
            IF SCHEMA_ID(N'{Literal(schema)}') IS NULL
                EXEC(N'CREATE SCHEMA {Literal(quotedSchema)}');
            IF OBJECT_ID(N'{Literal(inbox)}', N'U') IS NULL
                CREATE TABLE {inbox} (
                    consumer nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    message_id uniqueidentifier NOT NULL,
                    processed_at datetimeoffset(7) NOT NULL DEFAULT SYSUTCDATETIME(),
                    PRIMARY KEY (consumer, message_id)
                );
            IF OBJECT_ID(N'{Literal(table)}', N'U') IS NULL
            BEGIN
                CREATE TABLE {table} (
                    id binary(16) NOT NULL PRIMARY KEY CHECK (id <> 0x00000000000000000000000000000000),
                    lane int NULL CHECK (lane >= 0),
                    ordering_key nvarchar(max) NULL,
                    transport nvarchar(max) NOT NULL,
                    destination nvarchar(max) NOT NULL,
                    message_type nvarchar(max) NOT NULL,
                    headers varbinary(max) NOT NULL,
                    body varbinary(max) NOT NULL,
                    created_at datetimeoffset(7) NOT NULL,
                    deliver_at datetimeoffset(7) NULL,
                    attempts int NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                    last_error nvarchar(max) NULL,
                    tenant_id nvarchar(max) NULL,
                    status varchar(7) COLLATE Latin1_General_100_BIN2 NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'parked')),
                    claim_token binary(16) NULL CHECK (claim_token <> 0x00000000000000000000000000000000),
                    claim_expires_at datetimeoffset(7) NULL,
                    CHECK ((claim_token IS NULL AND claim_expires_at IS NULL)
                        OR (claim_token IS NOT NULL AND claim_expires_at IS NOT NULL))
                );
                CREATE INDEX {index} ON {table} (lane, id)
                    INCLUDE (deliver_at, claim_expires_at) WHERE status = 'pending';
            END;
            """;
    }

    internal static string InboxTable(SqlServerOutboxOptions options) =>
        QuoteIdentifier(options.Schema ?? "dbo") + "." + QuoteIdentifier(options.TablePrefix + "inbox");

    internal static string Table(SqlServerOutboxOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        return QuoteIdentifier(options.Schema ?? "dbo") + "." + QuoteIdentifier(options.TablePrefix + "outbox");
    }

    internal static string QuoteIdentifier(string identifier)
    {
        if (identifier.Length is 0 or > 128 || identifier.Contains('\0'))
            throw new ArgumentException("SQL Server identifiers must be nonempty, contain no NUL, and fit in 128 UTF-16 code units.", nameof(identifier));
        return "[" + identifier.Replace("]", "]]", StringComparison.Ordinal) + "]";
    }

    private static string Literal(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}
