namespace Relayer.Outbox.MySql;

/// <summary>Creates the MySQL durable outbox table in the connection's selected database.</summary>
public static class MySqlOutboxSchema
{
    /// <summary>Returns idempotent MySQL 8.4+ SQL for a new table. Existing definitions are not upgraded.</summary>
    /// <remarks>UUID bytes use network order; dates use UTC DATETIME(6). Apply with application deployment tooling.</remarks>
    public static string CreateScript(MySqlOutboxOptions? options = null)
    {
        var table = Table(options ?? new());
        return $"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BINARY(16) PRIMARY KEY CHECK (id <> X'00000000000000000000000000000000'),
                lane INT CHECK (lane >= 0),
                ordering_key LONGTEXT,
                transport LONGTEXT NOT NULL,
                destination LONGTEXT NOT NULL,
                message_type LONGTEXT NOT NULL,
                headers LONGBLOB NOT NULL,
                body LONGBLOB NOT NULL,
                created_at DATETIME(6) NOT NULL,
                deliver_at DATETIME(6),
                attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                last_error LONGTEXT,
                tenant_id LONGTEXT,
                status VARCHAR(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'parked')),
                claim_token BINARY(16) CHECK (claim_token <> X'00000000000000000000000000000000'),
                claim_expires_at DATETIME(6),
                CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL)),
                INDEX pending_idx (status, lane, id),
                INDEX scheduled_idx (deliver_at)
            ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
            """;
    }

    internal static string Table(MySqlOutboxOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var identifier = options.TablePrefix + "outbox";
        // MySQL appends _chk_1 ... _chk_6 to the table name for these native CHECK constraints.
        if (identifier.Length > 58 || identifier.Contains('\0') || identifier.EndsWith(' '))
            throw new ArgumentException("MySQL outbox table identifiers must fit in 58 characters (52 for TablePrefix), reserve 6 for generated CHECK names, contain no NUL, and not end in a space.", nameof(options));
        foreach (var character in identifier)
            if (char.IsSurrogate(character))
                throw new ArgumentException("MySQL identifiers cannot contain supplementary Unicode characters.", nameof(options));
        return "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
    }
}
