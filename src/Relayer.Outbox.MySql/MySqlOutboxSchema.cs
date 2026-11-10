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
                seq BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
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
                INDEX pending_idx (status, lane, seq),
                INDEX unordered_idx (status, lane, id),
                INDEX scheduled_idx (deliver_at)
            ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
            CREATE TABLE IF NOT EXISTS {DeadTable(options ?? new())} (
                id BINARY(16) PRIMARY KEY, lane INT, ordering_key LONGTEXT,
                transport LONGTEXT NOT NULL, destination LONGTEXT NOT NULL, message_type LONGTEXT NOT NULL,
                headers LONGBLOB NOT NULL, body LONGBLOB NOT NULL, created_at DATETIME(6) NOT NULL,
                deliver_at DATETIME(6), attempts INT NOT NULL, last_error LONGTEXT, tenant_id LONGTEXT,
                failed_at DATETIME(6) NOT NULL, INDEX failed_idx (failed_at)
            ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
            CREATE TABLE IF NOT EXISTS {LeaseTable(options ?? new())} (
                name VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
                owner VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                expires_at DATETIME(6) NOT NULL
            ) ENGINE=InnoDB;
            CREATE TABLE IF NOT EXISTS {InboxTable(options ?? new())} (
                consumer VARBINARY(768) NOT NULL,
                message_id BINARY(16) NOT NULL,
                processed_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
                PRIMARY KEY (consumer, message_id),
                INDEX processed_idx (processed_at)
            ) ENGINE=InnoDB;
            """;
    }

    /// <summary>One-time upgrade for tables created before ordered dispatch. Stop all writers and workers first.</summary>
    /// <remarks>MySQL DDL commits implicitly. Back up first and run each step exactly once; historical insert order is unavailable.</remarks>
    public static string UpgradeOrderedScript(MySqlOutboxOptions? options = null)
    {
        var table = Table(options ?? new());
        return $"""
            ALTER TABLE {table} ADD COLUMN seq BIGINT NULL;
            CREATE TEMPORARY TABLE relayer_sequence_upgrade AS
                SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS seq FROM {table};
            UPDATE {table} AS target JOIN relayer_sequence_upgrade AS numbered ON target.id = numbered.id
                SET target.seq = numbered.seq;
            DROP TEMPORARY TABLE relayer_sequence_upgrade;
            ALTER TABLE {table} MODIFY COLUMN seq BIGINT NOT NULL AUTO_INCREMENT, ADD UNIQUE INDEX sequence_idx (seq),
                DROP INDEX pending_idx, ADD INDEX pending_idx (status, lane, seq),
                ADD INDEX unordered_idx (status, lane, id);
            {CreateScript(options)}
            """;
    }

    internal static string DeadTable(MySqlOutboxOptions options) => Identifier(options, "dead");

    internal static string LeaseTable(MySqlOutboxOptions options) => Identifier(options, "leases");

    internal static string InboxTable(MySqlOutboxOptions options) => Identifier(options, "inbox");

    internal static string Table(MySqlOutboxOptions options) => Identifier(options, "outbox");

    private static string Identifier(MySqlOutboxOptions options, string suffix)
    {
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var identifier = options.TablePrefix + suffix;
        // MySQL appends _chk_1 ... _chk_6 to the table name for these native CHECK constraints.
        if (identifier.Length > 58 || identifier.Contains('\0') || identifier.EndsWith(' '))
            throw new ArgumentException("MySQL outbox table identifiers must fit in 58 characters (52 for TablePrefix), reserve 6 for generated CHECK names, contain no NUL, and not end in a space.", nameof(options));
        foreach (var character in identifier)
            if (char.IsSurrogate(character))
                throw new ArgumentException("MySQL identifiers cannot contain supplementary Unicode characters.", nameof(options));
        return "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
    }
}
