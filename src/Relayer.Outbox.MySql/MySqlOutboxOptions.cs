namespace Relayer.Outbox.MySql;

/// <summary>Names for MySQL durable outbox storage in the data source's selected database.</summary>
public sealed class MySqlOutboxOptions
{
    /// <summary>Prefix for table names. Each endpoint can use a separate prefix in the same database.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
