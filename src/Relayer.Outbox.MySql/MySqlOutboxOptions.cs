namespace Relayer.Outbox.MySql;

/// <summary>Names for MySQL durable outbox storage in the data source's selected database.</summary>
public sealed class MySqlOutboxOptions
{
    /// <summary>Stable lane count for ordering keys. All writers and ordered workers must agree.</summary>
    public int Lanes { get; init; } = 64;

    /// <summary>Optional registered direct-dispatch store name. Null keeps committed rows durable without notification.</summary>
    public string? DispatchStoreName { get; init; }

    /// <summary>Prefix for table names. Each endpoint can use a separate prefix in the same database.</summary>
    public string TablePrefix { get; init; } = "relayer_";
}
