namespace Relayer.Outbox.MySql;

/// <summary>Immutable settings for one explicitly named ordered MySQL dispatcher.</summary>
public sealed class MySqlOrderedDispatchOptions
{
    /// <summary>Table prefix, identical to the transactional writer's prefix.</summary>
    public string TablePrefix { get; init; } = "relayer_";
    /// <summary>Lane count, identical on every writer and dispatcher. Drain before changing it.</summary>
    public int Lanes { get; init; } = 64;
    /// <summary>Membership and lane lease duration. Must be between one second and one hour.</summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Delay between scans, less than one third of the lease duration.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>Preparation failures before parking a row; native send failures retry with durable backoff. Must match other dispatchers for this store.</summary>
    public int MaxDispatchAttempts { get; init; } = 10;
    /// <summary>Maximum sequential sends in one bounded lane transaction.</summary>
    public int BatchSize { get; init; } = 100;

    internal MySqlOutboxOptions Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxDispatchAttempts);
        ArgumentNullException.ThrowIfNull(TablePrefix);
        if (Lanes is < 1 or > 65536) throw new ArgumentOutOfRangeException(nameof(Lanes));
        if (BatchSize is < 1 or > 10000) throw new ArgumentOutOfRangeException(nameof(BatchSize));
        if (LeaseDuration < TimeSpan.FromSeconds(1) || LeaseDuration > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(LeaseDuration));
        if (PollInterval <= TimeSpan.Zero || PollInterval >= LeaseDuration / 3)
            throw new ArgumentOutOfRangeException(nameof(PollInterval), "PollInterval must be positive and less than LeaseDuration / 3.");
        var store = new MySqlOutboxOptions { TablePrefix = TablePrefix, Lanes = Lanes };
        _ = MySqlOutboxSchema.CreateScript(store);
        return store;
    }
}
