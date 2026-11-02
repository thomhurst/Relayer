namespace Relayer;

/// <summary>The transaction mechanism a send route requires to be available.</summary>
public enum SendTransactionRequirement
{
    /// <summary>No transaction declaration.</summary>
    None,
    /// <summary>Native broker transactions, configured through the native client.</summary>
    Native,
    /// <summary>Durable enqueue in an active database outbox transaction.</summary>
    Outbox
}
