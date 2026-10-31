namespace Relayer;

/// <summary>The wire identity of a message type.</summary>
/// <param name="Name">The stable message type name.</param>
/// <param name="Version">The contract version.</param>
public readonly record struct MessageType(string Name, int Version)
{
    /// <summary>Formats the identity as written to the <c>ce_type</c> attribute, e.g. <c>billing.charge-card;v=2</c>.</summary>
    public override string ToString() => $"{Name};v={Version}";
}
