namespace Relayer;

/// <summary>
/// Declares the stable wire identity of a message type. The name is written to the
/// <c>ce_type</c> CloudEvents attribute and must never change once messages are in flight.
/// </summary>
/// <param name="name">The stable, transport-independent message type name, e.g. <c>billing.charge-card</c>.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class MessageAttribute(string name) : Attribute
{
    /// <summary>The stable message type name.</summary>
    public string Name { get; } = name;

    /// <summary>The contract version. Defaults to 1.</summary>
    public int Version { get; init; } = 1;
}
