namespace Relayer;

/// <summary>
/// Tells analyzer RLY001 that the application registers identities for every closed form of a
/// generic message type at startup, for example <c>typeof(Envelope&lt;&gt;)</c>. RLY001 stays on for every
/// other type; startup validation still checks that each used identity is registered.
/// </summary>
/// <param name="messageType">The generic type definition, or a single type, whose identities are registered at startup.</param>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
public sealed class MessageIdentitiesRegisteredAtStartupAttribute(Type messageType) : Attribute
{
    /// <summary>The generic type definition or type whose identities are registered at startup.</summary>
    public Type MessageType { get; } = messageType;
}
