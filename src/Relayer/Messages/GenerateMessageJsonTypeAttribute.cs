namespace Relayer;

/// <summary>Gives a payload type an explicit metadata property name in this assembly's generated message JSON context.</summary>
/// <remarks>Referenced payload types must be visible through the global alias for System.Text.Json source generation.</remarks>
/// <param name="type">The payload type that needs a distinct metadata name.</param>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
public sealed class GenerateMessageJsonTypeAttribute(Type type) : Attribute
{
    /// <summary>The payload type to include in the generated context.</summary>
    public Type Type
    {
        get;
    } = type;
}
