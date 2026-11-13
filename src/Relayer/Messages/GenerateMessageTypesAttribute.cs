namespace Relayer;

/// <summary>Exports a public generated registration method for this assembly's messages and JSON metadata.</summary>
/// <param name="methodName">A unique C# identifier, for example <c>AddOrderMessages</c>.</param>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class GenerateMessageTypesAttribute(string methodName) : Attribute
{
    /// <summary>The public extension method name, in the Relayer namespace.</summary>
    public string MethodName { get; } = methodName;
}
