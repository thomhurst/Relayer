namespace Relayer;

/// <summary>
/// Tells analyzer RLY001 that a generic helper method registers the identity of one of its
/// type parameters, so each call with a concrete type argument counts as a registration.
/// Startup validation still checks that the identity is registered.
/// </summary>
/// <param name="typeParameterName">The method type parameter whose type argument is registered, for example <c>nameof(TMessage)</c>.</param>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
public sealed class RegistersMessageIdentityAttribute(string typeParameterName) : Attribute
{
    /// <summary>The name of the registered method type parameter.</summary>
    public string TypeParameterName { get; } = typeParameterName;
}
