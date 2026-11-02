namespace Relayer;

/// <summary>Application identity for an AsyncAPI document.</summary>
public sealed class AsyncApiOptions
{
    /// <summary>The application's human-readable title.</summary>
    public required string Title { get; init; }

    /// <summary>The application API version, independent of the AsyncAPI specification version.</summary>
    public required string Version { get; init; }
}
