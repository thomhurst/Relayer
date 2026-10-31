namespace Relayer;

/// <summary>How Relayer treats broker entities at startup (docs/decisions.md D16).</summary>
public enum TopologyMode
{
    /// <summary>Check that declared entities exist and fail fast with a diff. Needs least privilege.</summary>
    Validate,

    /// <summary>Create missing entities. Intended for development.</summary>
    Provision,

    /// <summary>Do nothing.</summary>
    None,
}
