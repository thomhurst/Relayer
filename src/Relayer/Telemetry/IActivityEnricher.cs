using System.Diagnostics;

namespace Relayer;

/// <summary>Adds tags or changes an activity's display name after sampling.</summary>
/// <remarks>Called only for recorded activities. Implementations must support concurrent calls and should not throw. Enricher failures are ignored to protect delivery.</remarks>
public interface IActivityEnricher
{
    /// <summary>Enriches a recorded messaging activity.</summary>
    void Enrich(Activity activity);
}
