using System.Diagnostics;

namespace Relayer;

/// <summary>Adds tags or changes an activity's display name after sampling.</summary>
/// <remarks>Called only for recorded activities. Implementations must support concurrent calls and must not throw.</remarks>
public interface IActivityEnricher
{
    /// <summary>Enriches a recorded messaging activity.</summary>
    void Enrich(Activity activity);
}
