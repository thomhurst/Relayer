using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace Relayer.Outbox.PostgreSql.Tests;

// Build relational metadata before EF publishes the cached runtime model to concurrent callers.
// Remove this convention once both target frameworks include the fix for dotnet/efcore#39214.
internal sealed class InitializeRelationalModel : IModelFinalizedConvention
{
    public IModel ProcessModelFinalized(IModel model)
    {
        _ = model.GetRelationalModel();
        return model;
    }
}
