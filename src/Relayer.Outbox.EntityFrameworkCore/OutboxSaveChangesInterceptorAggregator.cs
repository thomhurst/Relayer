using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;

namespace Relayer.Outbox.EntityFrameworkCore;

#pragma warning disable EF1001 // Extend EF's own save chain so callback and result propagation stay native.
internal sealed class OutboxSaveChangesInterceptorAggregator : SaveChangesInterceptorAggregator
{
    internal static void Configure(DbContextOptionsBuilder builder)
    {
        builder.ReplaceService<IInterceptorAggregator, SaveChangesInterceptorAggregator, OutboxSaveChangesInterceptorAggregator>();
    }

    protected override ISaveChangesInterceptor CreateChain(IEnumerable<ISaveChangesInterceptor> interceptors)
    {
        // Observe the final suppression result while keeping EF's native callback chain.
        return base.CreateChain(interceptors.OrderBy(static interceptor => interceptor is OutboxSaveChangesInterceptor));
    }
}
#pragma warning restore EF1001
