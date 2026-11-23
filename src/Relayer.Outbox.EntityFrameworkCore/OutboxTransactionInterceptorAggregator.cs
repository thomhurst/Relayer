using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;

namespace Relayer.Outbox.EntityFrameworkCore;

#pragma warning disable EF1001 // Extend EF's own transaction chain so callback and result propagation stay native.
internal sealed class OutboxTransactionInterceptorAggregator : DbTransactionInterceptorAggregator
{
    internal static void Configure(DbContextOptionsBuilder builder)
    {
        builder.ReplaceService<IInterceptorAggregator, DbTransactionInterceptorAggregator, OutboxTransactionInterceptorAggregator>();
    }

    protected override IDbTransactionInterceptor CreateChain(IEnumerable<IDbTransactionInterceptor> interceptors)
    {
        // EF calls TransactionCommitted even when the final committing callback suppresses the native commit.
        return base.CreateChain(interceptors.OrderBy(static interceptor => interceptor is OutboxTransactionInterceptor));
    }
}
#pragma warning restore EF1001
