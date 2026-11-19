using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Selects the database context that owns outgoing messages.</summary>
public static class DbContextOutboxExtensions
{
    /// <summary>Selects the context that owns sends before an implicit SaveChanges transaction, or disambiguates explicit transactions.</summary>
    /// <remarks>Await each enqueue before saving or disposing the selection. Disposing the last selection discards unsaved messages outside an explicit transaction.</remarks>
    public static IOutboxSelection EnlistRelayerOutbox(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var options = context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>();
        if (options?.Interceptors is { } interceptors)
            foreach (var interceptor in interceptors)
                if (interceptor is OutboxSaveChangesInterceptor save)
                    return save.Outbox.Select(context, save.Configuration, save.Services);
        throw new InvalidOperationException("Configure this DbContext with AddRelayerOutbox before calling EnlistRelayerOutbox.");
    }
}
