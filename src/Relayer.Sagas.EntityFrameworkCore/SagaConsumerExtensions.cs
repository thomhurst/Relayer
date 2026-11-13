using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;

namespace Relayer.Sagas.EntityFrameworkCore;

/// <summary>Composes explicit process state on the existing EF Core consumer inbox/outbox boundary.</summary>
public static class SagaConsumerExtensions
{
    /// <summary>Validates native state concurrency and configures the existing transactional consumer boundary.</summary>
    /// <remarks>Register first. Use the existing failure pipeline to retry the whole transition in a fresh scope.</remarks>
    public static ConsumePipelineBuilder Saga<TContext,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors
            | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.NonPublicFields
            | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.NonPublicProperties
            | DynamicallyAccessedMemberTypes.Interfaces)] TState>(this ConsumePipelineBuilder builder, string consumer)
        where TContext : DbContext
        where TState : EntityFrameworkCoreSagaData
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.Use(services =>
        {
            var middleware = EntityFrameworkCoreConsumerExtensions.Create<TContext>(services, consumer, requireOutbox: true);
            var database = services.GetRequiredService<TContext>();
            var entity = database.Model.FindEntityType(typeof(TState))
                ?? throw new InvalidOperationException($"Map saga state '{typeof(TState)}' in the selected DbContext.");
            var key = entity.FindPrimaryKey();
            if (key is null || key.Properties.Count != 1 || key.Properties[0].Name != nameof(EntityFrameworkCoreSagaData.Id)
                || key.Properties[0].ValueGenerated != ValueGenerated.Never)
                throw new InvalidOperationException("Saga state requires Id as its sole primary key with ValueGeneratedNever.");
            var version = entity.FindProperty(nameof(EntityFrameworkCoreSagaData.Version));
            if (version is not { IsConcurrencyToken: true, ValueGenerated: ValueGenerated.OnAddOrUpdate }
                || version.GetColumnName() != "xmin")
                throw new InvalidOperationException("Configure saga state Version with Property(s => s.Version).IsRowVersion() to use PostgreSQL xmin concurrency.");
            return middleware;
        });
    }

    /// <summary>Attaches the existing saga transaction boundary to a fluent consumer declaration. Register first.</summary>
    public static IConsumerBuilder Saga<TContext,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors
            | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.NonPublicFields
            | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.NonPublicProperties
            | DynamicallyAccessedMemberTypes.Interfaces)] TState>(this IConsumerBuilder builder, string consumer)
        where TContext : DbContext
        where TState : EntityFrameworkCoreSagaData
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.ConfigurePipeline(pipeline => pipeline.Saga<TContext, TState>(consumer));
    }
}
