using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Relayer.Pipeline;

namespace Relayer;

/// <summary>Registers handlers from generic code, where the generated <c>Handler&lt;THandler&gt;()</c> cannot see the handler.</summary>
public static class HandlerRegistrationExtensions
{
    /// <summary>
    /// Selects <typeparamref name="THandler"/> for this consumer's <typeparamref name="TMessage"/> contract and registers
    /// it as a scoped service. Use it in generic helpers, where the handler is a type parameter; elsewhere prefer the
    /// generated <c>Handler&lt;THandler&gt;()</c>, which registers every contract the handler implements.
    /// </summary>
    /// <remarks>Dispatch is typed and reflection-free.</remarks>
    /// <exception cref="InvalidOperationException">
    /// The handler handles tombstones or is a saga handler. Use the generated <c>Handler&lt;THandler&gt;()</c>.
    /// </exception>
    public static IConsumerBuilder Handler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        this IConsumerBuilder builder)
        where THandler : class, IHandler<TMessage>
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (typeof(TMessage).IsConstructedGenericType && typeof(TMessage).GetGenericTypeDefinition() == typeof(Tombstone<>))
        {
            throw new InvalidOperationException(
                $"Handler '{typeof(THandler)}' handles tombstones. Register it with the generated consumer.Handler<{typeof(THandler).Name}>().");
        }

        if (typeof(ISagaHandler).IsAssignableFrom(typeof(THandler)))
        {
            throw new InvalidOperationException(
                $"Handler '{typeof(THandler)}' is a saga. Register it with the generated consumer.Handler<{typeof(THandler).Name}>().");
        }

        builder.AddHandler(new TypedHandlerDescriptor<THandler, TMessage>());
        builder.Services.TryAddScoped<THandler>();
        return builder;
    }

    private sealed class TypedHandlerDescriptor<THandler, TMessage>() : HandlerDescriptor<TMessage>(typeof(THandler))
        where THandler : class, IHandler<TMessage>
    {
        public override IHandlerMarker Resolve(IServiceProvider services)
        {
            return services.GetRequiredService<THandler>();
        }

        public override ValueTask DispatchAsync(IHandlerMarker handler, TMessage message, MessageContext context,
            CancellationToken cancellationToken)
        {
            return ((THandler)handler).HandleAsync(message, context, cancellationToken);
        }
    }
}
