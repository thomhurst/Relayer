using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Replies through the ordinary scoped send pipeline and its configured native route.</summary>
public static class ReplyExtensions
{
    /// <summary>Sends a correlated reply to the request's explicitly declared reply address.</summary>
    /// <remarks>Uses the active outbox or handler send buffer. The response's Send route must match the reply address.</remarks>
    public static ValueTask ReplyAsync<TResponse>(this MessageContext context, TResponse response, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(response);
        var incomingNames = context is DefaultMessageContext incoming ? incoming.EnvelopeNames
            : throw new InvalidOperationException("ReplyAsync requires a context from a Relayer consume pipeline.");
        if (!context.Headers.TryGetValue(incomingNames.ReplyTo, out var replyTo) || replyTo.IsEmpty ||
            !context.Headers.TryGetValue(incomingNames.CorrelationId, out var correlation) || correlation.IsEmpty)
            throw new InvalidOperationException("This message has no replyto or correlationid CloudEvents extension. Send it through a registered IRequestClient.");
        var route = context.Services.GetRequiredService<SendPipeline>().GetRoute<TResponse>();
        if (!route.MatchesDestination(replyTo))
            throw new InvalidOperationException("The request's replyto does not match the response's configured Send route. Declare that native destination explicitly; arbitrary reply destinations are not supported.");
        if (route.Options.EnvelopeMode != MessageEnvelopeMode.CloudEvents)
            throw new InvalidOperationException("Replies require a CloudEvents send route to preserve correlationid.");
        if (context.Services.GetRequiredService<ISender>() is not Sender sender)
            throw new InvalidOperationException("ReplyAsync requires Relayer's scoped ISender. Remove the replacement ISender registration.");
        // Send buffers and outbox stores already copy borrowed header storage before returning.
        return sender.ReplyAsync(response, correlation,
            cancellationToken == default ? context.CancellationToken : cancellationToken);
    }
}
