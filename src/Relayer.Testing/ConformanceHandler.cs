using Relayer.Transports;
namespace Relayer.Testing;

// The generator composes the same dispatch path used by application handlers.
[Message("relayer.conformance")]
internal sealed record ConformanceMessage(int Value);

internal sealed class ConformanceControl
{
    internal Func<MessageContext, ValueTask<Disposition>> Handle { get; set; } = static _ => new(Disposition.Complete);
}

internal sealed record ConformanceDecision(Disposition Value);

internal sealed class ConformanceHandler(ConformanceControl control) : IHandler<ConformanceMessage>
{
    public async ValueTask HandleAsync(ConformanceMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var disposition = await control.Handle(context).ConfigureAwait(false);
        context.Features.Set(new ConformanceDecision(disposition));
    }
}
