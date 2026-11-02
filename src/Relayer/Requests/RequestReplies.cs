using System.Globalization;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

// A bounded slot table avoids dictionary nodes, locks and UTF-8 decoding on reply dispatch.
// Each reservation has a fresh identity: a late reply can never complete a later occupant of its slot.
internal sealed class RequestReplies<TRequest, TResponse>(string transport, string address, RequestOptions options) : IHandlerMarker
{
    private readonly Pending?[] _pending = new Pending[options.MaxPendingRequests];
    internal string Transport { get; } = transport;
    internal string Address { get; } = address;
    internal TimeProvider Clock { get; } = options.TimeProvider;

    internal Pending Reserve()
    {
        var pending = new Pending();
        for (var i = 0; i < _pending.Length; i++)
        {
            if (Interlocked.CompareExchange(ref _pending[i], pending, null) is null)
            {
                // No reply can know the new identity until the request is sent.
                pending.SetSlot(i);
                return pending;
            }
        }
        throw new InvalidOperationException("The request/response pair has reached MaxPendingRequests. Wait for outstanding requests or increase its bounded capacity.");
    }

    internal void Release(Pending pending) => Interlocked.CompareExchange(ref _pending[pending.Slot], null, pending);

    internal void Complete(TResponse response, WireString correlation)
    {
        Span<char> text = stackalloc char[48];
        if (!correlation.TryFormat(text, out var length)) return;
        var value = text[..length];
        var separator = value.IndexOf(':');
        if (separator < 1 || !int.TryParse(value[..separator], NumberStyles.None, CultureInfo.InvariantCulture, out var slot) ||
            (uint)slot >= (uint)_pending.Length || !Guid.TryParseExact(value[(separator + 1)..], "N", out var identity)) return;
        var pending = Volatile.Read(ref _pending[slot]);
        if (pending is not null && pending.Identity == identity) pending.Completion.TrySetResult(response);
    }

    internal sealed class Pending
    {
        internal Guid Identity { get; } = Guid.NewGuid();
        internal int Slot { get; private set; }
        internal string Correlation { get; private set; } = "";
        internal TaskCompletionSource<TResponse> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal void SetSlot(int slot)
        {
            Slot = slot;
            Correlation = string.Create(CultureInfo.InvariantCulture, $"{slot}:{Identity:N}");
        }
    }
}

internal sealed class ReplyDescriptor<TRequest, TResponse>() : HandlerDescriptor<TResponse>(typeof(RequestReplies<TRequest, TResponse>))
{
    public override IHandlerMarker Resolve(IServiceProvider services) => services.GetRequiredService<RequestReplies<TRequest, TResponse>>();

    public override ValueTask DispatchAsync(IHandlerMarker handler, TResponse message, MessageContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var names = ((DefaultMessageContext)context).EnvelopeNames;
        if (context.Headers.TryGetValue(names.CorrelationId, out var correlation))
            ((RequestReplies<TRequest, TResponse>)handler).Complete(message, correlation);
        return default;
    }
}
