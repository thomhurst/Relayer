using Relayer.Transports;
namespace Relayer.Pipeline;

// One instance per pooled context. Requests describe an outcome; they never settle native deliveries inline.
internal sealed class PendingSettlement(DefaultMessageContext context, ConsumePipeline owner) : ISettlementFeature
{
    internal Disposition? Request { get; private set; }
    public Disposition? RequestedDisposition => Request;

    public void Complete() => Set(Disposition.Complete);
    public void Retry(TimeSpan delay) => Set(Disposition.Redeliver(delay));
    public void DeadLetter(string reason) => Set(Disposition.DeadLettered(new(reason)));

    private void Set(Disposition disposition)
    {
        if (!context.CanRequestSettlement)
            throw new InvalidOperationException("Settlement can be requested only during message handling and scope disposal.");
        if (context.ObservedDisposition.Kind == DispositionKind.SettledByHandler)
            throw new InvalidOperationException("This delivery has already been settled natively by the handler.");
        if (Request is not null)
            throw new InvalidOperationException("Only one settlement request is allowed per processing attempt.");
        owner.ValidateSettlement(disposition);
        Request = disposition;
    }

    internal void Clear() => Request = null;
}
