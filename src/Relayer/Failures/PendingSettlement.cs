namespace Relayer;

// One instance per pooled context. Requests describe an outcome; they never settle native deliveries inline.
internal sealed class PendingSettlement(DefaultMessageContext context, ConsumePipeline owner) : ISettlementFeature
{
    internal Disposition? Request { get; private set; }

    public void Complete() => Set(Disposition.Complete);
    public void Retry(TimeSpan delay) => Set(Disposition.Redeliver(delay));
    public void DeadLetter(string reason) => Set(Disposition.DeadLettered(new(reason)));

    private void Set(Disposition disposition)
    {
        if (!context.CanRequestSettlement)
            throw new InvalidOperationException("Settlement can be requested only during message handling and scope disposal.");
        if (Request is not null)
            throw new InvalidOperationException("Only one settlement request is allowed per processing attempt.");
        owner.ValidateSettlement(disposition);
        Request = disposition;
    }

    internal void Clear() => Request = null;
}
