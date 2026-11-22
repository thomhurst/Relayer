using Relayer.Transports;

namespace Relayer.Pipeline;

// Optional test-only observation. Production pipelines retain their existing execution path.
internal interface IRelayerObserver
{
    ValueTask ProcessAsync(DefaultMessageContext context, ConsumeDelegate next);

    ValueTask ProcessBatchAsync(ConsumePipeline owner, ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken);

    ConsumeDelegate WrapSettlement(ConsumeDelegate next);

    void Failed(DefaultMessageContext context, Exception error);

    void WorkStarted();

    void WorkCompleted();

    void DeadLettered(InMemory.InMemoryDelivery delivery, DeadLetterInfo error, (string TransportName, string Source) endpoint);

    void OutboxCommitting(ReadOnlySpan<Guid> rows);

    void OutboxSettled(Guid rowId);
}

internal interface ITransportWrapper
{
    ITransport InnerTransport
    {
        get;
    }

    internal static ITransport Unwrap(ITransport transport)
    {
        while (transport is ITransportWrapper wrapper)
        {
            transport = wrapper.InnerTransport;
        }

        return transport;
    }
}

internal interface ITransportSendTargetWrapper
{
    // Preserve observation when a consumer supplies its own send target. The caller owns the target.
    ITransport WrapSendTarget(ITransport target);
}

internal interface ITransportFailureCopyObserver
{
    // Failure copies keep the source binding, even when the destination has no send route.
    ValueTask SendCopyAsync(OutgoingMessage message, HeaderNames sourceNames, CancellationToken cancellationToken);
}
