namespace Relayer;

// Optional test-only observation. Production pipelines retain their existing execution path.
internal interface IRelayerObserver
{
    ValueTask ProcessAsync(DefaultMessageContext context, ConsumeDelegate next);
    ValueTask ProcessBatchAsync(ConsumePipeline owner, ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken);
    ConsumeDelegate WrapSettlement(ConsumeDelegate next);
    void Failed(DefaultMessageContext context, Exception error);
    void WorkStarted();
    void WorkCompleted();
    void DeadLettered(InMemory.InMemoryDelivery delivery, DeadLetterInfo error);
}

internal interface ITransportWrapper
{
    Transports.ITransport InnerTransport { get; }
}
