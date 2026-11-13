namespace Relayer.Transports;

/// <summary>Creates a borrowed copy of the original body, all headers, and native ordering key.</summary>
/// <remarks>
/// Called only on failure. Preserve malformed envelopes, original identity and creation context as-is.
/// The pipeline replaces Destination, DeliverAt and its own error headers. Storage must survive until the returned send completes.
/// Use the transport's NativeMessage feature to access data not exposed by the lazy context.
/// </remarks>
public delegate OutgoingMessage FailureMessageCopyDelegate(MessageContext context);

/// <summary>An available executor that commits a scheduled copy to durable storage independently of the failed attempt.</summary>
public interface IFailureScheduler
{
    /// <summary>Persist the borrowed message before returning. A successful return permits completing the original.</summary>
    ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken);
}
