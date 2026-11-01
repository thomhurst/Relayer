namespace Relayer;

/// <summary>A step in the consume pipeline.</summary>
public delegate ValueTask ConsumeDelegate(MessageContext context);

/// <summary>Consume-side middleware. The pipeline is composed once at startup.</summary>
public interface IConsumeMiddleware
{
    /// <summary>Runs this middleware, calling <paramref name="next"/> to continue.</summary>
    ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next);
}

/// <summary>A step in the send pipeline.</summary>
public delegate ValueTask SendDelegate(SendContext context);

/// <summary>Send-side middleware. The pipeline is composed once at startup.</summary>
public interface ISendMiddleware
{
    /// <summary>Runs this middleware, calling <paramref name="next"/> to continue.</summary>
    ValueTask InvokeAsync(SendContext context, SendDelegate next);
}

/// <summary>Context for an outgoing message. Borrowed only until the middleware invocation completes.</summary>
public abstract class SendContext
{
    /// <summary>The outgoing message.</summary>
    public abstract object Message { get; }

    /// <summary>The wire identity of the message.</summary>
    public abstract MessageType MessageType { get; }

    /// <summary>The stable message ID (UUIDv7).</summary>
    public abstract string MessageId { get; }

    /// <summary>The send options.</summary>
    public abstract SendOptions Options { get; }

    /// <summary>Cancellation for the send.</summary>
    public abstract CancellationToken CancellationToken { get; }
}
