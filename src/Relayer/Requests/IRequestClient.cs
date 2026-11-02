namespace Relayer;

/// <summary>Requests a reply through explicitly declared native endpoints.</summary>
public interface IRequestClient<in TRequest, TResponse>
{
    /// <summary>Sends immediately and waits for one correlated reply within a mandatory finite timeout.</summary>
    /// <remarks>Pending state is process-local. Cancellation cannot retract a message already accepted by the broker.</remarks>
    ValueTask<TResponse> RequestAsync(TRequest request, TimeSpan timeout, CancellationToken cancellationToken = default);
}
