using Relayer.Transports;
namespace Relayer.Testing;

/// <summary>A standalone context for invoking a handler without building a host.</summary>
public sealed class TestMessageContext : MessageContext
{
    /// <inheritdoc />
    public override string MessageId { get; } = Guid.CreateVersion7().ToString("D");
    /// <inheritdoc />
    public override MessageType MessageType { get; }
    /// <inheritdoc />
    public override MessageHeaders Headers { get; }
    /// <inheritdoc />
    public override int Attempt { get; }
    /// <inheritdoc />
    public override IServiceProvider Services { get; }
    /// <inheritdoc />
    public override MessageFeatures Features { get; } = new();
    /// <inheritdoc />
    public override CancellationToken CancellationToken { get; }

    /// <summary>Creates a context with explicit wire identity and optional services, headers and cancellation.</summary>
    public TestMessageContext(MessageType messageType, IServiceProvider? services = null,
        IReadOnlyDictionary<string, string>? headers = null, string? messageId = null,
        int attempt = 1, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (messageId is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
            MessageId = messageId;
        }
        MessageType = messageType;
        Services = services ?? new EmptyServices();
        Headers = headers is null ? MessageHeaders.Empty : new TestHeaders(headers);
        Attempt = attempt;
        CancellationToken = cancellationToken;
    }

    private sealed class EmptyServices : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    private sealed class TestHeaders(IReadOnlyDictionary<string, string> headers) : MessageHeaders
    {
        private readonly Dictionary<string, string> _headers = new(headers, StringComparer.Ordinal);
        public override bool TryGetValue(string name, out WireString value)
        {
            var found = _headers.TryGetValue(name, out var text);
            value = found ? new WireString(text!) : default;
            return found;
        }
    }
}
