using System.Globalization;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.PostgreSql;

// Classifier scopes and context views exist only on the failed-send path.
internal sealed class PostgreSqlDispatchFailures(IServiceScopeFactory? scopes)
{
    private readonly DefaultFailureClassifier _fallback = new();

    internal async ValueTask<FailureAction> ClassifyAsync(Exception error, Guid id, string wireType,
        int attempt, string? tenantId, OutgoingMessage message, CancellationToken cancellationToken)
    {
        if (error is PostgreSqlLocalFailure local) return local.Action;
        if (scopes is null) return _fallback.Classify(error, new Context(id, wireType, attempt, message, null, cancellationToken));
        await using var scope = scopes.CreateAsyncScope();
        var services = scope.ServiceProvider;
        if (tenantId is not null && services.GetService<TenantContext>() is { } tenant) tenant.Id = new(tenantId);
        var classifier = services.GetService<IFailureClassifier>() ?? _fallback;
        return classifier.Classify(error, new Context(id, wireType, attempt, message, services, cancellationToken));
    }

    private sealed class Context(Guid id, string wireType, int attempt, OutgoingMessage message,
        IServiceProvider? services, CancellationToken cancellationToken) : MessageContext
    {
        public override string MessageId => id.ToString();
        public override MessageType MessageType
        {
            get
            {
                var separator = wireType.LastIndexOf(";v=", StringComparison.Ordinal);
                return separator >= 0 && int.TryParse(wireType.AsSpan(separator + 3), NumberStyles.None, CultureInfo.InvariantCulture, out var version)
                    ? new(wireType[..separator], version) : new(wireType, 1);
            }
        }
        public override MessageHeaders Headers { get; } = new HeadersView(message.Headers);
        public override int Attempt => attempt;
        public override IServiceProvider Services => services ?? throw new InvalidOperationException("Dispatch classifier services require a registered IServiceScopeFactory.");
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => cancellationToken;
    }

    private sealed class HeadersView(ReadOnlyMemory<KeyValuePair<string, WireString>> headers) : MessageHeaders
    {
        public override bool TryGetValue(string name, out WireString value)
        {
            foreach (var header in headers.Span)
                if (header.Key == name) { value = header.Value; return true; }
            value = default;
            return false;
        }
    }
}
