using System.Text;

namespace Relayer;

/// <summary>An application's assertion that its selected native store belongs to an authorized tenant.</summary>
/// <remarks>Create this in the scoped native store factory after authorization, and attach it to that
/// store's outbox options. Relayer validates scope and wire metadata; the application remains responsible
/// for authorizing the tenant and selecting the correct native connection, database and schema.</remarks>
public sealed class OutboxTenantBinding
{
    private readonly TenantContext _context;
    private readonly byte[] _utf8;
    private readonly long _leaseVersion;

    /// <summary>Binds the selected store to the current scope and its application-authorized tenant ID.</summary>
    public OutboxTenantBinding(TenantContext context, string authorizedTenantId)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(authorizedTenantId);
        _context = context;
        _leaseVersion = context.LeaseVersion;
        TenantId = authorizedTenantId;
        // Validate text once, without replacement fallbacks that could alias distinct tenant IDs.
        _utf8 = new UTF8Encoding(false, true).GetBytes(authorizedTenantId);
        Validate();
    }

    /// <summary>The immutable authorized ID written to durable tenant metadata.</summary>
    public string TenantId { get; }

    internal void ValidateContext(TenantContext context)
    {
        if (!ReferenceEquals(context, _context))
            throw new InvalidOperationException("The outbox tenant binding belongs to a different scope. Create it in this scope's authorized native store factory.");
        Validate();
    }

    internal void Validate()
    {
        var current = _context.Id;
        if (_context.LeaseVersion != _leaseVersion)
            throw new InvalidOperationException("The outbox tenant binding belongs to an earlier scope. Create a fresh binding in the authorized native store factory.");
        if (!Matches(current))
            throw new InvalidOperationException("The current tenant does not match the authorized outbox store. Create a fresh authorized scope and native store for each tenant or retry.");
    }

    internal OutboxRecord Bind(OutboxRecord record)
    {
        Validate();
        if (record.TenantId is not null && !string.Equals(record.TenantId, TenantId, StringComparison.Ordinal))
            throw new InvalidOperationException("OutboxRecord.TenantId does not match the authorized outbox store.");
        var found = false;
        foreach (var header in record.Message.Headers.Span)
        {
            if (header.Key != TenantContext.HeaderName) continue;
            if (found || !Matches(header.Value))
                throw new InvalidOperationException("The outgoing tenant header must occur once and match the authorized outbox store.");
            found = true;
        }
        if (!found)
            throw new InvalidOperationException("The outgoing message requires a tenant header matching the authorized outbox store. Use the scoped ISender or supply the tenant header explicitly.");
        return record with { TenantId = TenantId };
    }

    private bool Matches(WireString value) => value.Text is { } text
        ? string.Equals(text, TenantId, StringComparison.Ordinal)
        : value.Utf8.Span.SequenceEqual(_utf8);
}
