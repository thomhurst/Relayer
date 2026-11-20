using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

/// <summary>A borrowed durable local attempt. Use Transaction for business commands; never complete or retain it.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public readonly record struct PostgreSqlLocalDelivery(OutgoingMessage Message, OutboxClaim Claim,
    NpgsqlTransaction Transaction, int Attempt)
{
    private readonly NpgsqlTransaction? _transaction = Transaction;

    /// <summary>The row's fenced transaction for business commands. Not available to consumers that use <see cref="PostgreSqlLocalCompletion.AtLeastOnce"/>.</summary>
    /// <exception cref="InvalidOperationException">The consumer uses at-least-once completion, so its handler runs without a delivery transaction.</exception>
    public NpgsqlTransaction Transaction
    {
        get
        {
            return _transaction ?? throw new InvalidOperationException(
                        "This PostgreSQL local consumer uses PostgreSqlLocalCompletion.AtLeastOnce, so its handler runs without a delivery transaction. " +
                        "Open your own connection for business writes, or use PostgreSqlLocalCompletion.Atomic to write through the delivery transaction.");
        }
        init
        {
            _transaction = value;
        }
    }

    internal NpgsqlTransaction? OpenTransaction => _transaction;

    internal PostgreSqlLocalTransport Owner
    {
        get; init;
    } = null!;

    internal PostgreSqlOutboxStore CompletionStore
    {
        get; init;
    } = null!;
}

/// <summary>Reads original outbox payloads and CloudEvents headers without copying.</summary>
[Experimental("RLYEXP002", UrlFormat = "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyexp002")]
public readonly struct PostgreSqlLocalMessageBinding : IMessageBinding<PostgreSqlLocalDelivery>
{
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(PostgreSqlLocalDelivery message)
    {
        return new(message.Message.Body);
    }

    /// <inheritdoc />
    public static int? GetDeliveryCount(PostgreSqlLocalDelivery message)
    {
        return message.Attempt;
    }

    /// <inheritdoc />
    public static bool TryGetHeader(PostgreSqlLocalDelivery message, string name, out WireString value)
    {
        foreach (var header in message.Message.Headers.Span)
        {
            if (header.Key == name)
            {
                value = header.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
