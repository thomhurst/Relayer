using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqTopology
{
    private readonly RabbitMqExchange[] _exchanges;
    private readonly RabbitMqQueue[] _queues;
    private readonly RabbitMqBinding[] _bindings;

    internal RabbitMqTopology(RabbitMqTransportOptions options)
    {
        _exchanges = options.Exchanges.ToArray();
        _queues = options.Queues.ToArray();
        _bindings = options.Bindings.ToArray();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var exchange in _exchanges)
        {
            ArgumentNullException.ThrowIfNull(exchange);
            ArgumentException.ThrowIfNullOrWhiteSpace(exchange.Name);
            if (exchange.Type is not null) ArgumentException.ThrowIfNullOrWhiteSpace(exchange.Type);
            if (!names.Add(exchange.Name)) throw new ArgumentException($"Duplicate RabbitMQ exchange '{exchange.Name}'.");
        }
        names.Clear();
        foreach (var queue in _queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            ArgumentException.ThrowIfNullOrWhiteSpace(queue.Name);
            if (!names.Add(queue.Name)) throw new ArgumentException($"Duplicate RabbitMQ queue '{queue.Name}'.");
        }
        foreach (var binding in _bindings)
        {
            ArgumentNullException.ThrowIfNull(binding);
            ArgumentException.ThrowIfNullOrWhiteSpace(binding.Queue);
            ArgumentException.ThrowIfNullOrWhiteSpace(binding.Exchange);
            ArgumentNullException.ThrowIfNull(binding.RoutingKey);
        }
    }

    internal async ValueTask<TopologyReport> ApplyAsync(IConnection connection, TopologyMode mode, CancellationToken cancellationToken)
    {
        var differences = new List<string>();
        foreach (var exchange in _exchanges)
        {
            var entity = $"Exchange '{exchange.Name}'";
            var exists = await ApplyEntityAsync(connection, entity, differences, cancellationToken,
                (channel, token) => mode == TopologyMode.Validate
                    ? channel.ExchangeDeclarePassiveAsync(exchange.Name, token)
                    : channel.ExchangeDeclareAsync(exchange.Name, exchange.Type ?? ExchangeType.Direct,
                        exchange.Durable ?? true, exchange.AutoDelete ?? false, exchange.Arguments, cancellationToken: token)).ConfigureAwait(false);
            if (exists && mode == TopologyMode.Validate &&
                (exchange.Type is not null || exchange.Durable is not null || exchange.AutoDelete is not null || exchange.Arguments is not null))
                Unresolved(entity, differences);
        }
        foreach (var queue in _queues)
        {
            var entity = $"Queue '{queue.Name}'";
            var exists = await ApplyEntityAsync(connection, entity, differences, cancellationToken,
                (channel, token) => mode == TopologyMode.Validate
                    ? channel.QueueDeclarePassiveAsync(queue.Name, token)
                    : channel.QueueDeclareAsync(queue.Name, queue.Durable ?? true, queue.Exclusive ?? false,
                        queue.AutoDelete ?? false, queue.Arguments, cancellationToken: token)).ConfigureAwait(false);
            if (exists && mode == TopologyMode.Validate &&
                (queue.Durable is not null || queue.Exclusive is not null || queue.AutoDelete is not null || queue.Arguments is not null))
                Unresolved(entity, differences);
        }
        foreach (var binding in _bindings)
        {
            var entity = $"Binding '{binding.Exchange}' -> '{binding.Queue}' ({binding.RoutingKey})";
            if (mode == TopologyMode.Validate)
            {
                differences.Add($"{entity}: AMQP cannot inspect binding existence or arguments. Verify bindings through infrastructure tooling; use Provision explicitly to declare them.");
                continue;
            }
            await ApplyEntityAsync(connection, entity, differences, cancellationToken,
                (channel, token) => channel.QueueBindAsync(binding.Queue, binding.Exchange, binding.RoutingKey,
                    binding.Arguments, cancellationToken: token)).ConfigureAwait(false);
        }
        return new(differences);
    }

    private static void Unresolved(string entity, List<string> differences) =>
        differences.Add($"{entity}: exists, but AMQP passive declarations cannot verify the requested properties or arguments. Use name-only declarations for existence-only validation, or Provision explicitly for native equivalence checks. Relayer never modifies or deletes existing entities.");

    private static async Task<bool> ApplyEntityAsync(IConnection connection, string entity,
        List<string> differences, CancellationToken cancellationToken, Func<IChannel, CancellationToken, Task> apply)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await using var channelLifetime = channel.ConfigureAwait(false);
        try
        {
            await apply(channel, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationInterruptedException error) when (error.ShutdownReason is { ReplyCode: 404 or 405 or 406 })
        {
            differences.Add($"{entity}: {error.ShutdownReason.ReplyText}. Reconcile the declaration through infrastructure configuration; Relayer never deletes or modifies existing entities.");
            return false;
        }
    }
}
