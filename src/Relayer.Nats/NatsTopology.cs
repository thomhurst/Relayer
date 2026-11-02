using NATS.Client.JetStream;

namespace Relayer.Nats;

internal sealed class NatsTopology
{
    private readonly NatsStream[] _streams;
    private readonly NatsConsumer[] _consumers;

    internal NatsTopology(NatsTransportOptions options)
    {
        _streams = options.Streams.ToArray();
        _consumers = options.Consumers.ToArray();
        var streams = new HashSet<string>(StringComparer.Ordinal);
        foreach (var stream in _streams)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ValidateName(stream.Name);
            if (!streams.Add(stream.Name)) throw new ArgumentException($"Duplicate NATS stream '{stream.Name}'.");
            if (stream.Configuration is { } config && config.Name != stream.Name)
                throw new ArgumentException($"NATS stream '{stream.Name}': native StreamConfig.Name must match the declaration.");
        }
        var consumers = new HashSet<(string Stream, string Name)>();
        foreach (var consumer in _consumers)
        {
            ArgumentNullException.ThrowIfNull(consumer);
            ValidateName(consumer.Stream);
            ValidateName(consumer.Name);
            if (!consumers.Add((consumer.Stream, consumer.Name)))
                throw new ArgumentException($"Duplicate NATS consumer '{consumer.Stream}/{consumer.Name}'.");
            if (consumer.Configuration is { } config && (config.Name != consumer.Name ||
                config.DurableName is { } durable && durable != consumer.Name))
                throw new ArgumentException($"NATS consumer '{consumer.Stream}/{consumer.Name}': native ConsumerConfig.Name and any DurableName must match the declaration.");
        }
    }

    internal async ValueTask<TopologyReport> ApplyAsync(NatsJSContext context, TopologyMode mode, CancellationToken token)
    {
        var differences = new List<string>();
        foreach (var stream in _streams)
        {
            token.ThrowIfCancellationRequested();
            var entity = $"Stream '{stream.Name}'";
            try
            {
                if (mode == TopologyMode.Provision && stream.Configuration is { } config)
                    await context.CreateStreamAsync(config, token).ConfigureAwait(false);
                else
                {
                    await context.GetStreamAsync(stream.Name, cancellationToken: token).ConfigureAwait(false);
                    if (stream.Configuration is not null) Unverified(entity, differences);
                }
            }
            catch (NatsJSApiException error) when (error.Error.Code is 400 or 404)
            { differences.Add($"{entity}: {error.Error.Description}. Supply native Configuration to provision a missing entity; reconcile conflicting settings externally. Relayer never updates or deletes entities."); }
        }
        foreach (var consumer in _consumers)
        {
            token.ThrowIfCancellationRequested();
            var entity = $"Consumer '{consumer.Stream}/{consumer.Name}'";
            try
            {
                if (mode == TopologyMode.Provision && consumer.Configuration is { } config)
                    await context.CreateConsumerAsync(consumer.Stream, config, token).ConfigureAwait(false);
                else
                {
                    await context.GetConsumerAsync(consumer.Stream, consumer.Name, token).ConfigureAwait(false);
                    if (consumer.Configuration is not null) Unverified(entity, differences);
                }
            }
            catch (NatsJSApiException error) when (error.Error.Code is 400 or 404)
            { differences.Add($"{entity}: {error.Error.Description}. Supply native Configuration to provision a missing entity; reconcile conflicting settings externally. Relayer never updates or deletes entities."); }
        }
        return new(differences);
    }

    private static void Unverified(string entity, List<string> differences) =>
        differences.Add($"{entity}: exists, but read-only configuration equivalence checks are not implemented. Use a name-only declaration for existence validation, or Provision explicitly for native create-only equivalence checks.");

    private static void ValidateName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.AsSpan().ContainsAny(".*>/\\"))
            throw new ArgumentException($"Invalid NATS entity name '{name}': use a native stream or consumer name without '.', '*', '>', '/' or '\\'.");
    }
}
