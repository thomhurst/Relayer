using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.InMemory;

/// <summary>Registers in-memory transports using the common typed route and consumer builders.</summary>
public static class InMemoryBuilderExtensions
{
    /// <summary>Declares a named process-local transport. Each declared source or destination creates one explicit endpoint.</summary>
    public static IRelayerBuilder InMemory(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<InMemoryTransportOptions, InMemoryConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var configuration = provider.GetRequiredService<RelayerConfiguration>();
            var definition = configuration.Transports.Single(transport => transport.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<InMemoryTransportOptions>>().Get(name);
            var endpoints = new Dictionary<string, InMemoryEndpointOptions>(StringComparer.Ordinal);

            foreach (var route in definition.Routes)
            {
                endpoints.TryAdd(route.Destination, new());
            }

            foreach (var consumer in definition.Consumers)
            {
                endpoints.TryAdd(consumer.Source, new());
            }

            foreach (var pair in options.Endpoints)
            {
                if (!endpoints.ContainsKey(pair.Key))
                {
                    throw new InvalidOperationException($"In-memory transport '{name}': endpoint '{pair.Key}' has options but no route or consumer declaration.");
                }

                endpoints[pair.Key] = pair.Value;
            }

            var transport = new InMemoryTransport(name, endpoints, options.TimeProvider, provider.GetRequiredService<MessageTracing>(),
                provider.GetService<ILoggerFactory>());
            transport.Observe(provider.GetService<IRelayerObserver>());
            return transport;
        });
        builder.Services.AddSingleton<IHostedService>(provider => new InMemoryHostedService(provider, name));
        return builder;
    }
}

internal sealed class InMemoryHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private InMemoryTransport? _transport;
    private bool _started;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // The host can retry after a later hosted service fails while this transport is still running.
        if (_started)
        {
            return;
        }

        // Keep completed registrations if native startup observes cancellation before starting its loops.
        _transport ??= RegisterConsumers(cancellationToken);
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);

        // Once native loops start, observe their completion even if the startup token is canceled.
        await base.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _started = true;
    }

    private InMemoryTransport RegisterConsumers(CancellationToken cancellationToken)
    {
        var registered = provider.GetServices<ITransport>().Single(transport => transport.Name == name);
        var transport = (InMemoryTransport)ITransportWrapper.Unwrap(registered);
        var configuration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(transport => transport.Name == name);
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var headerNames = new HeaderNames();
        ValidateHostedRouteHeaders(configuration, headerNames);

        // Compose every pipeline before registering any native receivers so a failed composition can be retried.
        var receivers = new List<(string Source, InMemoryReceiveDelegate Receive, string? Group, int Concurrency)>(
            configuration.Consumers.Count);

        foreach (var declaration in configuration.Consumers)
        {
            var consumer = (ConsumerDefinition<InMemoryConsumerOptions>)declaration;

            if (consumer.BatchSize != 1)
            {
                throw new NotSupportedException($"In-memory consumer '{consumer.Source}': batch-handler composition is not implemented. Use single-message handlers.");
            }

            if (!transport.GetCapabilities(consumer.Source).DeadLetter &&
                string.Equals(consumer.Failure.DeadLetterEndpoint, consumer.Source, StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"In-memory consumer '{consumer.Source}': DeadLetterEndpoint must differ from the source so a dead-letter copy does not wait on its own admission credit. Enable native dead-lettering or select a separate endpoint.");
            }

            var failure = new FailurePipeline(provider, name, consumer.Source, consumer.Options.Group ?? consumer.Source,
                true, consumer.Failure, Copy, provider.GetService<IFailureScheduler>(),
                provider.GetRequiredService<IOptionsMonitor<InMemoryTransportOptions>>().Get(name).TimeProvider);
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name),
                failure, consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>());
            pipeline.Tracing(provider.GetRequiredService<MessageTracing>(), new()
            {
                System = InMemoryTransport.MessagingSystem,
                Destination = consumer.Source,
                ConsumerGroup = consumer.Options.Group,
            }).Settle(SettleAsync);
            pipeline.ConfigureConsumer(provider, consumer);

            foreach (var handler in handlers.Descriptors)
            {
                if (consumer.Selects(handler))
                {
                    pipeline.Handler(handler);
                }
            }

            var receive = new ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>(
                pipeline.Build(consumer.Concurrency), headerNames, consumer.Concurrency);
            receivers.Add((consumer.Source, receive.ProcessAsync, consumer.Options.Group, consumer.Concurrency));
        }

        cancellationToken.ThrowIfCancellationRequested();

        foreach (var receiver in receivers)
        {
            transport.RegisterConsumer(receiver.Source, receiver.Receive, receiver.Group, receiver.Concurrency);
        }

        return transport;
    }

    private static void ValidateHostedRouteHeaders(TransportDefinition configuration, HeaderNames receiveHeaders)
    {
        foreach (var route in configuration.Routes)
        {
            if (route.SendOptions.EnvelopeMode == MessageEnvelopeMode.CloudEvents &&
                route.SendOptions.HeaderNames.Type != receiveHeaders.Type &&
                configuration.Consumers.Any(consumer => consumer.Source == route.Destination))
            {
                throw new InvalidOperationException(
                    $"In-memory transport '{configuration.Name}': the CloudEvents send route for '{route.MessageType.Name}' to endpoint '{route.Destination}' writes '{route.SendOptions.HeaderNames.Type}', but hosted consumers require '{receiveHeaders.Type}'. Use the default SendRouteOptions.HeaderNames.");
            }
        }
    }

    private static OutgoingMessage Copy(MessageContext context, string destination)
    {
        var message = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Message.Message;
        return new(destination, message.Body, message.Headers, message.OrderingKey);
    }

    private static ValueTask SettleAsync(MessageContext context)
    {
        var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
        var disposition = native.Disposition;

        switch (disposition.Kind)
        {
            case DispositionKind.SettledByHandler:
                return default;
            case DispositionKind.Complete:
                native.Message.Complete();
                break;
            case DispositionKind.Redeliver:
                native.Message.Settle(disposition);
                break;
            case DispositionKind.DeadLetter:
                native.Message.DeadLetter(disposition.DeadLetter);
                break;
            default:
                throw new InvalidOperationException("Unknown in-memory settlement disposition.");
        }

        native.Disposition = Disposition.SettledByHandler;
        return default;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return _transport!.Completion;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_transport is not null)
        {
            _started = false;
            await _transport.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
