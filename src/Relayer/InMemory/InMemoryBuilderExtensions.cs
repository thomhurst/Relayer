using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.InMemory;

/// <summary>Registers in-memory transports using the common typed route and consumer builders.</summary>
public static class InMemoryBuilderExtensions
{
    /// <summary>Declares a named process-local transport. Each declared source or destination creates one explicit endpoint.</summary>
    public static IRelayerBuilder InMemory(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<InMemoryTransportOptions, InMemoryRouteOptions, InMemoryConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var configuration = provider.GetRequiredService<RelayerConfiguration>();
            var definition = configuration.Transports.Single(transport => transport.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<InMemoryTransportOptions>>().Get(name);
            var endpoints = new Dictionary<string, InMemoryEndpointOptions>(StringComparer.Ordinal);
            foreach (var route in definition.Routes) endpoints.TryAdd(route.Destination, new());
            foreach (var consumer in definition.Consumers) endpoints.TryAdd(consumer.Source, new());
            foreach (var pair in options.Endpoints)
            {
                if (!endpoints.ContainsKey(pair.Key))
                    throw new InvalidOperationException($"In-memory transport '{name}': endpoint '{pair.Key}' has options but no route or consumer declaration.");
                endpoints[pair.Key] = pair.Value;
            }
            return new InMemoryTransport(name, endpoints, options.TimeProvider, provider.GetRequiredService<MessageTracing>(),
                provider.GetService<ILoggerFactory>());
        });
        builder.Services.AddSingleton<IHostedService>(provider => new InMemoryHostedService(provider, name));
        return builder;
    }
}

internal sealed class InMemoryHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private InMemoryTransport? _transport;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport = (InMemoryTransport)provider.GetServices<ITransport>().Single(transport => transport.Name == name);
        var configuration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(transport => transport.Name == name);
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        foreach (var declaration in configuration.Consumers)
        {
            var consumer = (ConsumerDefinition<InMemoryConsumerOptions>)declaration;
            if (consumer.BatchSize != 1)
                throw new NotSupportedException($"In-memory consumer '{consumer.Source}': batch-handler composition is not implemented. Use single-message handlers.");
            var capabilities = _transport.GetCapabilities(consumer.Source);
            var failure = consumer.Options.Failure;
            if (failure is null)
            {
                if (!capabilities.DeadLetter)
                    throw new InvalidOperationException($"In-memory consumer '{consumer.Source}': enable DeadLetter or configure Options.Failure with a durable core failure executor.");
                failure = static (context, error, action) =>
                {
                    context.Features.GetRequired<NativeMessage<InMemoryDelivery>>().Disposition = action == FailureAction.Discard
                        ? Disposition.Complete : Disposition.DeadLettered(new("handler_failure", error.Message));
                    return default;
                };
            }
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
                failure, provider.GetService<IFailureClassifier>());
            pipeline.Tracing(provider.GetRequiredService<MessageTracing>(), new()
            {
                System = InMemoryTransport.MessagingSystem, Destination = consumer.Source, ConsumerGroup = consumer.Options.Group,
            }).Settle(SettleAsync);
            foreach (var handler in handlers.Descriptors)
                if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
            var receive = new ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>(pipeline.Build(consumer.Concurrency), new HeaderNames(), consumer.Concurrency);
            _transport.RegisterConsumer(consumer.Source, receive.ProcessAsync, consumer.Options.Group, consumer.Concurrency);
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ValueTask SettleAsync(MessageContext context)
    {
        var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
        var disposition = native.Disposition;
        switch (disposition.Kind)
        {
            case DispositionKind.SettledByHandler: return default;
            case DispositionKind.Complete: native.Message.Complete(); break;
            case DispositionKind.Redeliver: native.Message.Redeliver(disposition.Delay); break;
            case DispositionKind.DeadLetter: native.Message.DeadLetter(disposition.DeadLetter); break;
            default: throw new InvalidOperationException("Unknown in-memory settlement disposition.");
        }
        native.Disposition = Disposition.SettledByHandler;
        return default;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => _transport!.Completion;

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_transport is not null) await _transport.StopAsync(cancellationToken).ConfigureAwait(false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
