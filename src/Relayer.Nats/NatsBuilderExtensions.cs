using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using Relayer.Transports;

namespace Relayer.Nats;

/// <summary>Registers named native NATS JetStream transports.</summary>
public static class NatsBuilderExtensions
{
    /// <summary>Registers an owned connection, explicit topology, confirmed sends and durable pull consumers.</summary>
    public static IRelayerBuilder Nats(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<NatsTransportOptions, NatsRouteOptions, NatsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport<NatsTransportOptions, NatsRouteOptions, NatsConsumerOptions>(name, transport =>
        {
            transport.SendDefaults = new() { HeaderNames = new("ce-") };
            configure(transport);
        });
        builder.Services.AddKeyedSingleton<NatsNativeTransport>(name, (provider, _) => new(name,
            provider.GetRequiredService<IOptionsMonitor<NatsTransportOptions>>().Get(name),
            provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name),
            provider.GetRequiredService<MessageTracing>(), provider.GetService<ILoggerFactory>()));
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<NatsNativeTransport>(name));
        builder.Services.AddSingleton<IHostedService>(provider => new NatsHostedService(provider, name));
        return builder;
    }
}

/// <summary>JetStream route configuration. Native headers can be customized with SendOptions.Nats.</summary>
public sealed class NatsRouteOptions;

/// <summary>Native durable pull consumer selection and failure policy.</summary>
public sealed class NatsConsumerOptions
{
    /// <summary>The declared stream containing this durable consumer.</summary>
    public string Stream { get; set; } = "";
    /// <summary>Unchanged native pull buffering and drain settings. DrainOnCancel must be true.</summary>
    public NatsJSConsumeOpts Settings { get; set; } = new() { MaxMsgs = 1, DrainOnCancel = true };
    /// <summary>In-process retry and confirmed dead-letter copies. An explicit DeadLetterEndpoint subject is required.</summary>
    public FailureOptions Failure { get; set; } = new();
}

internal sealed class NatsHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private NatsNativeTransport? _transport;
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport = provider.GetRequiredKeyedService<NatsNativeTransport>(name);
        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
        var registry = provider.GetRequiredService<HandlerRegistry>();
        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<NatsConsumerOptions>)declaration;
            var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Source, consumer.Options.Failure, Copy);
            var endpoint = new MessagingEndpoint { System = "nats", Destination = consumer.Options.Stream, Subscription = consumer.Source };
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                provider.GetService<IFailureClassifier>())
                .Tracing(provider.GetRequiredService<MessageTracing>(), endpoint)
                .Telemetry(provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
                { System = "nats", Destination = consumer.Options.Stream, Subscription = consumer.Source }));
            var worker = _transport.AddConsumer(consumer.Options.Stream, consumer.Source, consumer.Options.Settings, consumer.Concurrency);
            pipeline.Settle(worker.SettleContextAsync);
            pipeline.ConfigureConsumer(provider, consumer);
            foreach (var handler in registry.Descriptors)
                if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
            var receive = new ReceivePipeline<NatsDelivery, NatsBinding>(pipeline.Build(consumer.Concurrency), new("ce-"), consumer.Concurrency);
            worker.Receive = receive.ProcessAsync;
        }
        await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var completion in Task.WhenEach(_transport!.Consumers.Select(worker => worker.Completion)).ConfigureAwait(false))
            await completion.ConfigureAwait(false);
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try { if (_transport is not null) await _transport.StopAsync(cancellationToken).ConfigureAwait(false); }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }
    private static OutgoingMessage Copy(MessageContext context)
    {
        var message = context.Nats().Message;
        var headers = new KeyValuePair<string, WireString>[message.Headers?.Count ?? 0];
        var index = 0;
        if (message.Headers is { } source)
            foreach (var pair in source)
            {
                // The copy is a new publication: stream-wide deduplication and optimistic
                // publication checks and destructive rollups belong to the original send, including noncanonical casing.
                if (string.Equals(pair.Key, "Nats-Msg-Id", StringComparison.OrdinalIgnoreCase) ||
                    pair.Key.StartsWith("Nats-Expected-", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(pair.Key, "Nats-Rollup", StringComparison.OrdinalIgnoreCase)) continue;
                headers[index++] = new(pair.Key, new(pair.Value.ToString()));
            }
        return new(message.Subject, message.Data, headers.AsMemory(0, index), options: new SendOptions().Nats(
            native =>
            {
                ((NatsPublishHeaders)native).IsFailureCopy = true;
                // Portable headers carry a single string; preserve unchanged native lists after
                // the core has added failure metadata. Do not overwrite updated failure headers.
                if (message.Headers is not { } original) return;
                foreach (var pair in original)
                    if (pair.Value.Count > 1 && native.TryGetValue(pair.Key, out var current) &&
                        current.ToString() == pair.Value.ToString())
                        native[pair.Key] = pair.Value;
            }));
    }
}
