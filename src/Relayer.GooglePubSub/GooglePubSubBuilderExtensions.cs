using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

/// <summary>Registers named native Google Cloud Pub/Sub transports.</summary>
public static class GooglePubSubBuilderExtensions
{
    /// <summary>Declares topics, streaming consumers and durable failure copies using prepared native API clients.</summary>
    public static IRelayerBuilder GooglePubSub(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<GooglePubSubTransportOptions, GooglePubSubRouteOptions, GooglePubSubConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport<GooglePubSubTransportOptions, GooglePubSubRouteOptions, GooglePubSubConsumerOptions>(name, transport =>
        {
            transport.SendDefaults = new() { HeaderNames = new("ce-") };
            configure(transport);
        });
        builder.Services.AddKeyedSingleton<GooglePubSubTransport>(name, (provider, _) =>
        {
            var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<GooglePubSubTransportOptions>>().Get(name);
            return new(name, options, definition, provider.GetRequiredService<MessageTracing>());
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<GooglePubSubTransport>(name));
        builder.Services.AddKeyedSingleton<IGooglePubSubClients>(name, (provider, _) => provider.GetRequiredKeyedService<GooglePubSubTransport>(name));
        builder.Services.AddSingleton<IHostedService>(provider => new GooglePubSubHostedService(provider, name));
        return builder;
    }
}

internal sealed class GooglePubSubHostedService(IServiceProvider provider, string name) : BackgroundService
{
    private GooglePubSubTransport? _transport;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        _transport = provider.GetRequiredKeyedService<GooglePubSubTransport>(name);
        var definition = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
        var registry = provider.GetRequiredService<HandlerRegistry>();
        var tracing = provider.GetRequiredService<MessageTracing>();
        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<GooglePubSubConsumerOptions>)declaration;
            var failures = new FailurePipeline(provider, name, consumer.Source, consumer.Source, consumer.Failure, Copy);
            var endpoint = new MessagingEndpoint { System = "gcp_pubsub", Destination = _transport.TopicFor(consumer.Source), Subscription = consumer.Source };
            var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), provider.GetMessageSerializer(name), failures,
                consumer.FailureClassifier ?? provider.GetService<IFailureClassifier>())
                .Tracing(tracing, endpoint)
                .Telemetry(provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
                { System = "gcp_pubsub", Destination = endpoint.Destination, Subscription = endpoint.Subscription }));
            var worker = _transport.AddConsumer(consumer.Source, consumer.Options.Settings);
            pipeline.Settle(worker.SettleContextAsync);
            pipeline.ConfigureConsumer(provider, consumer);
            foreach (var handler in registry.Descriptors)
                if (consumer.Handlers.Contains(handler.HandlerType)) pipeline.Handler(handler);
            var receive = new ReceivePipeline<GooglePubSubDelivery, GooglePubSubBinding>(pipeline.Build(consumer.Concurrency), new("ce-"), consumer.Concurrency);
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
        return Copy(context.GooglePubSub().Message);
    }

    internal static OutgoingMessage Copy(Google.Cloud.PubSub.V1.PubsubMessage message)
    {
        var count = 0;
        foreach (var header in message.Attributes)
            if (!header.Key.StartsWith("goog", StringComparison.Ordinal) && !GooglePubSubFailureHeaders.IsHistory(header.Key)) count++;
        var headers = new KeyValuePair<string, WireString>[count];
        var index = 0;
        foreach (var header in message.Attributes)
            if (!header.Key.StartsWith("goog", StringComparison.Ordinal) && !GooglePubSubFailureHeaders.IsHistory(header.Key))
                headers[index++] = new(header.Key, new(header.Value));
        return new("copy", message.Data.Memory, headers, message.OrderingKey.Length == 0 ? null : new WireString(message.OrderingKey));
    }
}
