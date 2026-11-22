using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>Registers named native SQS transports.</summary>
public static class AmazonSqsBuilderExtensions
{
    /// <summary>Declares native SQS sending, long-poll receivers and queue topology.</summary>
    public static IRelayerBuilder AmazonSqs(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<AmazonSqsTransportOptions, AmazonSqsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddKeyedSingleton<SqsSendTransport>(name, (provider, _) =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<AmazonSqsTransportOptions>>().Get(name);
            SqsSendTransport.ValidateDeclarations(name, options, declaration.Routes.Select(route => route.Destination));

            var routeIdentityHeaders = SqsSendTransport.ResolveIdentityHeaders(declaration.Routes);
            var identityHeaders = new Dictionary<string, string>(routeIdentityHeaders, StringComparer.Ordinal);

            foreach (var consumerDeclaration in declaration.Consumers)
            {
                var consumer = (ConsumerDefinition<AmazonSqsConsumerOptions>)consumerDeclaration;
                SqsConsumer.ValidateDeclaration(name, options, consumer);

                if (!consumer.Source.EndsWith(".fifo", StringComparison.Ordinal) ||
                    routeIdentityHeaders.ContainsKey(consumer.Source))
                {
                    continue;
                }

                // Resolve once so the failure copy and the eventual outbox dispatch use the same identity.
                string identityHeader = consumer.Options.HeaderNames.Id;

                if (!identityHeaders.TryAdd(consumer.Source, identityHeader) &&
                    identityHeaders[consumer.Source] != identityHeader)
                {
                    throw new InvalidOperationException($"SQS FIFO queue '{consumer.Source}': consumers must use the same HeaderNames identity binding when no CloudEvents send route declares one. Configure matching HeaderNames for consumers on this queue.");
                }
            }

            var client = options.SqsClient ?? new AmazonSQSClient(options.Sqs);
            return new SqsSendTransport(name, client, options.SqsClient is null, options,
                provider.GetRequiredService<MessageTracing>(), provider.GetService<ILoggerFactory>(), identityHeaders);
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<SqsSendTransport>(name));
        builder.Services.AddKeyedSingleton<SqsHostedService>(name, (provider, _) => new SqsHostedService(provider, name));
        builder.Services.AddKeyedSingleton<IConsumerControl>(name, (provider, key) => provider.GetRequiredKeyedService<SqsHostedService>(key));
        builder.Services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<SqsHostedService>(name));
        return builder;
    }

    /// <summary>Customizes the native request immediately before single or batch sending.</summary>
    public static SendOptions AmazonSqs(this SendOptions options, Action<SendMessageRequest> configure)
    {
        return options.ConfigureNative(configure);
    }
}

/// <summary>Native long-poll request settings and durable failure handling.</summary>
public sealed class AmazonSqsConsumerOptions
{
    /// <summary>CloudEvents header binding used by this queue. Match the sending route's HeaderNames.</summary>
    public HeaderNames HeaderNames
    {
        get; set;
    } = new();

    /// <summary>Configures each worker's native ReceiveMessageRequest once before polling. QueueUrl and all attributes are required. Standard queues allow 1–10 messages; FIFO requires 1.</summary>
    public Action<ReceiveMessageRequest>? ConfigureReceive
    {
        get; set;
    }
}

// SNS uses the same simple send-only lifecycle.
internal sealed class SqsSendHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);

    public Task StartAsync(CancellationToken token)
    {
        return Transport.StartAsync(token);
    }

    public Task StopAsync(CancellationToken token)
    {
        return Transport.StopAsync(token);
    }
}
