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
        Action<TransportBuilder<AmazonSqsTransportOptions, AmazonSqsRouteOptions, AmazonSqsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddKeyedSingleton<SqsSendTransport>(name, (provider, _) =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            var options = provider.GetRequiredService<IOptionsMonitor<AmazonSqsTransportOptions>>().Get(name);
            SqsSendTransport.ValidateDeclarations(name, options, declaration.Routes.Select(route => route.Destination));
            foreach (var consumer in declaration.Consumers)
                SqsConsumer.ValidateDeclaration(name, options, (ConsumerDefinition<AmazonSqsConsumerOptions>)consumer);
            var identityHeaders = SqsSendTransport.ResolveIdentityHeaders(declaration.Routes);
            var client = options.SqsClient ?? new AmazonSQSClient(options.Sqs);
            return new SqsSendTransport(name, client, options.SqsClient is null, options,
                provider.GetRequiredService<MessageTracing>(), provider.GetService<ILoggerFactory>(), identityHeaders);
        });
        builder.Services.AddSingleton<ITransport>(provider => provider.GetRequiredKeyedService<SqsSendTransport>(name));
        builder.Services.AddSingleton<IHostedService>(provider => new SqsHostedService(provider, name));
        return builder;
    }

    /// <summary>Customizes the native request immediately before single or batch sending.</summary>
    public static SendOptions AmazonSqs(this SendOptions options, Action<SendMessageRequest> configure) =>
        options.ConfigureNative(configure);
}

/// <summary>SQS route options. Native queue settings belong on the transport declaration.</summary>
public sealed class AmazonSqsRouteOptions;

/// <summary>Native long-poll request settings and durable failure handling.</summary>
public sealed class AmazonSqsConsumerOptions
{
    /// <summary>CloudEvents header binding used by this queue. Match the sending route's HeaderNames.</summary>
    public HeaderNames HeaderNames { get; set; } = new();
    /// <summary>Configures each worker's native ReceiveMessageRequest once before polling. QueueUrl, one-message receives and all attributes are required.</summary>
    public Action<ReceiveMessageRequest>? ConfigureReceive { get; set; }
    /// <summary>Retry policy and explicit dead-letter copy destination. Native redrive cannot attach failure metadata.</summary>
    public FailureOptions Failure { get; set; } = new();
}

// SNS uses the same simple send-only lifecycle.
internal sealed class SqsSendHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken token) => Transport.StartAsync(token);
    public Task StopAsync(CancellationToken token) => Transport.StopAsync(token);
}
