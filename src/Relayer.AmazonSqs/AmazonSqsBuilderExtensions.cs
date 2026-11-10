using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>Registers named native SQS send transports.</summary>
public static class AmazonSqsBuilderExtensions
{
    /// <summary>Declares SQS queue sending and topology. Receivers are not implemented yet; use AmazonSns for topic sending.</summary>
    public static IRelayerBuilder AmazonSqs(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<AmazonSqsTransportOptions, AmazonSqsRouteOptions, AmazonSqsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (declaration.Consumers.Count != 0)
                throw new NotSupportedException($"SQS transport '{name}': receiver composition is not implemented yet.");
            var options = provider.GetRequiredService<IOptionsMonitor<AmazonSqsTransportOptions>>().Get(name);
            SqsSendTransport.ValidateDeclarations(name, options, declaration.Routes.Select(route => route.Destination));
            var identityHeaders = SqsSendTransport.ResolveIdentityHeaders(declaration.Routes);
            var client = options.SqsClient ?? new AmazonSQSClient(options.Sqs);
            return new SqsSendTransport(name, client, options.SqsClient is null, options,
                provider.GetRequiredService<MessageTracing>(), provider.GetService<ILoggerFactory>(), identityHeaders);
        });
        builder.Services.AddSingleton<IHostedService>(provider => new SqsSendHostedService(provider, name));
        return builder;
    }

    /// <summary>Customizes the native request immediately before single or batch sending.</summary>
    public static SendOptions AmazonSqs(this SendOptions options, Action<SendMessageRequest> configure) =>
        options.ConfigureNative(configure);
}

/// <summary>SQS route options. Native queue settings belong on the transport declaration.</summary>
public sealed class AmazonSqsRouteOptions;

/// <summary>Reserved for native receiver composition.</summary>
public sealed class AmazonSqsConsumerOptions;

internal sealed class SqsSendHostedService(IServiceProvider provider, string name) : IHostedService
{
    private ITransport Transport => provider.GetServices<ITransport>().Single(transport => transport.Name == name);
    public Task StartAsync(CancellationToken cancellationToken) => Transport.StartAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Transport.StopAsync(cancellationToken);
}
