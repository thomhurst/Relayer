using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>Registers native SNS topic sending and topology.</summary>
public static class AmazonSnsBuilderExtensions
{
    /// <summary>Declares a named SNS transport with explicit native topics and subscriptions.</summary>
    public static IRelayerBuilder AmazonSns(this IRelayerBuilder builder, string name,
        Action<TransportBuilder<AmazonSnsTransportOptions, AmazonSnsRouteOptions, AmazonSnsConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.AddTransport(name, configure);
        builder.Services.AddSingleton<ITransport>(provider =>
        {
            var declaration = provider.GetRequiredService<RelayerConfiguration>().Transports.Single(item => item.Name == name);
            if (declaration.Consumers.Count != 0)
                throw new NotSupportedException($"SNS transport '{name}': consume through a native SQS subscription instead.");
            var options = provider.GetRequiredService<IOptionsMonitor<AmazonSnsTransportOptions>>().Get(name);
            SnsSendTransport.ValidateDeclarations(name, options, declaration.Routes.Select(route => route.Destination));
            var headers = SqsSendTransport.ResolveIdentityHeaders(declaration.Routes, "SNS FIFO topic");
            var client = options.SnsClient ?? new AmazonSimpleNotificationServiceClient(options.Sns);
            return new SnsSendTransport(name, client, options.SnsClient is null, options,
                provider.GetRequiredService<MessageTracing>(), provider.GetService<ILoggerFactory>(), headers);
        });
        builder.Services.AddSingleton<IHostedService>(provider => new SqsSendHostedService(provider, name));
        return builder;
    }

    /// <summary>Customizes the native Publish request, also copied into native batch entries.</summary>
    public static SendOptions AmazonSns(this SendOptions options, Action<PublishRequest> configure) => options.ConfigureNative(configure);
}

/// <summary>SNS route options; topic configuration belongs on native declarations.</summary>
public sealed class AmazonSnsRouteOptions;
/// <summary>SNS consumption requires a native subscription, rather than an SNS receiver.</summary>
public sealed class AmazonSnsConsumerOptions;
