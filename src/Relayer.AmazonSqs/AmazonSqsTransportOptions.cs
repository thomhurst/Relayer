using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace Relayer.AmazonSqs;

/// <summary>Options for the AmazonSqs transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class AmazonSqsTransportOptions
{
    /// <summary>Native SQS client configuration.</summary>
    public AmazonSQSConfig Sqs { get; set; } = new();

    /// <summary>Native SNS client configuration.</summary>
    public AmazonSimpleNotificationServiceConfig Sns { get; set; } = new();
}
