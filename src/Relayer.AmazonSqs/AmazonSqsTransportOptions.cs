using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Relayer.AmazonSqs;

/// <summary>Options for the AmazonSqs transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class AmazonSqsTransportOptions
{
    /// <summary>A caller-owned SQS client. When absent, Relayer uses Sqs and the AWS credential chain.</summary>
    public IAmazonSQS? SqsClient { get; set; }

    /// <summary>Native queue declarations. Queue names must match route destinations.</summary>
    public IList<CreateQueueRequest> Queues { get; } = [];

    /// <summary>Native SQS client configuration.</summary>
    public AmazonSQSConfig Sqs { get; set; } = new();

    /// <summary>Native SNS client configuration.</summary>
    public AmazonSimpleNotificationServiceConfig Sns { get; set; } = new();
}
