using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace Relayer.AmazonSqs;

/// <summary>Native SNS topic declarations and optional SQS queue topology.</summary>
public sealed class AmazonSnsTransportOptions
{
    /// <summary>A caller-owned SNS client. Otherwise the native AWS credential chain is used.</summary>
    public IAmazonSimpleNotificationService? SnsClient { get; set; }
    /// <summary>Native SNS client configuration, passed through unchanged.</summary>
    public AmazonSimpleNotificationServiceConfig Sns { get; set; } = new();
    /// <summary>Native topic declarations. Route destinations are topic names.</summary>
    public IList<CreateTopicRequest> Topics { get; } = [];
    /// <summary>Native SQS subscriptions with explicit topic and queue ARNs and RawMessageDelivery=true.</summary>
    public IList<SubscribeRequest> Subscriptions { get; } = [];
    /// <summary>Optional queue declarations, including native access policies granting SNS SendMessage permission.</summary>
    public AmazonSqsTransportOptions QueueTopology { get; } = new();
}
